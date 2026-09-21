using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace EventRelay.Tests;

public sealed class SubscribeTests
{
    private static async Task PublishAsync(HttpClient client, string stream, string type, object data)
    {
        var response = await client.PostAsJsonAsync($"/streams/{stream}/events", new { type, data });
        response.EnsureSuccessStatusCode();
    }

    [Fact]
    public async Task Replay_FromBeginning_DeliversHistoryInOrder()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "replay-" + Guid.NewGuid().ToString("N");
        for (var i = 1; i <= 5; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        using var response = await SseClient.ConnectAsync(client, stream, after: 0);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        var frames = await SseClient.ReadFramesAsync(response, 5, TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "1", "2", "3", "4", "5" }, frames.Select(f => f.Id).ToArray());
        Assert.All(frames, f => Assert.Equal("tick", f.Event));
        Assert.Contains("\"n\":5", frames[4].Data);
    }

    [Fact]
    public async Task Replay_AfterQueryParam_SkipsEarlierEvents()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "after-" + Guid.NewGuid().ToString("N");
        for (var i = 1; i <= 5; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        using var response = await SseClient.ConnectAsync(client, stream, after: 3);
        var frames = await SseClient.ReadFramesAsync(response, 2, TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "4", "5" }, frames.Select(f => f.Id).ToArray());
    }

    [Fact]
    public async Task Replay_LastEventIdHeader_IsUsedWhenNoQueryParam()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "leid-" + Guid.NewGuid().ToString("N");
        for (var i = 1; i <= 4; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        using var response = await SseClient.ConnectAsync(client, stream, lastEventId: "2");
        var frames = await SseClient.ReadFramesAsync(response, 2, TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "3", "4" }, frames.Select(f => f.Id).ToArray());
    }

    [Fact]
    public async Task ReplayToLive_HandoffDuringConcurrentPublish_LosesNothingAndDuplicatesNothing()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "handoff-" + Guid.NewGuid().ToString("N");
        for (var i = 1; i <= 10; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        using var response = await SseClient.ConnectAsync(client, stream, after: 5);

        var livePublish = Task.Run(async () =>
        {
            for (var i = 11; i <= 40; i++)
                await PublishAsync(client, stream, "tick", new { n = i });
        });

        var frames = await SseClient.ReadFramesAsync(response, 35, TimeSpan.FromSeconds(15));
        await livePublish;

        var ids = frames.Select(f => long.Parse(f.Id!)).ToArray();
        Assert.Equal(Enumerable.Range(6, 35).Select(i => (long)i), ids);
    }

    [Fact]
    public async Task MultipleSubscribers_EachReceiveAllLiveEvents()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "multi-" + Guid.NewGuid().ToString("N");

        using var first = await SseClient.ConnectAsync(client, stream, after: 0);
        using var second = await SseClient.ConnectAsync(client, stream, after: 0);

        for (var i = 1; i <= 3; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        var firstFrames = await SseClient.ReadFramesAsync(first, 3, TimeSpan.FromSeconds(10));
        var secondFrames = await SseClient.ReadFramesAsync(second, 3, TimeSpan.FromSeconds(10));

        Assert.Equal(new[] { "1", "2", "3" }, firstFrames.Select(f => f.Id).ToArray());
        Assert.Equal(new[] { "1", "2", "3" }, secondFrames.Select(f => f.Id).ToArray());
    }

    [Fact]
    public async Task CursorOlderThanRetainedHistory_Returns409()
    {
        using var factory = new EventRelayFactory { MaxHistoryPerStream = 3 };
        using var client = factory.CreateClient();
        var stream = "expired-" + Guid.NewGuid().ToString("N");
        for (var i = 1; i <= 5; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        using var conflict = await SseClient.ConnectAsync(client, stream, after: 1);
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        var error = await conflict.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("cursor_expired", error.GetProperty("error").GetProperty("code").GetString());

        using var ok = await SseClient.ConnectAsync(client, stream, after: 2);
        Assert.Equal(HttpStatusCode.OK, ok.StatusCode);
        var frames = await SseClient.ReadFramesAsync(ok, 3, TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "3", "4", "5" }, frames.Select(f => f.Id).ToArray());
    }

    [Fact]
    public async Task InvalidCursor_Returns400()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "cursor-" + Guid.NewGuid().ToString("N");

        Assert.Equal(HttpStatusCode.BadRequest,
            (await client.GetAsync($"/streams/{stream}/events?after=-1")).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest,
            (await client.GetAsync($"/streams/{stream}/events?after=abc")).StatusCode);

        using var badHeader = await SseClient.ConnectAsync(client, stream, lastEventId: "not-a-number");
        Assert.Equal(HttpStatusCode.BadRequest, badHeader.StatusCode);
    }

    [Fact]
    public async Task SlowConsumer_ReceivesErrorFrameAndStreamCloses()
    {
        using var factory = new EventRelayFactory { MaxHistoryPerStream = 10, SubscriberQueueCapacity = 2 };
        using var client = factory.CreateClient();
        var stream = "slow-" + Guid.NewGuid().ToString("N");
        for (var i = 1; i <= 5; i++)
            await PublishAsync(client, stream, "tick", new { n = i });

        // Replay of 5 events into a capacity-2 queue overflows immediately -> slow consumer.
        using var response = await SseClient.ConnectAsync(client, stream, after: 0);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var body = await response.Content.ReadAsStreamAsync(cts.Token);
        var frames = new List<SseFrame>();
        SseFrame? frame;
        while ((frame = await SseClient.ReadFrameAsync(body, cts.Token)) is not null)
            frames.Add(frame);

        var slowFrame = frames.Last();
        Assert.Equal("slow-consumer", slowFrame.Event);
        Assert.Contains("slow_consumer", slowFrame.Data);
    }

    [Fact]
    public async Task ClientDisconnect_ReleasesSubscription_AndServiceKeepsWorking()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = "disc-" + Guid.NewGuid().ToString("N");

        using var cts = new CancellationTokenSource();
        var response = await SseClient.ConnectAsync(client, stream, after: 0, cancellationToken: cts.Token);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        cts.Cancel();
        response.Dispose();
        await Task.Delay(200);

        await PublishAsync(client, stream, "tick", new { n = 1 });
        using var second = await SseClient.ConnectAsync(client, stream, after: 0);
        var frames = await SseClient.ReadFramesAsync(second, 1, TimeSpan.FromSeconds(10));
        Assert.Equal("1", frames[0].Id);
    }
}

