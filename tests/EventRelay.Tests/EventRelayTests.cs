using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace EventRelay.Tests;

public class EventRelayTests
{
    private static StringContent Json(string content) =>
        new(content, System.Text.Encoding.UTF8, "application/json");

    private static async Task<JsonElement> PublishAsync(
        HttpClient client, string stream, string type, string dataJson)
    {
        var response = await client.PostAsJsonAsync(
            $"/streams/{stream}/events", new { type, data = JsonDocument.Parse(dataJson).RootElement });
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        return (await response.Content.ReadFromJsonAsync<JsonElement>());
    }

    private static string StreamName([System.Runtime.CompilerServices.CallerMemberName] string name = "") =>
        $"s-{name}-{Guid.NewGuid():N}";

    [Fact]
    public async Task Publish_AssignsStrictlyIncreasingIds_AndReturnsFullEvent()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        var first = await PublishAsync(client, stream, "created", """{"n":1}""");
        var second = await PublishAsync(client, stream, "updated", """{"n":2}""");

        Assert.Equal(1, first.GetProperty("id").GetInt64());
        Assert.Equal(2, second.GetProperty("id").GetInt64());
        Assert.Equal(stream, first.GetProperty("stream").GetString());
        Assert.Equal("created", first.GetProperty("type").GetString());
        Assert.Equal(1, first.GetProperty("data").GetProperty("n").GetInt32());
        Assert.True(first.GetProperty("timestamp").GetDateTimeOffset() > DateTimeOffset.MinValue);
    }

    [Fact]
    public async Task Publish_ConcurrentWriters_ProduceUniqueSequentialIds()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        const int total = 200;
        var tasks = Enumerable.Range(0, total).Select(i =>
            PublishAsync(client, stream, "tick", $"{{\"n\":{i}}}")).ToList();
        var results = await Task.WhenAll(tasks);

        var ids = results.Select(r => r.GetProperty("id").GetInt64()).OrderBy(id => id).ToArray();
        Assert.Equal(Enumerable.Range(1, total).Select(i => (long)i), ids);
    }

    [Theory]
    [InlineData("not-json", "invalid-json")]
    [InlineData("{}", "invalid-type")]
    [InlineData("""{"type":"x"}""", "invalid-data")]
    [InlineData("""{"type":"x","data":null}""", "invalid-data")]
    [InlineData("""{"type":"  ","data":{}}""", "invalid-type")]
    public async Task Publish_InvalidInput_Returns4xxAndDoesNotConsumeId(string body, string expectedCode)
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        var bad = await client.PostAsync($"/streams/{stream}/events", Json(body));
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        var error = await bad.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(expectedCode, error.GetProperty("error").GetProperty("code").GetString());

        var good = await PublishAsync(client, stream, "ok", "{}");
        Assert.Equal(1, good.GetProperty("id").GetInt64());
    }

    [Fact]
    public async Task Subscribe_ReplaysHistoryAfterCursor_InOrder()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();
        for (var i = 1; i <= 4; i++)
            await PublishAsync(client, stream, $"t{i}", $"{{\"n\":{i}}}");

        await using var sse = await SseClient.ConnectAsync(client, stream, after: 2);
        var first = await sse.ReadEventAsync();
        var second = await sse.ReadEventAsync();

        Assert.Equal("3", first.Id);
        Assert.Equal("t3", first.EventType);
        Assert.Equal(3, JsonDocument.Parse(first.Data).RootElement.GetProperty("data").GetProperty("n").GetInt32());
        Assert.Equal("4", second.Id);
        Assert.Equal("t4", second.EventType);
    }

    [Fact]
    public async Task Subscribe_UsesLastEventIdHeader_WhenNoQueryParam()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();
        for (var i = 1; i <= 3; i++)
            await PublishAsync(client, stream, "tick", $"{{\"n\":{i}}}");

        await using var sse = await SseClient.ConnectAsync(client, stream, lastEventId: "1");
        var evt = await sse.ReadEventAsync();
        Assert.Equal("2", evt.Id);
    }

    [Fact]
    public async Task Subscribe_HandsOffFromReplayToLive_WithoutGapsOrDuplicates()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();
        for (var i = 1; i <= 5; i++)
            await PublishAsync(client, stream, "history", $"{{\"n\":{i}}}");

        await using var sse = await SseClient.ConnectAsync(client, stream, after: 0);

        // Publish live events while the subscriber may still be replaying.
        var livePublish = Task.Run(async () =>
        {
            for (var i = 6; i <= 20; i++)
                await PublishAsync(client, stream, "live", $"{{\"n\":{i}}}");
        });

        var received = new List<long>();
        while (received.Count < 20)
            received.Add(long.Parse((await sse.ReadEventAsync()).Id!));
        await livePublish;

        Assert.Equal(Enumerable.Range(1, 20).Select(i => (long)i), received);
    }

    [Fact]
    public async Task Subscribe_MultipleSubscribers_ReceiveSameEventsIndependently()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        await using var first = await SseClient.ConnectAsync(client, stream);
        await using var second = await SseClient.ConnectAsync(client, stream);
        for (var i = 1; i <= 5; i++)
            await PublishAsync(client, stream, "tick", $"{{\"n\":{i}}}");

        foreach (var sse in new[] { first, second })
        {
            for (var i = 1; i <= 5; i++)
                Assert.Equal(i.ToString(), (await sse.ReadEventAsync()).Id);
        }
    }

    [Fact]
    public async Task Subscribe_SlowConsumer_ReceivesErrorEvent_AndOthersAreUnaffected()
    {
        using var factory = new EventRelayFactory { MaxSubscriberQueueSize = 4 };
        using var client = factory.CreateClient();
        var stream = StreamName();

        await using var healthy = await SseClient.ConnectAsync(client, stream);
        await using var slow = await SseClient.ConnectAsync(client, stream);

        var service = factory.Services.GetRequiredService<EventRelay.Api.Domain.IEventStreamService>();

        // Drain the healthy subscriber concurrently while publishing.
        var healthyIds = new List<long>();
        var healthyReader = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var evt = await healthy.ReadEventAsync();
                    if (evt.Id is null)
                        break;
                    healthyIds.Add(long.Parse(evt.Id));
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        // Publish until the slow subscriber's bounded queue overflows (its socket is
        // never drained). The publisher must never block on the slow subscriber.
        var published = 0;
        while (published < 100000 && service.SubscriberCount(stream) == 2)
        {
            published++;
            await PublishAsync(client, stream, "tick", $"{{\"n\":{published}}}");
        }
        Assert.True(published < 100000, "Slow subscriber was never dropped.");
        Assert.Equal(1, service.SubscriberCount(stream));

        // The healthy subscriber received every event in order, with no gaps.
        var after = await PublishAsync(client, stream, "after", "{}");
        Assert.Equal(published + 1, after.GetProperty("id").GetInt64());
        using (var waitTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        {
            while (healthyIds.Count < published + 1)
            {
                waitTimeout.Token.ThrowIfCancellationRequested();
                await Task.Delay(10, waitTimeout.Token);
            }
        }
        Assert.Equal(Enumerable.Range(1, published + 1).Select(i => (long)i), healthyIds);

        // The slow subscriber is notified with a slow-consumer error event.
        var sawError = false;
        for (var i = 0; i < published + 8 && !sawError; i++)
        {
            var evt = await slow.ReadEventAsync();
            if (evt.EventType == "error")
            {
                sawError = true;
                Assert.Contains("slow-consumer", evt.Data);
            }
        }
        Assert.True(sawError, "Slow subscriber should receive a slow-consumer error event.");
        await healthy.CancelAsync();
        await healthyReader;
    }

    [Fact]
    public async Task Subscribe_StaleCursor_ReturnsConflict()
    {
        using var factory = new EventRelayFactory { MaxHistoryPerStream = 5 };
        using var client = factory.CreateClient();
        var stream = StreamName();
        for (var i = 1; i <= 10; i++)
            await PublishAsync(client, stream, "tick", $"{{\"n\":{i}}}");

        var stale = await client.GetAsync($"/streams/{stream}/events?after=2");
        Assert.Equal(HttpStatusCode.Conflict, stale.StatusCode);
        var error = await stale.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("cursor-expired", error.GetProperty("error").GetProperty("code").GetString());

        await using var ok = await SseClient.ConnectAsync(client, stream, after: 5);
        Assert.Equal("6", (await ok.ReadEventAsync()).Id);
    }

    [Theory]
    [InlineData("?after=-1")]
    [InlineData("?after=abc")]
    public async Task Subscribe_InvalidCursor_ReturnsBadRequest(string query)
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        var response = await client.GetAsync($"/streams/{stream}/events{query}");
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var error = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("invalid-cursor", error.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public async Task Subscribe_EmptyStream_WaitsForLiveEvents()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        await using var sse = await SseClient.ConnectAsync(client, stream);
        await PublishAsync(client, stream, "first", """{"n":1}""");

        var evt = await sse.ReadEventAsync();
        Assert.Equal("1", evt.Id);
        Assert.Equal("first", evt.EventType);
    }

    [Fact]
    public async Task Subscribe_ClientDisconnect_ReleasesSubscription()
    {
        using var factory = new EventRelayFactory();
        using var client = factory.CreateClient();
        var stream = StreamName();

        var sse = await SseClient.ConnectAsync(client, stream);
        await sse.CancelAsync();
        await sse.DisposeAsync();

        // Publishing after disconnect must not throw or block.
        var published = await PublishAsync(client, stream, "tick", "{}");
        Assert.Equal(1, published.GetProperty("id").GetInt64());
    }
}
