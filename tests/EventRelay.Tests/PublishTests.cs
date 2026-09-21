using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace EventRelay.Tests;

public sealed class PublishTests : IClassFixture<EventRelayFactory>
{
    private readonly EventRelayFactory _factory;

    public PublishTests(EventRelayFactory factory) => _factory = factory;

    [Fact]
    public async Task Publish_Returns201_WithStrictlyIncreasingIds()
    {
        using var client = _factory.CreateClient();
        var stream = "publish-" + Guid.NewGuid().ToString("N");

        for (var expected = 1; expected <= 3; expected++)
        {
            var response = await client.PostAsJsonAsync($"/streams/{stream}/events",
                new { type = "created", data = new { value = expected } });
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            var body = await response.Content.ReadFromJsonAsync<JsonElement>();
            Assert.Equal(expected, body.GetProperty("id").GetInt64());
            Assert.Equal("created", body.GetProperty("type").GetString());
            Assert.Equal(expected, body.GetProperty("data").GetProperty("value").GetInt32());
        }
    }

    [Fact]
    public async Task Publish_IdsAreIndependentPerStream()
    {
        using var client = _factory.CreateClient();
        var suffix = Guid.NewGuid().ToString("N");

        var first = await client.PostAsJsonAsync($"/streams/a-{suffix}/events", new { type = "t", data = 1 });
        var second = await client.PostAsJsonAsync($"/streams/b-{suffix}/events", new { type = "t", data = 1 });

        Assert.Equal(1, (await first.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetInt64());
        Assert.Equal(1, (await second.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetInt64());
    }

    [Theory]
    [InlineData("""{"type":"","data":1}""")]
    [InlineData("""{"type":"  ","data":1}""")]
    [InlineData("""{"data":1}""")]
    [InlineData("""{"type":"t"}""")]
    [InlineData("not-json")]
    public async Task Publish_InvalidInput_Returns400_AndDoesNotConsumeId(string payload)
    {
        using var client = _factory.CreateClient();
        var stream = "invalid-" + Guid.NewGuid().ToString("N");

        var bad = await client.PostAsync($"/streams/{stream}/events",
            new StringContent(payload, System.Text.Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        var error = await bad.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(string.IsNullOrWhiteSpace(error.GetProperty("error").GetProperty("code").GetString()));

        var good = await client.PostAsJsonAsync($"/streams/{stream}/events", new { type = "t", data = 1 });
        Assert.Equal(HttpStatusCode.Created, good.StatusCode);
        Assert.Equal(1, (await good.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetInt64());
    }

    [Fact]
    public async Task Publish_WhitespaceStream_Returns400()
    {
        using var client = _factory.CreateClient();
        var response = await client.PostAsJsonAsync("/streams/%20%20/events", new { type = "t", data = 1 });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Publish_ConcurrentPublishers_IdsAreDenseAndOrdered()
    {
        using var client = _factory.CreateClient();
        var stream = "conc-" + Guid.NewGuid().ToString("N");
        const int publishers = 8;
        const int perPublisher = 25;

        var tasks = Enumerable.Range(0, publishers).Select(_ => Task.Run(async () =>
        {
            var ids = new List<long>();
            for (var i = 0; i < perPublisher; i++)
            {
                var response = await client.PostAsJsonAsync($"/streams/{stream}/events", new { type = "t", data = i });
                response.EnsureSuccessStatusCode();
                ids.Add((await response.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetInt64());
            }
            return ids;
        })).ToArray();

        var all = (await Task.WhenAll(tasks)).SelectMany(x => x).OrderBy(x => x).ToArray();
        Assert.Equal(Enumerable.Range(1, publishers * perPublisher).Select(i => (long)i), all);
    }
}

