using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace EventRelay.Tests;

public sealed record SseEvent(string? Id, string EventType, string Data);

public sealed class SseClient : IAsyncDisposable
{
    private readonly HttpClient _client;
    private readonly CancellationTokenSource _cts = new();
    private HttpResponseMessage? _response;
    private StreamReader? _reader;

    private SseClient(HttpClient client) => _client = client;

    public static async Task<SseClient> ConnectAsync(
        HttpClient client, string stream, long? after = null, string? lastEventId = null)
    {
        var sse = new SseClient(client);
        var url = $"/streams/{stream}/events" + (after.HasValue ? $"?after={after.Value}" : "");
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
        if (lastEventId is not null)
            request.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
        sse._response = await client.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, sse._cts.Token);
        sse._response.EnsureSuccessStatusCode();
        var body = await sse._response.Content.ReadAsStreamAsync(sse._cts.Token);
        sse._reader = new StreamReader(body, Encoding.UTF8);
        return sse;
    }

    public async Task<SseEvent> ReadEventAsync(TimeSpan? timeout = null)
    {
        using var timeoutCts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(10));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, timeoutCts.Token);
        string? id = null;
        string? eventType = null;
        var data = new StringBuilder();
        while (true)
        {
            var line = await ReadLineAsync(linked.Token)
                ?? throw new EndOfStreamException("SSE stream ended before a complete event was received.");
            if (line.Length == 0)
            {
                if (eventType is null && data.Length == 0)
                    continue;
                return new SseEvent(id, eventType ?? "message", data.ToString());
            }
            if (line.StartsWith("id:", StringComparison.Ordinal))
                id = line[3..].TrimStart();
            else if (line.StartsWith("event:", StringComparison.Ordinal))
                eventType = line[6..].TrimStart();
            else if (line.StartsWith("data:", StringComparison.Ordinal))
                data.Append(line[5..].TrimStart());
        }
    }

    private async Task<string?> ReadLineAsync(CancellationToken cancellationToken)
    {
        var readTask = _reader!.ReadLineAsync(cancellationToken).AsTask();
        return await readTask.WaitAsync(cancellationToken);
    }

    public async Task CancelAsync()
    {
        await _cts.CancelAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _response?.Dispose();
        _reader?.Dispose();
        _cts.Dispose();
    }
}
