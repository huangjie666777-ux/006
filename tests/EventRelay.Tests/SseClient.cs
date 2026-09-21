using System.Net.Http.Headers;

namespace EventRelay.Tests;

public sealed record SseFrame(string? Id, string? Event, string? Data);

public static class SseClient
{
    public static async Task<HttpResponseMessage> ConnectAsync(
        HttpClient client, string stream, long? after = null, string? lastEventId = null,
        CancellationToken cancellationToken = default)
    {
        var url = $"/streams/{stream}/events";
        if (after.HasValue)
            url += $"?after={after.Value}";
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        if (lastEventId is not null)
            request.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
        return await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
    }

    public static async Task<SseFrame?> ReadFrameAsync(Stream stream, CancellationToken cancellationToken)
    {
        string? id = null, eventName = null, data = null;
        var sawField = false;
        while (true)
        {
            var line = await ReadLineAsync(stream, cancellationToken);
            if (line is null)
                return sawField ? new SseFrame(id, eventName, data) : null;
            if (line.Length == 0)
                return new SseFrame(id, eventName, data);
            if (line.StartsWith(':'))
            {
                sawField = true;
                continue;
            }
            sawField = true;
            if (line.StartsWith("id: ")) id = line[4..];
            else if (line.StartsWith("event: ")) eventName = line[7..];
            else if (line.StartsWith("data: ")) data = line[6..];
        }
    }

    public static async Task<List<SseFrame>> ReadFramesAsync(
        HttpResponseMessage response, int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        var frames = new List<SseFrame>();
        while (frames.Count < count)
        {
            var frame = await ReadFrameAsync(stream, cts.Token);
            if (frame is null)
                break;
            if (frame.Id is null && frame.Event is null && frame.Data is null)
                continue;
            frames.Add(frame);
        }
        return frames;
    }

    private static async Task<string?> ReadLineAsync(Stream stream, CancellationToken cancellationToken)
    {
        var buffer = new List<byte>();
        var single = new byte[1];
        while (true)
        {
            var read = await stream.ReadAsync(single, cancellationToken);
            if (read == 0)
                return buffer.Count == 0 ? null : System.Text.Encoding.UTF8.GetString(buffer.ToArray());
            if (single[0] == (byte)'\n')
                break;
            if (single[0] != (byte)'\r')
                buffer.Add(single[0]);
        }
        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}

