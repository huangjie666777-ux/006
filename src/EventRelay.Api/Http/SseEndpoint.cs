using System.Text.Json;
using EventRelay.Api.Domain;
using Microsoft.Extensions.Primitives;

namespace EventRelay.Api.Http;

public static class SseEndpoint
{
    private static readonly JsonSerializerOptions ErrorJsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public static async Task Subscribe(string stream, long? after, HttpContext context, IEventStreamService service)
    {
        long cursor = 0;
        if (after.HasValue)
        {
            cursor = after.Value;
        }
        else if (context.Request.Headers.TryGetValue("Last-Event-ID", out var lastEventId) && !StringValues.IsNullOrEmpty(lastEventId))
        {
            if (!long.TryParse(lastEventId.ToString(), out cursor))
            {
                await WriteError(context, ApiError.BadRequest("invalid_cursor", "Last-Event-ID header must be a valid integer."));
                return;
            }
        }

        if (cursor < 0)
        {
            await WriteError(context, ApiError.BadRequest("invalid_cursor", "Cursor must be >= 0."));
            return;
        }

        var outcome = service.Subscribe(stream, cursor);
        if (outcome.Error is { } subscribeError)
        {
            await WriteError(context, subscribeError);
            return;
        }

        var subscription = outcome.Subscription!;
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";

        var cancellation = context.RequestAborted;
        try
        {
            await WriteFrame(context, ": subscribed", cancellation);

            while (await subscription.Reader.WaitToReadAsync(cancellation))
            {
                while (subscription.Reader.TryRead(out var record))
                {
                    var frame = $"id: {record.Id}\nevent: {record.Type}\ndata: {record.Data.GetRawText()}";
                    await WriteFrame(context, frame, cancellation);
                }
            }

            if (subscription.Slow)
            {
                var payload = JsonSerializer.Serialize(
                    new { error = new { code = "slow_consumer", message = "Subscriber queue capacity exceeded; subscription closed." } },
                    ErrorJsonOptions);
                await WriteFrame(context, $"event: slow-consumer\ndata: {payload}", CancellationToken.None);
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
        }
        finally
        {
            service.Unsubscribe(stream, subscription);
        }
    }

    private static async Task WriteFrame(HttpContext context, string frame, CancellationToken cancellation)
    {
        await context.Response.WriteAsync(frame + "\n\n", cancellation);
        await context.Response.Body.FlushAsync(cancellation);
    }

    private static async Task WriteError(HttpContext context, ApiError error)
    {
        context.Response.StatusCode = error.Status;
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsJsonAsync(new { error = new { code = error.Code, message = error.Message } });
    }
}
