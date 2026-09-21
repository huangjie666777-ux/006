using System.Text.Json;
using EventRelay.Api.Domain;
using Microsoft.AspNetCore.Mvc;

namespace EventRelay.Api.Http;

public static class EventEndpoints
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static IEndpointRouteBuilder MapEventEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/streams/{stream}/events", PublishAsync);
        app.MapGet("/streams/{stream}/events", SubscribeAsync);
        return app;
    }

    private static async Task<IResult> PublishAsync(
        string stream,
        HttpRequest request,
        IEventStreamService events,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(stream))
            return Error(StatusCodes.Status400BadRequest, "invalid-stream", "Stream name must not be empty.");

        PublishRequest? body;
        try
        {
            body = await JsonSerializer.DeserializeAsync<PublishRequest>(
                request.Body, JsonOptions, cancellationToken);
        }
        catch (JsonException ex)
        {
            return Error(StatusCodes.Status400BadRequest, "invalid-json", $"Request body is not valid JSON: {ex.Message}");
        }

        if (body is null)
            return Error(StatusCodes.Status400BadRequest, "invalid-json", "Request body must be a JSON object.");
        if (string.IsNullOrWhiteSpace(body.Type))
            return Error(StatusCodes.Status400BadRequest, "invalid-type", "Field 'type' must be a non-empty string.");
        if (body.Data is not { } data || data.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
            return Error(StatusCodes.Status400BadRequest, "invalid-data", "Field 'data' is required and must not be null.");

        var record = events.Append(stream, body.Type, data);
        return Results.Created(
            $"/streams/{Uri.EscapeDataString(stream)}/events/{record.Id}",
            ToDto(record));
    }

    private static async Task SubscribeAsync(
        string stream,
        HttpContext context,
        IEventStreamService events,
        [FromQuery(Name = "after")] string? after)
    {
        if (string.IsNullOrWhiteSpace(stream))
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, "invalid-stream", "Stream name must not be empty.");
            return;
        }

        long afterId;
        var lastEventId = context.Request.Headers["Last-Event-ID"].ToString();
        if (!string.IsNullOrEmpty(after))
        {
            if (!long.TryParse(after, out afterId) || afterId < 0)
            {
                await WriteErrorAsync(context, StatusCodes.Status400BadRequest, "invalid-cursor",
                    $"Query parameter 'after' must be a non-negative integer, got '{after}'.");
                return;
            }
        }
        else if (!string.IsNullOrEmpty(lastEventId))
        {
            if (!long.TryParse(lastEventId, out afterId) || afterId < 0)
            {
                await WriteErrorAsync(context, StatusCodes.Status400BadRequest, "invalid-cursor",
                    $"Last-Event-ID header must be a non-negative integer, got '{lastEventId}'.");
                return;
            }
        }
        else
        {
            afterId = 0;
        }

        ISubscription subscription;
        try
        {
            subscription = events.Subscribe(stream, afterId);
        }
        catch (InvalidCursorException ex)
        {
            await WriteErrorAsync(context, StatusCodes.Status400BadRequest, ex.Code, ex.Message);
            return;
        }
        catch (StaleCursorException ex)
        {
            await WriteErrorAsync(context, StatusCodes.Status409Conflict, ex.Code, ex.Message);
            return;
        }

        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";
        await context.Response.Body.FlushAsync();

        try
        {
            await foreach (var item in subscription.ReadAllAsync(context.RequestAborted))
            {
                if (item.Kind == SubscriptionItemKind.SlowConsumer)
                {
                    await WriteSseAsync(context, "error",
                        """{"code":"slow-consumer","message":"Subscriber fell behind; closing connection."}""",
                        id: null);
                    break;
                }

                var record = item.Event!;
                await WriteSseAsync(context, record.Type,
                    JsonSerializer.Serialize(ToDto(record), JsonOptions),
                    record.Id.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            // Client disconnected; nothing to send.
        }
        finally
        {
            subscription.Dispose();
        }
    }

    private static async Task WriteSseAsync(HttpContext context, string eventType, string data, string? id)
    {
        var writer = context.Response.BodyWriter;
        if (id is not null)
            await writer.WriteAsync(System.Text.Encoding.UTF8.GetBytes($"id: {id}\n"));
        await writer.WriteAsync(System.Text.Encoding.UTF8.GetBytes($"event: {eventType}\n"));
        await writer.WriteAsync("data: "u8.ToArray());
        await writer.WriteAsync(System.Text.Encoding.UTF8.GetBytes(data));
        await writer.WriteAsync("\n\n"u8.ToArray());
        await writer.FlushAsync();
    }

    private static object ToDto(EventRecord record) => new
    {
        id = record.Id,
        stream = record.Stream,
        type = record.Type,
        data = record.Data,
        timestamp = record.Timestamp,
    };

    private static IResult Error(int statusCode, string code, string message) =>
        Results.Json(new { error = new { code, message } }, statusCode: statusCode);

    private static async Task WriteErrorAsync(HttpContext context, int statusCode, string code, string message)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsJsonAsync(new { error = new { code, message } });
    }

    private sealed class PublishRequest
    {
        public string? Type { get; set; }
        public JsonElement? Data { get; set; }
    }
}
