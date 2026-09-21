using System.Text.Json;
using EventRelay.Api.Domain;
using EventRelay.Api.Http;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<EventRelayOptions>(builder.Configuration.GetSection(EventRelayOptions.SectionName));
builder.Services.AddSingleton<IEventStreamService, InMemoryEventStreamService>();

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new { service = "event-relay", status = "ready" }));

app.MapPost("/streams/{stream}/events", async (string stream, HttpContext context, IEventStreamService service) =>
{
    PublishRequest? request;
    try
    {
        request = await JsonSerializer.DeserializeAsync<PublishRequest>(
            context.Request.Body,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true },
            context.RequestAborted);
    }
    catch (JsonException)
    {
        return ApiError.BadRequest("invalid_json", "Request body must be a valid JSON object.").ToHttpResult();
    }

    if (request is null)
        return ApiError.BadRequest("invalid_json", "Request body must be a valid JSON object.").ToHttpResult();

    var outcome = service.Publish(stream, request.Type, request.Data);
    if (outcome.Error is { } error)
        return error.ToHttpResult();

    var record = outcome.Record!;
    return Results.Created($"/streams/{stream}/events/{record.Id}", record);
});

app.MapGet("/streams/{stream}/events", (string stream, long? after, HttpContext context, IEventStreamService service) =>
    SseEndpoint.Subscribe(stream, after, context, service));

app.Run();

public partial class Program { }

