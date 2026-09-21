using EventRelay.Api.Domain;
using EventRelay.Api.Http;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<EventRelayOptions>(
    builder.Configuration.GetSection(EventRelayOptions.SectionName));
builder.Services.AddSingleton<IEventStreamService, InMemoryEventStreamService>();

var app = builder.Build();
app.MapGet("/", () => Results.Ok(new { service = "event-relay", status = "ready" }));
app.MapEventEndpoints();
app.Run();

public partial class Program { }
