var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.MapGet("/", () => Results.Ok(new { service = "event-relay", status = "ready" }));
app.Run();
public partial class Program { }
