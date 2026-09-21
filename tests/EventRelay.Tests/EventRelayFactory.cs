using EventRelay.Api.Domain;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace EventRelay.Tests;

public class EventRelayFactory : WebApplicationFactory<Program>
{
    public int MaxHistoryPerStream { get; set; } = 1000;

    public int MaxSubscriberQueueSize { get; set; } = 256;

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            services.Configure<EventRelayOptions>(options =>
            {
                options.MaxHistoryPerStream = MaxHistoryPerStream;
                options.MaxSubscriberQueueSize = MaxSubscriberQueueSize;
            });
        });
    }
}
