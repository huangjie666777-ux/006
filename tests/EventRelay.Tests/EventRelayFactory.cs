using EventRelay.Api.Domain;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace EventRelay.Tests;

public sealed class EventRelayFactory : WebApplicationFactory<Program>
{
    public int MaxHistoryPerStream { get; init; } = 1000;
    public int SubscriberQueueCapacity { get; init; } = 256;

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            services.Configure<EventRelayOptions>(options =>
            {
                options.MaxHistoryPerStream = MaxHistoryPerStream;
                options.SubscriberQueueCapacity = SubscriberQueueCapacity;
            });
        });
    }
}

