using System.Text.Json;
using EventRelay.Api.Domain;
using Microsoft.Extensions.Options;

namespace EventRelay.Tests;

public sealed class ServiceUnitTests
{
    private static InMemoryEventStreamService CreateService(int history = 100, int capacity = 4) =>
        new(Options.Create(new EventRelayOptions
        {
            MaxHistoryPerStream = history,
            SubscriberQueueCapacity = capacity
        }));

    private static JsonElement Data(int value) => JsonDocument.Parse($"{{\"n\":{value}}}").RootElement.Clone();

    [Fact]
    public void SlowSubscriber_IsMarkedSlowAndCompleted_WithoutBlockingPublish()
    {
        var service = CreateService(capacity: 2);
        var outcome = service.Subscribe("s", 0);
        Assert.True(outcome.IsSuccess);
        var subscription = outcome.Subscription!;

        for (var i = 0; i < 5; i++)
            Assert.True(service.Publish("s", "t", Data(i)).IsSuccess);

        Assert.True(subscription.Slow);
        var drained = 0;
        while (subscription.Reader.TryRead(out _))
            drained++;
        Assert.Equal(2, drained);
    }

    [Fact]
    public void SlowSubscriber_DoesNotAffectOtherSubscribers()
    {
        var service = CreateService(capacity: 1);
        var slow = service.Subscribe("s", 0).Subscription!;
        var healthy = service.Subscribe("s", 0).Subscription!;

        Assert.True(service.Publish("s", "t", Data(1)).IsSuccess);
        Assert.True(healthy.Reader.TryRead(out _)); // healthy drains immediately
        Assert.True(service.Publish("s", "t", Data(2)).IsSuccess);

        Assert.True(slow.Slow);
        Assert.False(healthy.Slow);
        Assert.True(healthy.Reader.TryRead(out var second));
        Assert.Equal(2, second.Id);
    }

    [Fact]
    public void ConcurrentPublish_AssignsDenseStrictlyIncreasingIds()
    {
        var service = CreateService();
        const int total = 2000;
        var ids = new long[total];
        Parallel.For(0, total, i =>
        {
            var outcome = service.Publish("s", "t", Data(i));
            Assert.True(outcome.IsSuccess);
            ids[i] = outcome.Record!.Id;
        });
        Array.Sort(ids);
        Assert.Equal(Enumerable.Range(1, total).Select(i => (long)i), ids);
    }

    [Fact]
    public void Subscribe_CursorBeyondLastId_WaitsForLiveEventsOnly()
    {
        var service = CreateService();
        service.Publish("s", "t", Data(1));
        var subscription = service.Subscribe("s", 100).Subscription!;
        Assert.False(subscription.Reader.TryRead(out _));
        service.Publish("s", "t", Data(2));
        Assert.True(subscription.Reader.TryRead(out var record));
        Assert.Equal(2, record.Id);
    }

    [Fact]
    public void Unsubscribe_StopsDelivery()
    {
        var service = CreateService();
        var subscription = service.Subscribe("s", 0).Subscription!;
        service.Unsubscribe("s", subscription);
        service.Publish("s", "t", Data(1));
        Assert.False(subscription.Reader.TryRead(out _));
    }
}

