using System.Text.Json;

namespace EventRelay.Api.Domain;

public readonly record struct PublishOutcome(EventRecord? Record, ApiError? Error)
{
    public bool IsSuccess => Error is null;
}

public readonly record struct SubscribeOutcome(EventSubscription? Subscription, ApiError? Error)
{
    public bool IsSuccess => Error is null;
}

public interface IEventStreamService
{
    PublishOutcome Publish(string? stream, string? type, JsonElement data);
    SubscribeOutcome Subscribe(string? stream, long after);
    void Unsubscribe(string stream, EventSubscription subscription);
}

