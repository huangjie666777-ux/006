namespace EventRelay.Api.Domain;

public enum SubscriptionItemKind
{
    Event,
    SlowConsumer,
}

public readonly record struct SubscriptionItem(SubscriptionItemKind Kind, EventRecord? Event)
{
    public static SubscriptionItem FromEvent(EventRecord record) => new(SubscriptionItemKind.Event, record);

    public static readonly SubscriptionItem SlowConsumer = new(SubscriptionItemKind.SlowConsumer, null);
}

public interface ISubscription : IDisposable
{
    string Stream { get; }

    IAsyncEnumerable<SubscriptionItem> ReadAllAsync(CancellationToken cancellationToken);
}
