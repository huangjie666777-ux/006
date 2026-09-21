namespace EventRelay.Api.Domain;

public sealed class EventRelayOptions
{
    public const string SectionName = "EventRelay";

    public int MaxHistoryPerStream { get; set; } = 1000;

    public int MaxSubscriberQueueSize { get; set; } = 256;
}
