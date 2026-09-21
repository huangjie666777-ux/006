namespace EventRelay.Api.Domain;

public sealed class EventRelayOptions
{
    public const string SectionName = "EventRelay";

    public int MaxHistoryPerStream { get; set; } = 1000;
    public int SubscriberQueueCapacity { get; set; } = 256;
    public int MaxStreamNameLength { get; set; } = 128;
    public int MaxTypeLength { get; set; } = 128;
}

