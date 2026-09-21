using System.Threading.Channels;

namespace EventRelay.Api.Domain;

public sealed class EventSubscription
{
    private readonly Channel<EventRecord> _channel;
    private int _slow;

    public EventSubscription(int capacity)
    {
        _channel = Channel.CreateBounded<EventRecord>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public ChannelReader<EventRecord> Reader => _channel.Reader;
    public bool Slow => Volatile.Read(ref _slow) == 1;

    internal bool TryOffer(EventRecord record) => _channel.Writer.TryWrite(record);

    internal void MarkSlow()
    {
        Interlocked.Exchange(ref _slow, 1);
        _channel.Writer.TryComplete();
    }

    internal void Complete() => _channel.Writer.TryComplete();
}

