using System.Threading.Channels;
using Microsoft.Extensions.Options;

namespace EventRelay.Api.Domain;

public sealed class InMemoryEventStreamService : IEventStreamService
{
    private readonly object _gate = new();
    private readonly Dictionary<string, StreamState> _streams = new(StringComparer.Ordinal);
    private readonly EventRelayOptions _options;

    public InMemoryEventStreamService(IOptions<EventRelayOptions> options)
    {
        _options = options.Value;
        if (_options.MaxHistoryPerStream < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxHistoryPerStream must be >= 1.");
        if (_options.MaxSubscriberQueueSize < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxSubscriberQueueSize must be >= 1.");
    }

    public EventRecord Append(string stream, string type, System.Text.Json.JsonElement data)
    {
        lock (_gate)
        {
            var state = GetOrCreate(stream);
            var record = new EventRecord(
                checked(state.LastId + 1), stream, type, data.Clone(), DateTimeOffset.UtcNow);
            state.LastId = record.Id;
            state.History.Enqueue(record);
            while (state.History.Count > _options.MaxHistoryPerStream)
                state.History.Dequeue();

            for (var i = state.Subscribers.Count - 1; i >= 0; i--)
            {
                var subscriber = state.Subscribers[i];
                if (!subscriber.TryEnqueue(record))
                {
                    subscriber.MarkSlowConsumer();
                    state.Subscribers.RemoveAt(i);
                }
            }

            return record;
        }
    }

    public ISubscription Subscribe(string stream, long afterId)
    {
        if (afterId < 0)
            throw new InvalidCursorException($"Cursor must be >= 0, got {afterId}.");

        lock (_gate)
        {
            var state = GetOrCreate(stream);
            if (afterId < state.OldestRetainedId - 1)
                throw new StaleCursorException(stream, afterId, state.OldestRetainedId);

            var replay = new List<EventRecord>();
            foreach (var record in state.History)
            {
                if (record.Id > afterId)
                    replay.Add(record);
            }

            var subscription = new Subscription(stream, replay, _options.MaxSubscriberQueueSize, this);
            state.Subscribers.Add(subscription);
            return subscription;
        }
    }

    private void Remove(Subscription subscription)
    {
        lock (_gate)
        {
            if (_streams.TryGetValue(subscription.Stream, out var state))
                state.Subscribers.Remove(subscription);
        }
    }

    public int SubscriberCount(string stream)
    {
        lock (_gate)
        {
            return _streams.TryGetValue(stream, out var state) ? state.Subscribers.Count : 0;
        }
    }

    private StreamState GetOrCreate(string stream)
    {
        if (!_streams.TryGetValue(stream, out var state))
        {
            state = new StreamState();
            _streams[stream] = state;
        }
        return state;
    }

    private sealed class StreamState
    {
        public long LastId;
        public readonly Queue<EventRecord> History = new();
        public readonly List<Subscription> Subscribers = new();
        public long OldestRetainedId => History.Count > 0 ? History.Peek().Id : LastId + 1;
    }

    private sealed class Subscription : ISubscription
    {
        private readonly IReadOnlyList<EventRecord> _replay;
        private readonly Channel<EventRecord> _live;
        private readonly InMemoryEventStreamService _owner;
        private volatile bool _slowConsumer;
        private int _disposed;

        public Subscription(
            string stream,
            IReadOnlyList<EventRecord> replay,
            int queueCapacity,
            InMemoryEventStreamService owner)
        {
            Stream = stream;
            _replay = replay;
            _owner = owner;
            _live = Channel.CreateBounded<EventRecord>(new BoundedChannelOptions(queueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            });
        }

        public string Stream { get; }

        public bool TryEnqueue(EventRecord record)
        {
            if (_slowConsumer || Volatile.Read(ref _disposed) == 1)
                return true;
            return _live.Writer.TryWrite(record);
        }

        public void MarkSlowConsumer()
        {
            _slowConsumer = true;
            _live.Writer.TryComplete();
        }

        public async IAsyncEnumerable<SubscriptionItem> ReadAllAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var record in _replay)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return SubscriptionItem.FromEvent(record);
            }

            while (await _live.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_live.Reader.TryRead(out var record))
                    yield return SubscriptionItem.FromEvent(record);
            }

            if (_slowConsumer)
                yield return SubscriptionItem.SlowConsumer;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _live.Writer.TryComplete();
                _owner.Remove(this);
            }
        }
    }
}
