using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace EventRelay.Api.Domain;

public sealed class InMemoryEventStreamService : IEventStreamService
{
    private sealed class StreamState
    {
        public readonly object Sync = new();
        public long LastId;
        public readonly Queue<EventRecord> History = new();
        public readonly List<EventSubscription> Subscribers = new();
    }

    private readonly ConcurrentDictionary<string, StreamState> _streams = new(StringComparer.Ordinal);
    private readonly EventRelayOptions _options;

    public InMemoryEventStreamService(IOptions<EventRelayOptions> options)
    {
        _options = options.Value;
        if (_options.MaxHistoryPerStream < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxHistoryPerStream must be >= 1.");
        if (_options.SubscriberQueueCapacity < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "SubscriberQueueCapacity must be >= 1.");
    }

    public PublishOutcome Publish(string? stream, string? type, JsonElement data)
    {
        var error = ValidateStream(stream) ?? ValidateType(type) ?? ValidateData(data);
        if (error is not null)
            return new PublishOutcome(null, error);

        var state = _streams.GetOrAdd(stream!, _ => new StreamState());
        lock (state.Sync)
        {
            var id = state.LastId + 1;
            var record = new EventRecord(id, stream!, type!, data, DateTimeOffset.UtcNow);
            state.LastId = id;

            state.History.Enqueue(record);
            while (state.History.Count > _options.MaxHistoryPerStream)
                state.History.Dequeue();

            for (var i = state.Subscribers.Count - 1; i >= 0; i--)
            {
                var subscriber = state.Subscribers[i];
                if (!subscriber.TryOffer(record))
                {
                    subscriber.MarkSlow();
                    state.Subscribers.RemoveAt(i);
                }
            }

            return new PublishOutcome(record, null);
        }
    }

    public SubscribeOutcome Subscribe(string? stream, long after)
    {
        var error = ValidateStream(stream);
        if (error is not null)
            return new SubscribeOutcome(null, error);
        if (after < 0)
            return new SubscribeOutcome(null, ApiError.BadRequest("invalid_cursor", "Cursor 'after' must be >= 0."));

        var state = _streams.GetOrAdd(stream!, _ => new StreamState());
        lock (state.Sync)
        {
            if (state.History.Count > 0 && after < state.History.Peek().Id - 1)
            {
                return new SubscribeOutcome(null, ApiError.Conflict(
                    "cursor_expired",
                    $"Cursor {after} is older than the retained history for stream '{stream}'. Oldest retained event id is {state.History.Peek().Id}."));
            }

            var subscription = new EventSubscription(_options.SubscriberQueueCapacity);
            var slow = false;
            foreach (var record in state.History)
            {
                if (record.Id <= after)
                    continue;
                if (!subscription.TryOffer(record))
                {
                    slow = true;
                    break;
                }
            }

            if (slow)
            {
                subscription.MarkSlow();
            }
            else
            {
                state.Subscribers.Add(subscription);
            }

            return new SubscribeOutcome(subscription, null);
        }
    }

    public void Unsubscribe(string stream, EventSubscription subscription)
    {
        if (!_streams.TryGetValue(stream, out var state))
            return;
        lock (state.Sync)
        {
            state.Subscribers.Remove(subscription);
        }
        subscription.Complete();
    }

    private ApiError? ValidateStream(string? stream)
    {
        if (string.IsNullOrWhiteSpace(stream))
            return ApiError.BadRequest("invalid_stream", "Stream name must be a non-empty, non-whitespace value.");
        if (stream.Length > _options.MaxStreamNameLength)
            return ApiError.BadRequest("invalid_stream", $"Stream name must be at most {_options.MaxStreamNameLength} characters.");
        return null;
    }

    private ApiError? ValidateType(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
            return ApiError.BadRequest("invalid_type", "Event type must be a non-empty, non-whitespace value.");
        if (type.Length > _options.MaxTypeLength)
            return ApiError.BadRequest("invalid_type", $"Event type must be at most {_options.MaxTypeLength} characters.");
        return null;
    }

    private static ApiError? ValidateData(JsonElement data)
    {
        if (data.ValueKind == JsonValueKind.Undefined)
            return ApiError.BadRequest("invalid_data", "Event data is required and must be a valid JSON value.");
        return null;
    }
}

