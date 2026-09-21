namespace EventRelay.Api.Domain;

public abstract class EventRelayException(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}

public sealed class StaleCursorException(string stream, long afterId, long oldestRetainedId)
    : EventRelayException("cursor-expired",
        $"Cursor {afterId} is older than the oldest retained event id {oldestRetainedId} on stream '{stream}'.")
{
    public long OldestRetainedId { get; } = oldestRetainedId;
}

public sealed class InvalidCursorException(string message)
    : EventRelayException("invalid-cursor", message);
