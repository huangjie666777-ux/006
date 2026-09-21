using System.Text.Json;

namespace EventRelay.Api.Domain;

public sealed record EventRecord(
    long Id,
    string Stream,
    string Type,
    JsonElement Data,
    DateTimeOffset Timestamp);
