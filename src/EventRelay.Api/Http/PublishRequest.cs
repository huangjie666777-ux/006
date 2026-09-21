using System.Text.Json;
using System.Text.Json.Serialization;

namespace EventRelay.Api.Http;

public sealed class PublishRequest
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("data")]
    public JsonElement Data { get; set; }
}

