using System.Text.Json;

namespace EventRelay.Api.Domain;

public interface IEventStreamService
{
    EventRecord Append(string stream, string type, JsonElement data);

    ISubscription Subscribe(string stream, long afterId);

    int SubscriberCount(string stream);
}
