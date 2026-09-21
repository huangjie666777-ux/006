# EventRelay

Local in-memory event publish/subscribe service built with C# 12 and ASP.NET Core 8.
No database, message broker or external service is used; all state lives in memory.

## API

- `POST /streams/{stream}/events` — publish a JSON event `{"type": "...", "data": {...}}`.
  Returns `201 Created` with the full stored event. Each stream assigns ids as a strictly
  increasing `long` starting at 1. Validation failures return `400` with
  `{"error": {"code", "message"}}` and never consume an id.
- `GET /streams/{stream}/events` — Server-Sent Events subscription.
  - `?after=N` query parameter or `Last-Event-ID` request header selects the cursor.
  - History after the cursor is replayed in id order, then the stream switches seamlessly
    to live events (no gaps, no duplicates).
  - Each frame carries SSE `id`, `event` (event type) and `data` (JSON payload).
  - A cursor older than the retained history returns `409` with code `cursor_expired`.
  - Slow consumers that exceed their queue capacity receive an `event: slow-consumer`
    frame and the subscription is closed; publishers are never blocked.

## Configuration

All limits are centralized in `EventRelayOptions` (see `src/EventRelay.Api/appsettings.json`):

| Key | Default | Meaning |
| --- | --- | --- |
| `EventRelay:MaxHistoryPerStream` | 1000 | retained events per stream |
| `EventRelay:SubscriberQueueCapacity` | 256 | pending events per subscriber |
| `EventRelay:MaxStreamNameLength` | 128 | stream name limit |
| `EventRelay:MaxTypeLength` | 128 | event type limit |

## Run

```bash
dotnet run --project src/EventRelay.Api
```

## Test

```bash
dotnet test
```

Integration tests (xunit + `Microsoft.AspNetCore.Mvc.Testing`) cover history replay,
`Last-Event-ID`, the replay-to-live handoff under concurrent publish, subscriber
isolation, slow consumers, cursor expiry (409) and invalid input. Unit tests cover the
in-memory domain service directly.

## Demo

```bash
./demo/demo.sh        # starts the app, publishes, replays and live-subscribes
```

A captured run is kept in `demo/output.txt`.

