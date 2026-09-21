# EventRelay

Local in-memory event publish/subscribe service (ASP.NET Core 8, C# 12). No database, message queue, or external services.

## API

- `POST /streams/{stream}/events` — publish a JSON event `{"type": "...", "data": {...}}`. Returns `201` with the full event (`id`, `stream`, `type`, `data`, `timestamp`). Ids are strictly increasing `long` per stream, starting at 1. Validation failures return `400` with `{"error": {"code", "message"}}` and never consume an id.
- `GET /streams/{stream}/events?after={id}` — SSE subscription (`text/event-stream`). Replays retained history after the cursor in id order, then hands off to live events without gaps or duplicates. The `Last-Event-ID` request header is honored when `after` is absent. Each SSE frame carries `id`, `event` (type), and `data` (JSON).

## Errors

- `400 invalid-stream / invalid-json / invalid-type / invalid-data / invalid-cursor` — malformed publish or subscription request.
- `409 cursor-expired` — cursor is older than the oldest retained event for the stream.
- SSE `event: error` with `{"code":"slow-consumer"}` — subscriber exceeded its pending queue; the connection is closed without blocking publishers or other subscribers.

## Configuration

Bounds are centralized in `EventRelayOptions` (`appsettings.json` section `EventRelay`):

- `MaxHistoryPerStream` (default 1000) — retained events per stream.
- `MaxSubscriberQueueSize` (default 256) — pending events per subscriber.

## Run

```bash
dotnet run --project src/EventRelay.Api
```

## Test

```bash
dotnet test
```

## Demo

```bash
./demo/demo.sh
```

Starts the service, publishes events, replays history via `after` and `Last-Event-ID`, shows a live subscription receiving a new event, and a validation error. A captured run is kept in `demo/demo-output.txt`.
