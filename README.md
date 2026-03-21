# event-bus

The Lunaris Event Bus is a Unix socket daemon that routes structured events between system components. Producers send events; consumers subscribe to event types and receive matching events.

This is the central nervous system of the Lunaris data pipeline. Every component that wants to record or react to system activity goes through here.

## What it does

- Accepts producer connections on one Unix socket, consumer connections on another
- Routes events to consumers based on subscription filters (exact match, prefix match, or wildcard)
- Handles backpressure per consumer: a slow consumer gets dropped events, not a stalled bus
- Validates incoming events against required fields before dispatching

## Protocol

Events are length-prefixed protobuf messages. The schema lives in `proto/event.proto`.

**Producer:** connect to `LUNARIS_PRODUCER_SOCKET`, send `[4-byte big-endian length][protobuf Event]`

**Consumer:** connect to `LUNARIS_CONSUMER_SOCKET`, send registration:
```
<consumer-id>\n
<event-type1>,<event-type2>,...\n
```
Then receive `[4-byte big-endian length][protobuf Event]` messages as they arrive.

Event type filters support:
- Exact match: `file.opened`
- Prefix match: `file.` matches all file events
- Wildcard: `*` matches everything

## Running

```bash
LUNARIS_PRODUCER_SOCKET=/run/lunaris/event-bus-producer.sock \
LUNARIS_CONSUMER_SOCKET=/run/lunaris/event-bus-consumer.sock \
RUST_LOG=info \
./event-bus
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `LUNARIS_PRODUCER_SOCKET` | `/run/lunaris/event-bus-producer.sock` | Producer socket path |
| `LUNARIS_CONSUMER_SOCKET` | `/run/lunaris/event-bus-consumer.sock` | Consumer socket path |

## Testing

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Part of

[Lunaris](https://github.com/lunaris-sys) — a Linux desktop OS built around a system-wide knowledge graph.
