# hirnok

A fast, minimal NATS messaging library for Go.

**Status:** early. API is not yet stable. Core NATS + JetStream pub/sub work; auth, tracing hooks, retry middleware, KV/Object store, and similar are deliberately out of scope until needed.

## Why

Most messaging abstractions in Go pile on router + middleware + envelope + publisher/subscriber adapters when all you actually need is "connect, register a handler." `hirnok` is the thin layer that does only that — and pairs each subscription with a bounded queue and a small worker pool, so you get backpressure and concurrency without writing them yourself.

Design rules:

- **No envelopes.** Handlers receive `*Message` (which embeds `*nats.Msg`) or `*JetMessage` (which embeds `jetstream.Msg`). Whatever schema you want lives in your code.
- **No copies.** Pointers and interface values pass through unchanged from the NATS client to your handler.
- **No router, no middleware chain.** Wrap your own handler if you want retry/tracing/DLQ.
- **Context tracks the ack window.** For JetStream, the handler's `ctx` cancels just before server-side `AckWait` would redeliver. Respect `ctx` and your work stops doing redundant effort on slow handlers.

## Install

```bash
go get github.com/rlsvr/hirnok
```

Requires Go 1.26+.

## Packages

| Path         | What it is                                                          |
|--------------|---------------------------------------------------------------------|
| `pkg/queue`  | Bounded generic FIFO `Queue[T]` — the building block.                |
| `pkg/worker` | Generic worker pool that drains a `Queue[T]` into a dispatch func.   |
| `pkg/nats`   | NATS connection + subscribe/publish + JetStream consumer/publisher. |

`pkg/queue` and `pkg/worker` are usable on their own — you don't need NATS to get value out of them. Example:

```go
q := queue.New[Job](64)
pool := worker.Run(ctx, 4, q, func(ctx context.Context, j Job) {
    j.Do(ctx)
})

// ... produce ...
_ = q.Push(ctx, Job{...})

// shutdown:
q.Close()
_ = pool.Wait(ctx)
```

## Quick start

### Core NATS

```go
import (
    "context"
    qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

conn, err := qpnats.Connect(qpnats.Config{
    URL:       "nats://localhost:4222",
    Name:      "my-service",
    QueueSize: 64,
    Workers:   4,
})
if err != nil { panic(err) }
defer conn.Close()

sub, _ := conn.Subscribe(ctx, "events.*", func(ctx context.Context, m *qpnats.Message) error {
    // m embeds *nats.Msg — m.Data, m.Subject, m.Header all work directly.
    log.Printf("got %s on %s", m.Data, m.Subject)
    return nil
})
defer sub.Unsubscribe()

_ = conn.Publish("events.user.signup", []byte(`{"id":1}`))
```

Graceful shutdown:

```go
sub.Unsubscribe()                                  // stop accepting new messages
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
_ = sub.Wait(ctx)                                  // block until workers actually exit
```

`Sub.Wait` and `Consumer.Wait` block until every worker goroutine has returned (after its current handler call completes). `Stopped() <-chan struct{}` is the same signal as a channel for use in your own `select`.

### JetStream

```go
js, _ := conn.JetStream()

// Pre-create the stream once (or do it elsewhere).
_, _ = js.Raw().CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:     "ORDERS",
    Subjects: []string{"orders.>"},
})

cons, err := js.Consume(ctx, "ORDERS", jetstream.ConsumerConfig{
    Durable:       "orders-worker",
    FilterSubject: "orders.>",
    AckWait:       30 * time.Second,
}, func(ctx context.Context, m *qpnats.JetMessage) error {
    // m embeds jetstream.Msg — m.Data(), m.Subject(), m.Ack() all work.
    // Return nil → Ack. Return err → Nak. ctx-expired → neither (server redelivers).
    return process(ctx, m.Data())
})
if err != nil { panic(err) }
defer cons.Stop()

ack, _ := js.Publish(ctx, "orders.new", []byte(`{"id":1}`))
log.Printf("stored at seq=%d", ack.Sequence)
```

### Context & AckWait

JetStream handlers receive a `ctx` that cancels **before** server-side `AckWait` expires (default: cancel at `AckWait - AckWait/10`; tunable via `Config.AckBuffer`). Two reasons:

1. When `ctx` expires, the wrapper *doesn't* call Ack or Nak — it lets the server redeliver naturally. No double-processing collision.
2. Downstream callees (DB, HTTP, pipelines) bail out via the same `ctx`, so a 5-minute-old in-flight handler doesn't keep hammering the DB while the redelivered copy is already running on another worker.

Handlers must thread `ctx` through. Standard Go practice; just don't ignore it.

For core NATS there's no AckWait, so the handler `ctx` is the parent subscription ctx — it cancels only on `Unsubscribe`, `Close`, or parent cancel.

## Concurrency

`Config.Workers` sets the per-subscription worker count. Each subscription owns a bounded `pkg/queue` and that many goroutines drain it concurrently. Use `Workers: 1` for strict serial processing; higher values fan out at the cost of FIFO completion order.

## Development

```bash
make build              # go build ./...
make test               # go test -race ./...
make test-verbose       # ...with -v
make lint               # golangci-lint run ./...
make fmt                # golangci-lint fmt
make lint-fix           # golangci-lint run --fix
make tidy               # go mod tidy
make check              # fmt + lint + tidy
```

Tests use an embedded `nats-server` (no external broker needed). The full suite — including JetStream redelivery, dedupe, ack-window timing — runs in a few seconds.

## Smoke & stress

Programs in `test/` exercise the library against a **real** NATS broker (not the embedded one), useful for catching regressions that only show up against a real server and for measuring throughput.

```bash
make nats               # start dockerized nats-server -js (Ctrl-C to stop)

# in another terminal:
make smoke              # core + JS round-trip; exits non-zero on failure
make stress-core        # producers × workers × messages throughput
make stress-jetstream   # JS publish + consume throughput
make stress-backpressure # slow handler / fast producer; verifies clean shutdown
make stress-burst       # cold-idle → burst; measures first-msg latency + drain
make stress             # run all four sequentially
make nats-stop          # tear down the broker
```

Each stress bench takes flags — e.g. `go run ./test/stress/core --producers 8 --workers 16 --messages 1000000`. See each program's `--help`.

`test/setup.sh` uses Docker by default (`nats:latest`). Set `USE_BINARY=1` to use a locally-installed `nats-server` instead.

## What's intentionally not here (yet)

- Auth: nkeys, creds files, tokens. Add to `Config` when needed.
- Tracing / metrics hooks. Wrap your handler.
- Retry middleware / dead-letter helpers. Wrap your handler.
- `Term` semantics (a sentinel `ErrTerminate` is the planned door).
- Pull-consumer batch fetch (`Fetch`) for high throughput.
- KV and Object Store wrappers — `js.Raw()` gives you the underlying jetstream context if you need them today.
