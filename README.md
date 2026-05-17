# hirnok

A small Go library for NATS handlers with bounded queues, worker pools, and
JetStream ack handling.

**Status:** early. The API is still pre-v1.

## What It Is

`hirnok` is intentionally thin:

- Handlers receive native NATS types: `*nats.Msg` for core NATS and
  `jetstream.Msg` for JetStream.
- Every subscription or consumer owns a bounded queue and a worker pool.
- JetStream handlers get a context tied to `AckWait`, so slow work can stop
  before the server redelivers the message.
- Middleware is plain function wrapping. There is no router or custom envelope
  type in the core API.

## Install

```bash
go get github.com/rlsvr/hirnok
```

Requires Go 1.25+.

## Packages

| Package | Purpose |
|---|---|
| `pkg/nats` | NATS connection, core subscribe/publish, JetStream publish/consume. |
| `pkg/queue` | Bounded generic FIFO queue. |
| `pkg/worker` | Generic worker pool over a queue. |
| `pkg/middleware` | Handler decorators: recover, retry, DLQ. |
| `pkg/middleware/otel` | OpenTelemetry decorators and header carrier. |

## Core NATS

```go
import (
    "context"
    "log"

    natsio "github.com/nats-io/nats.go"
    qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

conn, err := qpnats.Connect(qpnats.Config{
    URL:       "nats://localhost:4222",
    Name:      "orders-api",
    QueueSize: 64,
    Workers:   4,
})
if err != nil {
    panic(err)
}
defer conn.Close()

sub, err := conn.Subscribe(ctx, "orders.*", func(ctx context.Context, m *natsio.Msg) error {
    log.Printf("subject=%s body=%s", m.Subject, m.Data)
    return nil
})
if err != nil {
    panic(err)
}
defer sub.Stop()

_ = conn.Publish("orders.created", []byte(`{"id":"ord_1"}`))
```

Use `PublishMsg` when you need headers or reply subjects:

```go
msg := &natsio.Msg{
    Subject: "orders.created",
    Data:    payload,
    Header:  natsio.Header{},
}
msg.Header.Set("X-Request-Id", requestID)

if err := conn.PublishMsg(msg); err != nil {
    return err
}
```

## JetStream

Create or fetch streams with `js.Raw()` when you need the underlying nats.go
JetStream API:

```go
js, err := conn.JetStream()
if err != nil {
    panic(err)
}

_, _ = js.Raw().CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:     "ORDERS",
    Subjects: []string{"orders.>"},
})
```

### Pull Consumer

`NewConsumer` creates or updates a pull consumer, starts delivery, and runs the
handler through hirnok's queue and worker pool.

```go
cons, err := js.NewConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
    Durable:       "orders-worker",
    FilterSubject: "orders.>",
    AckWait:       30 * time.Second,
}, func(ctx context.Context, m jetstream.Msg) error {
    return processOrder(ctx, m.Data())
})
if err != nil {
    panic(err)
}
defer cons.Stop()
```

### Push Consumer

`NewPushConsumer` creates or updates a push consumer and uses the same handler,
queue, worker, and ack path as `NewConsumer`.

Set `DeliverSubject` and `DeliverGroup` when you want stable queue-style push
delivery across service instances. If `DeliverSubject` is empty, hirnok uses a
generated NATS inbox, which is convenient for single-process consumers.

```go
cons, err := js.NewPushConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
    Durable:        "orders-push",
    DeliverSubject: "orders.deliver",
    DeliverGroup:   "orders-workers",
    FilterSubject:  "orders.>",
    AckWait:        30 * time.Second,
}, func(ctx context.Context, m jetstream.Msg) error {
    return processOrder(ctx, m.Data())
})
if err != nil {
    panic(err)
}
defer cons.Stop()
```

### Publishing

```go
ack, err := js.Publish(ctx, "orders.created", payload)
if err != nil {
    return err
}
log.Printf("stored at stream sequence %d", ack.Sequence)
```

Use `PublishMsg` when you need JetStream headers such as dedupe IDs:

```go
msg := &natsio.Msg{
    Subject: "orders.created",
    Data:    payload,
    Header:  natsio.Header{},
}
msg.Header.Set(jetstream.MsgIDHeader, orderID)

ack, err := js.PublishMsg(ctx, msg)
```

## Ack Semantics

JetStream handlers return errors to control the final ack action:

| Handler result | JetStream action | Meaning |
|---|---|---|
| `nil` | `Ack()` | Message handled. |
| `nats.ErrSkip` | `Ack()` | Drop intentionally without DLQ. |
| `nats.ErrTerminate` | `Term()` | Permanent failure, no redelivery. |
| Any other error | `Nak()` | Redeliver according to consumer policy. |
| Handler context expired | no ack action | Let server redeliver after `AckWait`. |

`AckPolicy` defaults to `AckExplicitPolicy` and `AckWait` defaults to 30s when
they are not set on the consumer config.

The handler context timeout is calculated from `AckWait - AckBuffer`. If
`AckBuffer` is not set, hirnok uses a small default buffer. Pass the context to
database, HTTP, and other downstream calls so work stops when the message is
about to be redelivered.

## Concurrency And Lifecycle

Connection-level defaults:

```go
conn, _ := qpnats.Connect(qpnats.Config{
    URL:       "nats://localhost:4222",
    QueueSize: 128,
    Workers:   8,
    AckBuffer: 2 * time.Second,
    NATSOptions: []natsio.Option{
        natsio.MaxReconnects(-1),
    },
})
```

Override queue and worker settings per subscription or consumer:

```go
sub, err := conn.Subscribe(ctx, "events.*", handle,
    qpnats.WithQueueSize(256),
    qpnats.WithWorkers(4),
)

cons, err := js.NewConsumer(ctx, "ORDERS", cfg, handleJet,
    qpnats.WithQueueSize(512),
    qpnats.WithWorkers(16),
    qpnats.WithAckBuffer(2*time.Second),
)
```

Lifecycle methods:

| Method | Behavior |
|---|---|
| `Stop()` | Stop delivery, cancel workers, return immediately. |
| `Drain(ctx)` | Stop new delivery, finish queued and in-flight work, wait. |
| `Wait(ctx)` | Wait until worker goroutines exit. |
| `Stopped()` | Channel closed when workers exit. |
| `Conn.Close()` | Stop registered subscriptions/consumers and close NATS. |
| `Conn.Shutdown(ctx)` | Drain registered subscriptions/consumers, then drain NATS. |

`Unsubscribe()` is kept as an alias for `Sub.Stop()`.

## Middleware

Handlers are functions, so middleware is just function composition.

```go
import qpmw "github.com/rlsvr/hirnok/pkg/middleware"

h := qpmw.Recover(
    qpmw.Retry(handle, 3, 100*time.Millisecond, 5*time.Second, nil),
)

sub, err := conn.Subscribe(ctx, "events.*", h)
```

`Recover` catches panics and returns them as handler errors.

`Retry` retries handler errors with exponential backoff. The default retry
filter skips `context.Canceled`, `context.DeadlineExceeded`,
`nats.ErrSkip`, and `nats.ErrTerminate`.

`DLQ` and `DLQJet` publish failed messages to a dead-letter subject with
diagnostic headers:

```go
h := qpmw.Recover(
    qpmw.DLQJet(handleOrder, js, "orders.dlq", nil),
)

cons, err := js.NewConsumer(ctx, "ORDERS", cfg, h)
```

Default DLQ filtering sends normal errors to DLQ and skips context cancellation,
deadline expiry, and `ErrSkip`. `ErrTerminate` is DLQ-worthy by default.

For poison payloads, wrap `ErrTerminate` so the original message is not retried:

```go
func handleOrder(ctx context.Context, m jetstream.Msg) error {
    var event OrderEvent
    if err := json.Unmarshal(m.Data(), &event); err != nil {
        return fmt.Errorf("decode order event: %w", qpnats.ErrTerminate)
    }
    if err := event.Validate(); err != nil {
        return fmt.Errorf("validate order event: %w", qpnats.ErrTerminate)
    }
    return processOrder(ctx, event)
}
```

## OpenTelemetry

Tracing lives in `pkg/middleware/otel` so the base middleware package does not
pull OpenTelemetry dependencies into applications that do not use them.

```go
import (
    qpmw "github.com/rlsvr/hirnok/pkg/middleware"
    qpotel "github.com/rlsvr/hirnok/pkg/middleware/otel"
    "go.opentelemetry.io/otel"
)

tracer := otel.Tracer("orders-api")
h := qpmw.Recover(qpotel.TraceJet(handleOrder, tracer))

cons, err := js.NewConsumer(ctx, "ORDERS", cfg, h)
```

For publish-side tracing, inject into the native NATS headers:

```go
msg := &natsio.Msg{Subject: "orders.created", Data: payload, Header: natsio.Header{}}
ctx, span := tracer.Start(ctx, "nats.publish")
defer span.End()

otel.GetTextMapPropagator().Inject(ctx, qpotel.HeaderCarrier{Header: msg.Header})
_, err := js.PublishMsg(ctx, msg)
```

## Custom Subscribers

`NewConsumer` and `NewPushConsumer` are convenient built-ins. If you need a
different source, such as a custom pull batch loop, partitioned delivery, or an
in-process test source, use the lower-level pieces directly.

```go
q := queue.New[jetstream.Msg](64)

pool := worker.Run(ctx, 4, q, func(ctx context.Context, m jetstream.Msg) {
    qpnats.DispatchJet(ctx, handle, m, 25*time.Second)
})

stopSource, err := mySource.Subscribe(func(m jetstream.Msg) {
    _ = q.Push(ctx, m)
})
if err != nil {
    return err
}
defer stopSource()

// shutdown
q.Close()
_ = pool.Wait(shutdownCtx)
```

For core NATS handlers there is no exported dispatch helper because there is no
ack/nak/term behavior to centralize. A worker can call `_ = h(ctx, msg)`
directly.

## Development

```bash
make build               # go build ./...
make test                # go test -race ./...
make test-verbose        # go test -race -v ./...
make lint                # golangci-lint run ./...
make fmt                 # golangci-lint fmt
make tidy                # go mod tidy
make check               # fmt + lint + tidy
```

Unit tests start an embedded `nats-server`.

Smoke and stress programs in `test/` run against a real broker:

```bash
make nats                # start dockerized nats-server -js
make smoke               # core + JetStream round trip
make stress-core
make stress-jetstream
make stress-backpressure
make stress-burst
make stress
make nats-stop
```

## Scope

Currently out of scope:

- Router framework.
- Custom message envelope.
- Auth-specific config helpers. Use `Config.NATSOptions` today.
- KV and Object Store wrappers. Use `js.Raw()` today.
- Pull batch `Fetch` helpers. Use nats.go directly or `DispatchJet` with your
  own source loop.
