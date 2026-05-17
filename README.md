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

| Path             | What it is                                                          |
|------------------|---------------------------------------------------------------------|
| `pkg/queue`      | Bounded generic FIFO `Queue[T]` — the building block.                |
| `pkg/worker`     | Generic worker pool that drains a `Queue[T]` into a dispatch func.   |
| `pkg/nats`       | NATS connection + subscribe/publish + JetStream consumer/publisher. |
| `pkg/middleware` | Composable handler decorators: `Recover`, `Retry`, `Trace`, `TraceJet`. |

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

## Middleware

Handlers are functions, so "middleware" is just `func(Handler) Handler`. `pkg/middleware` provides composable decorators — compose by nesting or assigning, no framework needed.

```go
import qpmw "github.com/rlsvr/hirnok/pkg/middleware"

h := qpmw.Recover(
    qpmw.Retry(myHandler, 3, 100*time.Millisecond, 5*time.Second, nil),
)
conn.Subscribe(ctx, "foo", h)
```

Or imperatively when you have more than two layers:

```go
h := myHandler
h = qpmw.Recover(h)
h = qpmw.Retry(h, 3, 100*time.Millisecond, 5*time.Second, nil)
h = qpmw.Trace(h, tracer)
```

**`Recover`** catches panics in the handler and returns them as errors so a misbehaving handler doesn't take down the worker goroutine.

**`Retry`** retries on error with exponential backoff capped at `maxBackoff`. The `shouldRetry` filter decides which errors are retryable; pass `nil` for the default (retry everything except `context.Canceled` / `context.DeadlineExceeded`). Backoff sleeps respect ctx, so on JetStream the retry exits cleanly when the handler's AckWait-derived ctx expires. Note: put `Recover` *inside* `Retry` if you want panics to trigger retries — `Retry` only sees errors, not panics.

`Recover` and `Retry` are generic — they work with `qpnats.Handler`, `qpnats.JetHandler`, or any function of shape `func(context.Context, M) error`. Go infers the message type from the handler you pass in.

### Tracing (OpenTelemetry)

`Trace` / `TraceJet` wrap a handler with a `hirnok.handle` OTel span. They:

- extract the upstream W3C `traceparent`/`tracestate` from `msg.Header` so the new span continues the caller's trace;
- attach `messaging.*` semconv attributes (`system=nats`, `destination`, `operation=process`, `message.id`, `body.size`);
- record handler errors as span errors;
- make the span available to the handler via `ctx` — call `trace.SpanFromContext(ctx)` to add your own attributes.

```go
import (
    qpmw "github.com/rlsvr/hirnok/pkg/middleware"
    "go.opentelemetry.io/otel"
)

tracer := otel.Tracer("my-service")
h := qpmw.Recover(qpmw.Trace(myHandler, tracer))
conn.Subscribe(ctx, "events.*", h)
```

For publish-side tracing, inject the current ctx into outgoing headers yourself — five lines, no wrapper type needed:

```go
import "go.opentelemetry.io/otel/codes"

m := &nats.Msg{Subject: "events.user.signup", Data: payload, Header: nats.Header{}}
ctx, span := tracer.Start(ctx, "hirnok.publish")
defer span.End()
otel.GetTextMapPropagator().Inject(ctx, qpmw.HeaderCarrier{Header: m.Header})
if err := conn.PublishMsg(&qpnats.Message{Msg: m}); err != nil {
    span.RecordError(err); span.SetStatus(codes.Error, err.Error())
}
```

### Sentinel errors

Two sentinels in `pkg/nats` give handlers a shared vocabulary for "don't redeliver this":

- **`nats.ErrTerminate`** — handler failed permanently; don't retry, send to DLQ. The JetStream dispatcher acks (no more redelivery). The default `Retry` shouldRetry skips it. The default `DLQ` shouldDLQ matches it. Wrap a domain error to surface a useful reason in DLQ headers:
  ```go
  return fmt.Errorf("bad payload: %w", nats.ErrTerminate)
  ```

- **`nats.ErrSkip`** — handler decided the message is irrelevant; ack and drop silently. No retry, no DLQ. JetStream acks; DLQ short-circuits without publishing.

Sentinel-aware components see them via `errors.Is`, so callers can wrap them with their own domain errors and everything still works.

### DLQ (dead-letter queue)

`DLQ` (core) and `DLQJet` (JetStream) wrap a handler so failed messages get republished to a dead-letter subject with diagnostic headers attached:

| Header | Value |
|---|---|
| `Hirnok-DLQ-Reason` | `err.Error()` |
| `Hirnok-DLQ-Subject` | original subject |
| `Hirnok-DLQ-At` | RFC3339 timestamp |

Original headers (traceparent, custom metadata) are preserved. The wrapped handler returns nil on a successful DLQ publish so the caller acks the original message.

```go
// Plain: every error → DLQ after retries exhaust
h := qpmw.DLQJet(qpmw.RetryJet(myHandler, 3, 100*time.Millisecond, 5*time.Second, nil),
    js, "events.dlq", nil)

// Filtered: only specific errors go to DLQ
isSchemaError := func(err error) bool { return errors.Is(err, ErrSchema) }
h := qpmw.DLQJet(myHandler, js, "schema.dlq", isSchemaError)

// Terminate-bypass-retry: handler signals "don't retry, just DLQ"
//   in handler:   return fmt.Errorf("bad: %w", nats.ErrTerminate)
//   in wiring:
h := qpmw.RetryJet(qpmw.DLQJet(myHandler, js, "events.dlq", nil), 3, ...)
//   Retry's default shouldRetry skips ErrTerminate; DLQJet publishes
//   and returns nil; Retry sees nil and doesn't loop.
```

Default `shouldDLQ` routes everything to DLQ *except* `ErrSkip`, `context.Canceled`, and `context.DeadlineExceeded` — those indicate "try again later," not "the data is poison."

If the DLQ publish itself fails, the wrapper returns a joined error (original + publish error) so the outer redelivery path can retry. For JetStream DLQs, make sure the DLQ subject is covered by a stream of its own so DLQ messages are durable.

### Composition: DLQ inside vs outside Retry

- **Outside** (`DLQ(Retry(h))`): only the *final* post-retry failure is DLQ'd. Use when retries are worth trying first.
- **Inside** (`Retry(DLQ(h))`): each failed attempt is DLQ'd — but DLQ swallows the error so Retry sees nil and doesn't loop. Use for fast-fail patterns where retries are pointless (e.g., handler returns `ErrTerminate`).

### Wiring multiple handlers

There's no `Router` in hirnok on purpose. If you want one central place to register handlers, apply a default middleware chain, and shut everything down together, that's about 25 lines of caller code:

```go
type Setup struct {
    Conn   *qpnats.Conn
    Tracer trace.Tracer
    subs   []*qpnats.Sub
}

func (s *Setup) wrap(h qpnats.Handler) qpnats.Handler {
    h = qpmw.Trace(h, s.Tracer)
    h = qpmw.Retry(h, 3, 100*time.Millisecond, 5*time.Second, nil)
    h = qpmw.Recover(h)
    return h
}

func (s *Setup) Sub(ctx context.Context, subject string, h qpnats.Handler) error {
    sub, err := s.Conn.Subscribe(ctx, subject, s.wrap(h))
    if err != nil {
        return err
    }
    s.subs = append(s.subs, sub)
    return nil
}

func (s *Setup) Stop(ctx context.Context) error {
    for _, sub := range s.subs {
        _ = sub.Unsubscribe()
    }
    for _, sub := range s.subs {
        if err := sub.Wait(ctx); err != nil {
            return err
        }
    }
    return nil
}
```

Same idea works for JetStream — add a `JS *qpnats.JetStream` field, a parallel `Consume` method, and track `[]*qpnats.Consumer`. The point is: this is *your* glue, shaped to *your* app. We don't impose a Router on you.

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
