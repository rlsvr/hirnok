package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/rlsvr/hirnok/pkg/queue"
	"github.com/rlsvr/hirnok/pkg/worker"
)

const defaultAckWait = 30 * time.Second

// JetStream wraps a jetstream.JetStream context plus the parent Conn's
// config (so Consume inherits QueueSize/Workers/AckBuffer).
type JetStream struct {
	js  jetstream.JetStream
	cfg Config
}

// JetStream returns a JetStream view of this connection. Cheap to call;
// nats.go memoizes the underlying jetstream.JetStream per *natsio.Conn.
func (c *Conn) JetStream() (*JetStream, error) {
	js, err := jetstream.New(c.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &JetStream{js: js, cfg: c.cfg}, nil
}

// Raw returns the underlying jetstream.JetStream for advanced use
// (stream / KV / object store management not yet exposed here).
func (j *JetStream) Raw() jetstream.JetStream { return j.js }

// JetMessage embeds the jetstream.Msg interface so Ack/Nak/Term/InProgress
// and Data()/Subject()/Headers()/Metadata() are all reachable directly.
type JetMessage struct {
	jetstream.Msg
}

// JetHandler is the JetStream handler signature. The wrapper auto-acks on
// nil and naks on non-nil error. If ctx is already Done when the handler
// returns, the wrapper does nothing — server-side AckWait has elapsed and
// the message is being (or has been) redelivered.
//
// Handlers MUST respect ctx: thread it through DB calls, HTTP requests,
// and pipelines so work stops cleanly when the ack window closes.
type JetHandler func(ctx context.Context, m *JetMessage) error

// Consumer is a running JS consumer with a worker pool.
type Consumer struct {
	cc     jetstream.ConsumeContext
	q      *queue.Queue[*JetMessage]
	cancel context.CancelFunc
	pool   *worker.Pool
}

// Consume creates-or-updates a durable consumer on stream and starts a
// worker pool. cfg is passed through to jetstream.ConsumerConfig — Durable,
// FilterSubject, AckPolicy, DeliverPolicy, AckWait, etc. all live there.
//
// AckPolicy is forced to AckExplicitPolicy if unset (the wrapper relies on
// it). AckWait defaults to 30s when zero.
func (j *JetStream) Consume(
	ctx context.Context,
	stream string,
	cfg jetstream.ConsumerConfig,
	h JetHandler,
) (*Consumer, error) {
	if h == nil {
		return nil, errors.New("nats: handler is required")
	}
	if cfg.AckPolicy == jetstream.AckPolicy(0) {
		cfg.AckPolicy = jetstream.AckExplicitPolicy
	}
	if cfg.AckWait == 0 {
		cfg.AckWait = defaultAckWait
	}

	s, err := j.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("get stream %q: %w", stream, err)
	}
	cons, err := s.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("upsert consumer: %w", err)
	}

	consCtx, cancel := context.WithCancel(ctx)
	q := queue.New[*JetMessage](j.cfg.queueSize())
	handlerTimeout := perMessageTimeout(cfg.AckWait, j.cfg.AckBuffer)

	cc, err := cons.Consume(func(raw jetstream.Msg) {
		_ = q.Push(consCtx, &JetMessage{Msg: raw})
	})
	if err != nil {
		cancel()
		q.Close()
		return nil, fmt.Errorf("start consume: %w", err)
	}

	pool := worker.Run(consCtx, j.cfg.workers(), q, func(parent context.Context, m *JetMessage) {
		dispatchJet(parent, h, m, handlerTimeout)
	})

	return &Consumer{
		cc:     cc,
		q:      q,
		cancel: cancel,
		pool:   pool,
	}, nil
}

func dispatchJet(parent context.Context, h JetHandler, m *JetMessage, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	err := h(ctx, m)
	switch {
	case ctx.Err() != nil:
		// AckWait elapsed mid-handler (or parent canceled). Server has
		// redelivered or will. Don't Ack (it'd be a no-op); don't Nak
		// (would shorten the redelivery cycle for no reason).
	case err == nil:
		_ = m.Ack()
	case errors.Is(err, ErrSkip), errors.Is(err, ErrTerminate):
		// Explicit "don't redeliver" signal from the handler — ack so
		// the server stops trying. Pair ErrTerminate with DLQ middleware
		// to capture the payload before it's lost.
		_ = m.Ack()
	default:
		_ = m.Nak()
	}
}

// Stop halts delivery and signals workers to exit. Returns once the JS
// consume context is stopped. In-flight handlers may still be running
// until they observe ctx cancellation — call Wait to block until they
// have actually exited.
func (c *Consumer) Stop() {
	c.cc.Stop()
	c.cancel()
	c.q.Close()
}

// Wait blocks until all worker goroutines have exited or ctx is canceled.
// Pair with Stop (or ctx cancellation) for graceful shutdown.
func (c *Consumer) Wait(ctx context.Context) error {
	return c.pool.Wait(ctx)
}

// Stopped returns a channel that closes when all workers have exited.
func (c *Consumer) Stopped() <-chan struct{} { return c.pool.Stopped() }

// Publish synchronously publishes data to a JS subject and returns the
// server ack (sequence, duplicate flag).
func (j *JetStream) Publish(ctx context.Context, subject string, data []byte) (*jetstream.PubAck, error) {
	ack, err := j.js.Publish(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("js publish %q: %w", subject, err)
	}
	return ack, nil
}

// PublishMsg publishes a fully-formed *natsio.Msg (use this to set
// Nats-Msg-Id for dedupe, or Nats-Expected-Stream for guards).
func (j *JetStream) PublishMsg(ctx context.Context, m *natsio.Msg) (*jetstream.PubAck, error) {
	if m == nil {
		return nil, errors.New("nats: PublishMsg requires a non-nil msg")
	}
	ack, err := j.js.PublishMsg(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("js publish msg %q: %w", m.Subject, err)
	}
	return ack, nil
}

// PublishAsync queues a publish and returns an ack future. Caller awaits
// via <-future.Ok() / <-future.Err().
func (j *JetStream) PublishAsync(subject string, data []byte) (jetstream.PubAckFuture, error) {
	f, err := j.js.PublishAsync(subject, data)
	if err != nil {
		return nil, fmt.Errorf("js publish async %q: %w", subject, err)
	}
	return f, nil
}

// perMessageTimeout derives the handler ctx deadline from AckWait minus
// AckBuffer. The buffer defaults to ackWait/10 (cancel ~10% before server
// redelivery) but never less than 1ms and never larger than ackWait/2.
func perMessageTimeout(ackWait, buffer time.Duration) time.Duration {
	if ackWait <= 0 {
		ackWait = defaultAckWait
	}
	if buffer <= 0 {
		buffer = ackWait / 10
	}
	if buffer > ackWait/2 {
		buffer = ackWait / 2
	}
	return max(ackWait-buffer, time.Millisecond)
}
