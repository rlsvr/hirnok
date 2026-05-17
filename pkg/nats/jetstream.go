package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/rlsvr/hirnok/pkg/queue"
	"github.com/rlsvr/hirnok/pkg/worker"
)

const defaultAckWait = 30 * time.Second

// JetStream wraps a jetstream.JetStream context plus the parent Conn's
// config (so NewConsumer/NewPushConsumer inherit QueueSize/Workers/AckBuffer).
type JetStream struct {
	conn *Conn
	js   jetstream.JetStream
	cfg  Config
}

// JetStream returns a JetStream view of this connection. Cheap to call;
// nats.go memoizes the underlying jetstream.JetStream per *natsio.Conn.
func (c *Conn) JetStream() (*JetStream, error) {
	js, err := jetstream.New(c.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &JetStream{conn: c, js: js, cfg: c.cfg}, nil
}

// Raw returns the underlying jetstream.JetStream for advanced use
// (stream / KV / object store management not yet exposed here).
func (j *JetStream) Raw() jetstream.JetStream { return j.js }

// JetHandler is the JetStream handler signature. The wrapper auto-acks on
// nil and naks on non-nil error. If ctx is already Done when the handler
// returns, the wrapper does nothing - server-side AckWait has elapsed and
// the message is being (or has been) redelivered.
//
// Handlers MUST respect ctx: thread it through DB calls, HTTP requests,
// and pipelines so work stops cleanly when the ack window closes.
type JetHandler func(ctx context.Context, m jetstream.Msg) error

// Consumer is a running JS consumer with a worker pool.
type Consumer struct {
	conn   *Conn
	cc     jetstream.ConsumeContext
	q      *queue.Queue[jetstream.Msg]
	cancel context.CancelFunc
	pool   *worker.Pool
	mu     sync.Mutex
	closed bool
	err    error
}

type startJetConsumer func(func(jetstream.Msg)) (jetstream.ConsumeContext, error)

// NewConsumer creates-or-updates a pull consumer on stream and starts a
// worker pool. cfg is passed through to jetstream.ConsumerConfig: Durable,
// FilterSubject, AckPolicy, DeliverPolicy, AckWait, etc. all live there.
//
// AckPolicy is forced to AckExplicitPolicy if unset (the wrapper relies on
// it). AckWait defaults to 30s when zero.
func (j *JetStream) NewConsumer(
	ctx context.Context,
	stream string,
	cfg jetstream.ConsumerConfig,
	h JetHandler,
	opts ...Option,
) (*Consumer, error) {
	if h == nil {
		return nil, errors.New("nats: handler is required")
	}
	cfg = normalizeConsumerConfig(cfg)

	s, err := j.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("get stream %q: %w", stream, err)
	}
	cons, err := s.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("upsert consumer: %w", err)
	}

	return j.startConsumer(ctx, cfg, h, func(cb func(jetstream.Msg)) (jetstream.ConsumeContext, error) {
		return cons.Consume(cb)
	}, opts...)
}

// NewPushConsumer creates-or-updates a push consumer on stream and starts a
// worker pool. cfg is passed through to jetstream.ConsumerConfig. Set
// DeliverSubject and DeliverGroup when you need stable queue-style push
// delivery; when DeliverSubject is empty, hirnok uses a generated inbox.
//
// AckPolicy is forced to AckExplicitPolicy if unset (the wrapper relies on
// it). AckWait defaults to 30s when zero.
func (j *JetStream) NewPushConsumer(
	ctx context.Context,
	stream string,
	cfg jetstream.ConsumerConfig,
	h JetHandler,
	opts ...Option,
) (*Consumer, error) {
	if h == nil {
		return nil, errors.New("nats: handler is required")
	}
	cfg = normalizeConsumerConfig(cfg)
	if cfg.DeliverSubject == "" {
		cfg.DeliverSubject = natsio.NewInbox()
	}

	s, err := j.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("get stream %q: %w", stream, err)
	}
	cons, err := s.CreateOrUpdatePushConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("upsert push consumer: %w", err)
	}

	return j.startConsumer(ctx, cfg, h, func(cb func(jetstream.Msg)) (jetstream.ConsumeContext, error) {
		return cons.Consume(cb)
	}, opts...)
}

// Consume is a deprecated alias for NewConsumer.
//
// Deprecated: use NewConsumer for pull consumers or NewPushConsumer for push
// consumers.
func (j *JetStream) Consume(
	ctx context.Context,
	stream string,
	cfg jetstream.ConsumerConfig,
	h JetHandler,
	opts ...Option,
) (*Consumer, error) {
	return j.NewConsumer(ctx, stream, cfg, h, opts...)
}

func normalizeConsumerConfig(cfg jetstream.ConsumerConfig) jetstream.ConsumerConfig {
	if cfg.AckPolicy == jetstream.AckPolicy(0) {
		cfg.AckPolicy = jetstream.AckExplicitPolicy
	}
	if cfg.AckWait == 0 {
		cfg.AckWait = defaultAckWait
	}
	return cfg
}

func (j *JetStream) startConsumer(
	ctx context.Context,
	cfg jetstream.ConsumerConfig,
	h JetHandler,
	start startJetConsumer,
	opts ...Option,
) (*Consumer, error) {
	if h == nil {
		return nil, errors.New("nats: handler is required")
	}
	run := j.cfg.runOptions(opts)
	consCtx, cancel := context.WithCancel(ctx)
	q := queue.New[jetstream.Msg](run.queueSize)
	handlerTimeout := perMessageTimeout(cfg.AckWait, run.ackBuffer)

	cc, err := start(func(raw jetstream.Msg) {
		_ = q.Push(consCtx, raw)
	})
	if err != nil {
		cancel()
		q.Close()
		return nil, fmt.Errorf("start consume: %w", err)
	}

	pool := worker.Run(consCtx, run.workers, q, func(parent context.Context, m jetstream.Msg) {
		DispatchJet(parent, h, m, handlerTimeout)
	})

	consumer := &Consumer{
		conn:   j.conn,
		cc:     cc,
		q:      q,
		cancel: cancel,
		pool:   pool,
	}
	if j.conn != nil {
		j.conn.registerConsumer(consumer)
	}
	return consumer, nil
}

// DispatchJet runs h on m with a per-message context derived from parent
// (timeout = AckWait - AckBuffer when configured via JetStream.NewConsumer or
// JetStream.NewPushConsumer), then translates the handler's return into a
// JetStream ack action:
//
//   - handler returns nil -> Ack
//   - handler returns ErrSkip -> Ack (explicit silent drop)
//   - handler returns ErrTerminate -> Term (explicit permanent failure)
//   - handler returns any other error -> Nak (server will redeliver)
//   - per-message ctx expired before handler returns -> no action (server
//     has already redelivered or will after AckWait)
//
// JetStream.NewConsumer and JetStream.NewPushConsumer use this internally.
// It's exported so callers wiring a custom subscriber - a partitioned
// consumer, a pull-consumer batch loop, an in-process test source - can use it
// from their own worker pool and get the same ack semantics. See the "Custom
// subscribers" section in the README for the wiring recipe.
func DispatchJet(parent context.Context, h JetHandler, m jetstream.Msg, timeout time.Duration) {
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
	case errors.Is(err, ErrSkip):
		_ = m.Ack()
	case errors.Is(err, ErrTerminate):
		// Explicit permanent failure signal from the handler. Pair with DLQ
		// middleware to capture the payload before it is terminated.
		_ = m.Term()
	default:
		_ = m.Nak()
	}
}

// Stop halts delivery and signals workers to exit. In-flight handlers may
// still be running until they observe ctx cancellation - call Wait to block
// until they have actually exited.
func (c *Consumer) Stop() {
	_ = c.stop(false, context.Background())
}

// Drain stops delivery, lets queued/in-flight handlers finish, and waits for
// worker shutdown.
func (c *Consumer) Drain(ctx context.Context) error {
	return c.stop(true, ctx)
}

func (c *Consumer) stop(drain bool, ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		err := c.err
		c.mu.Unlock()
		if drain {
			return errors.Join(err, c.Wait(ctx))
		}
		return err
	}
	c.closed = true
	c.mu.Unlock()

	var err error
	if drain {
		c.cc.Drain()
		select {
		case <-c.cc.Closed():
		case <-ctx.Done():
			err = ctx.Err()
			c.cc.Stop()
			c.cancel()
		}
		c.q.Close()
		if waitErr := c.pool.Wait(ctx); waitErr != nil {
			err = errors.Join(err, waitErr)
			c.cancel()
		} else {
			c.cancel()
		}
	} else {
		c.cc.Stop()
		c.cancel()
		c.q.Close()
	}
	if c.conn != nil {
		c.conn.unregisterConsumer(c)
	}
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	return err
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
