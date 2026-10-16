package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/rlsvr/inas/pkg/queue"
	"github.com/rlsvr/inas/pkg/worker"
)

const defaultAckWait = 30 * time.Second

// JetStream wraps a jetstream.JetStream context plus the parent Conn's
// config (so NewConsumer/NewPushConsumer inherit QueueSize/Workers/AckBuffer).
type JetStream struct {
	conn *Conn
	js   jetstream.JetStream
	cfg  Config
}

// JetStream returns a JetStream view of this connection, inheriting QueueSize,
// Workers, AckBuffer, and ErrorHandler defaults from the parent Conn.
func (c *Conn) JetStream() (*JetStream, error) {
	js, err := jetstream.New(c.nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &JetStream{conn: c, js: js, cfg: c.cfg}, nil
}

// Conn returns the underlying *Conn that owns this JetStream context.
func (j *JetStream) Conn() *Conn { return j.conn }

// Raw returns the underlying jetstream.JetStream for advanced use
// (stream / KV / object store management not yet exposed here).
func (j *JetStream) Raw() jetstream.JetStream { return j.js }

// DefaultConsumerConfig returns a consumer configuration initialized with sensible
// production defaults: explicit ack policy, deliver all policy, and 30-second ack wait.
// Callers can adjust or override fields (such as FilterSubject, MaxDeliver, etc.) as needed.
func DefaultConsumerConfig(durable string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       defaultAckWait,
		MaxDeliver:    20,
	}
}

// JetHandler is the JetStream handler signature. The wrapper auto-acks on
// nil and naks on non-nil error. If ctx is already Done when the handler
// returns, the wrapper does nothing - server-side AckWait has elapsed and
// the message is being (or has been) redelivered.
//
// Handlers MUST respect ctx: thread it through DB calls, HTTP requests,
// and pipelines so work stops cleanly when the ack window closes.
type JetHandler func(ctx context.Context, m jetstream.Msg) error

type queuedJetMsg struct {
	msg      jetstream.Msg
	deadline time.Time
}

// Consumer is a running JS consumer with a worker pool.
type Consumer struct {
	conn         *Conn
	cc           jetstream.ConsumeContext
	q            *queue.Queue[queuedJetMsg]
	cancel       context.CancelFunc
	stopAfter    func() bool
	pool         *worker.Pool
	mu           sync.Mutex
	closing      bool
	closeDone    chan struct{}
	err          error
	errorHandler func(error)
}

type startJetConsumer func(func(jetstream.Msg)) (jetstream.ConsumeContext, error)

// NewConsumer creates-or-updates a pull consumer on stream and starts a
// worker pool. cfg is passed through to jetstream.ConsumerConfig: Durable,
// FilterSubject, AckPolicy, DeliverPolicy, AckWait, etc. all live there.
//
// AckPolicy defaults to AckExplicitPolicy when unset; non-explicit policies
// (AckAllPolicy, AckNonePolicy) are rejected because per-message worker acks
// depend on explicit acknowledgments. AckWait defaults to 30s when zero.
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
	run := j.cfg.runOptions(opts)
	var err error
	cfg, err = normalizeConsumerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.MaxAckPending == 0 {
		cfg.MaxAckPending = run.queueSize + run.workers
	}

	s, err := j.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("get stream %q: %w", stream, err)
	}
	cons, err := s.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("upsert consumer: %w", err)
	}

	return j.startConsumer(ctx, cfg, h, func(cb func(jetstream.Msg)) (jetstream.ConsumeContext, error) {
		return cons.Consume(cb, jetstream.PullMaxMessages(run.queueSize+run.workers))
	}, opts...)
}

// NewPushConsumer creates-or-updates a push consumer on stream and starts a
// worker pool. cfg is passed through to jetstream.ConsumerConfig. Set
// DeliverSubject and DeliverGroup when you need stable queue-style push
// delivery; when DeliverSubject is empty, hirnok uses a generated inbox.
//
// AckPolicy defaults to AckExplicitPolicy when unset; non-explicit policies
// are rejected. AckWait defaults to 30s when zero.
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
	run := j.cfg.runOptions(opts)
	var err error
	cfg, err = normalizeConsumerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.MaxAckPending == 0 {
		cfg.MaxAckPending = run.queueSize + run.workers
	}
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

func normalizeConsumerConfig(cfg jetstream.ConsumerConfig) (jetstream.ConsumerConfig, error) {
	if cfg.AckPolicy != jetstream.AckPolicy(0) && cfg.AckPolicy != jetstream.AckExplicitPolicy {
		return cfg, fmt.Errorf("nats: unsupported AckPolicy %v (AckExplicitPolicy required)", cfg.AckPolicy)
	}
	cfg.AckPolicy = jetstream.AckExplicitPolicy
	if cfg.AckWait == 0 {
		cfg.AckWait = defaultAckWait
	}
	return cfg, nil
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
	q := queue.New[queuedJetMsg](run.queueSize)
	handlerTimeout := perMessageTimeout(cfg.AckWait, run.ackBuffer)

	cc, err := start(func(raw jetstream.Msg) {
		dl := time.Now().Add(handlerTimeout)
		if pushErr := q.Push(consCtx, queuedJetMsg{
			msg:      raw,
			deadline: dl,
		}); pushErr != nil {
			if nakErr := raw.Nak(); nakErr != nil && run.errorHandler != nil {
				run.errorHandler(nakErr)
			}
		}
	})
	if err != nil {
		cancel()
		q.Close()
		return nil, fmt.Errorf("start consume: %w", err)
	}

	pool := worker.Run(consCtx, run.workers, q, func(parent context.Context, item queuedJetMsg) {
		if parent.Err() != nil || time.Now().After(item.deadline) {
			if nakErr := item.msg.Nak(); nakErr != nil && run.errorHandler != nil {
				run.errorHandler(nakErr)
			}
			return
		}
		if inProgErr := item.msg.InProgress(); inProgErr != nil && run.errorHandler != nil {
			run.errorHandler(inProgErr)
		}
		DispatchJetWithDeadline(parent, h, item.msg, time.Now().Add(handlerTimeout), run.errorHandler)
	})

	consumer := &Consumer{
		conn:         j.conn,
		cc:           cc,
		q:            q,
		cancel:       cancel,
		pool:         pool,
		errorHandler: run.errorHandler,
	}
	if j.conn != nil {
		j.conn.registerConsumer(consumer)
	}
	consumer.mu.Lock()
	consumer.stopAfter = context.AfterFunc(ctx, func() {
		consumer.Stop()
	})
	consumer.mu.Unlock()
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
//   - handler returns NakDelayError -> NakWithDelay (server will redeliver after delay)
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
	DispatchJetWithDeadline(parent, h, m, time.Now().Add(timeout))
}

// DispatchJetWithDeadline runs h on m with a context having the specified deadline,
// syncing the handler context directly with the server AckWait countdown.
// If an errorHandler is provided, any error returned by Ack, Nak, or Term is passed to it.
func DispatchJetWithDeadline(parent context.Context, h JetHandler, m jetstream.Msg, deadline time.Time, errorHandler ...func(error)) {
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	err := h(ctx, m)
	var nd *NakDelayError
	var ackErr error
	switch {
	case ctx.Err() != nil:
		// AckWait elapsed mid-handler (or parent canceled). Server has
		// redelivered or will. Don't Ack (it'd be a no-op); don't Nak
		// (would shorten the redelivery cycle for no reason).
	case err == nil || errors.Is(err, ErrSkip):
		ackErr = m.Ack()
	case errors.Is(err, ErrTerminate):
		// Explicit permanent failure signal from the handler. Pair with DLQ
		// middleware to capture the payload before it is terminated.
		ackErr = m.Term()
	case errors.As(err, &nd):
		if nd.Delay > 0 {
			ackErr = m.NakWithDelay(nd.Delay)
		} else {
			ackErr = m.Nak()
		}
	default:
		ackErr = m.Nak()
	}
	if ackErr != nil && len(errorHandler) > 0 && errorHandler[0] != nil {
		errorHandler[0](ackErr)
	}
}

// Stop halts delivery and signals workers to exit. In-flight handlers may
// still be running until they observe ctx cancellation - call Wait to block
// until they have actually exited.
func (c *Consumer) Stop() {
	_ = c.closeWithDrain(context.Background(), false)
}

// Drain stops delivery, lets queued/in-flight handlers finish, and waits for
// worker shutdown.
func (c *Consumer) Drain(ctx context.Context) error {
	return c.closeWithDrain(ctx, true)
}

func (c *Consumer) closeWithDrain(ctx context.Context, drain bool) error {
	c.mu.Lock()
	if c.closing {
		done := c.closeDone
		c.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		if drain {
			return errors.Join(c.err, c.Wait(ctx))
		}
		return c.err
	}
	c.closing = true
	c.closeDone = make(chan struct{})
	if c.stopAfter != nil {
		c.stopAfter()
	}
	c.mu.Unlock()

	defer func() {
		close(c.closeDone)
	}()

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
	c.drainRemaining()
	if c.conn != nil {
		c.conn.unregisterConsumer(c)
	}
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	return err
}

func (c *Consumer) drainRemaining() {
	for {
		select {
		case item := <-c.q.Recv():
			if err := item.msg.Nak(); err != nil && c.errorHandler != nil {
				c.errorHandler(err)
			}
		default:
			return
		}
	}
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
