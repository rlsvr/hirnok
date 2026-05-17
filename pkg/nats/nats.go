// Package nats is a thin, opinionated wrapper around github.com/nats-io/nats.go
// that gives callers two things: connect, and register a handler. A bounded
// hirnok/pkg/queue.Queue sits between the NATS receive callback and the
// handler so each subscription has a small, decoupled worker pool with
// backpressure.
//
// Two delivery models are supported:
//
//   - Core NATS (Subscribe / QueueSubscribe / Publish): fire-and-forget,
//     no acks, no retry. Handler errors are not retried; the message is
//     simply dropped. Handler ctx is the parent subscription ctx and only
//     cancels on Unsubscribe, Conn.Close, or parent ctx cancellation.
//
//   - JetStream (Conn.JetStream / Consume / Publish): durable consumer with
//     explicit ack. Handler ctx is derived from AckWait and cancels just
//     before server-side redelivery would kick in. Handlers MUST respect
//     ctx so downstream work stops when the ack window closes.
package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/rlsvr/hirnok/pkg/queue"
	"github.com/rlsvr/hirnok/pkg/worker"
)

const (
	defaultQueueSize = 64
	defaultWorkers   = 1
)

// Config holds the connect-time knobs. Anything not set has a sane default.
type Config struct {
	URL         string          // required, e.g. "nats://localhost:4222"
	Name        string          // optional client name
	QueueSize   int             // per-subscription buffer; default 64
	Workers     int             // per-subscription worker count; default 1
	AckBuffer   time.Duration   // JS only: handler ctx expires this much before AckWait; default = AckWait/10
	NATSOptions []natsio.Option // appended after hirnok defaults
}

func (c Config) queueSize() int {
	if c.QueueSize > 0 {
		return c.QueueSize
	}
	return defaultQueueSize
}

func (c Config) workers() int {
	if c.Workers > 0 {
		return c.Workers
	}
	return defaultWorkers
}

type runOptions struct {
	queueSize int
	workers   int
	ackBuffer time.Duration
}

// Option overrides per-subscription/per-consumer execution settings.
type Option func(*runOptions)

// WithQueueSize overrides the per-subscription queue capacity. Non-positive
// values are ignored.
func WithQueueSize(n int) Option {
	return func(o *runOptions) {
		if n > 0 {
			o.queueSize = n
		}
	}
}

// WithWorkers overrides the per-subscription worker count. Non-positive values
// are ignored.
func WithWorkers(n int) Option {
	return func(o *runOptions) {
		if n > 0 {
			o.workers = n
		}
	}
}

// WithAckBuffer overrides the JetStream handler context buffer before AckWait.
func WithAckBuffer(d time.Duration) Option {
	return func(o *runOptions) {
		o.ackBuffer = d
	}
}

func (c Config) runOptions(opts []Option) runOptions {
	o := runOptions{
		queueSize: c.queueSize(),
		workers:   c.workers(),
		ackBuffer: c.AckBuffer,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// Conn wraps a *natsio.Conn and tracks every Sub/Consumer it owns so Close and
// Shutdown can stop them all.
type Conn struct {
	nc        *natsio.Conn
	cfg       Config
	mu        sync.Mutex
	subs      map[*Sub]struct{}
	consumers map[*Consumer]struct{}
}

// Connect dials NATS and returns a ready Conn. Reconnect is enabled with
// 1s backoff and unlimited attempts
func Connect(cfg Config) (*Conn, error) {
	if cfg.URL == "" {
		return nil, errors.New("nats: Config.URL is required")
	}
	opts := []natsio.Option{
		natsio.RetryOnFailedConnect(true),
		natsio.ReconnectWait(time.Second),
		natsio.MaxReconnects(-1),
	}
	if cfg.Name != "" {
		opts = append(opts, natsio.Name(cfg.Name))
	}
	opts = append(opts, cfg.NATSOptions...)
	nc, err := natsio.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &Conn{
		nc:        nc,
		cfg:       cfg,
		subs:      make(map[*Sub]struct{}),
		consumers: make(map[*Consumer]struct{}),
	}, nil
}

// Raw returns the underlying nats.go connection for advanced use.
func (c *Conn) Raw() *natsio.Conn { return c.nc }

// Close immediately stops registered subscriptions/consumers and closes the
// underlying NATS connection. It does not wait for in-flight handlers.
func (c *Conn) Close() {
	for _, sub := range c.subscriptions() {
		_ = sub.Stop()
	}
	for _, cons := range c.consumerList() {
		cons.Stop()
	}
	if c.nc != nil {
		c.nc.Close()
	}
}

// Shutdown drains registered subscriptions/consumers, waits for their workers,
// then drains and closes the underlying NATS connection.
func (c *Conn) Shutdown(ctx context.Context) error {
	var errs []error
	for _, sub := range c.subscriptions() {
		if err := sub.Drain(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	for _, cons := range c.consumerList() {
		if err := cons.Drain(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if c.nc == nil || c.nc.IsClosed() {
		return errors.Join(errs...)
	}
	if err := c.nc.Drain(); err != nil && !errors.Is(err, natsio.ErrConnectionClosed) {
		errs = append(errs, err)
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for !c.nc.IsClosed() {
		select {
		case <-ctx.Done():
			c.nc.Close()
			errs = append(errs, ctx.Err())
			return errors.Join(errs...)
		case <-t.C:
		}
	}
	return errors.Join(errs...)
}

func (c *Conn) registerSub(s *Sub) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[s] = struct{}{}
}

func (c *Conn) unregisterSub(s *Sub) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subs, s)
}

func (c *Conn) subscriptions() []*Sub {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Sub, 0, len(c.subs))
	for sub := range c.subs {
		out = append(out, sub)
	}
	return out
}

func (c *Conn) registerConsumer(consumer *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers[consumer] = struct{}{}
}

func (c *Conn) unregisterConsumer(consumer *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.consumers, consumer)
}

func (c *Conn) consumerList() []*Consumer {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Consumer, 0, len(c.consumers))
	for consumer := range c.consumers {
		out = append(out, consumer)
	}
	return out
}

// Handler is the core-NATS handler signature. Return nil to drop, return
// an error to indicate failure — the wrapper does nothing with the error
// for core NATS (no ack/retry exists), but the signature is symmetric with
// JetHandler.
type Handler func(ctx context.Context, m *natsio.Msg) error

// Sub is a running subscription with a worker pool.
type Sub struct {
	conn   *Conn
	sub    *natsio.Subscription
	q      *queue.Queue[*natsio.Msg]
	cancel context.CancelFunc
	pool   *worker.Pool
	mu     sync.Mutex
	closed bool
	err    error
}

// Subscribe registers handler on subject and starts a worker pool. ctx
// is the parent for every handler invocation; cancelling it stops the pool.
func (c *Conn) Subscribe(ctx context.Context, subject string, h Handler, opts ...Option) (*Sub, error) {
	return c.startSub(ctx, subject, "", h, opts...)
}

// QueueSubscribe joins a NATS queue group so messages are load-balanced
// across all subscribers sharing the same `group`.
func (c *Conn) QueueSubscribe(ctx context.Context, subject, group string, h Handler, opts ...Option) (*Sub, error) {
	if group == "" {
		return nil, errors.New("nats: QueueSubscribe requires a non-empty group")
	}
	return c.startSub(ctx, subject, group, h, opts...)
}

func (c *Conn) startSub(ctx context.Context, subject, group string, h Handler, opts ...Option) (*Sub, error) {
	if h == nil {
		return nil, errors.New("nats: handler is required")
	}
	run := c.cfg.runOptions(opts)
	subCtx, cancel := context.WithCancel(ctx)
	q := queue.New[*natsio.Msg](run.queueSize)

	cb := func(raw *natsio.Msg) {
		_ = q.Push(subCtx, raw)
	}

	var (
		ns  *natsio.Subscription
		err error
	)
	if group == "" {
		ns, err = c.nc.Subscribe(subject, cb)
	} else {
		ns, err = c.nc.QueueSubscribe(subject, group, cb)
	}
	if err != nil {
		cancel()
		q.Close()
		return nil, fmt.Errorf("nats subscribe %q: %w", subject, err)
	}

	pool := worker.Run(subCtx, run.workers, q, func(ctx context.Context, m *natsio.Msg) {
		_ = h(ctx, m)
	})

	sub := &Sub{
		conn:   c,
		sub:    ns,
		q:      q,
		cancel: cancel,
		pool:   pool,
	}
	c.registerSub(sub)
	return sub, nil
}

// Stop stops the subscription and signals workers to exit. Returns
// once the NATS subscription is gone. In-flight handlers may still be
// running until they observe ctx cancellation — call Wait to block until
// they have actually exited.
func (s *Sub) Stop() error {
	return s.stop(false, context.Background())
}

// Unsubscribe is an alias for Stop.
func (s *Sub) Unsubscribe() error {
	return s.Stop()
}

// Drain stops accepting new messages, closes the queue to new pushes, and
// waits for already queued/in-flight handler calls to finish.
func (s *Sub) Drain(ctx context.Context) error {
	return s.stop(true, ctx)
}

func (s *Sub) stop(drain bool, ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		err := s.err
		s.mu.Unlock()
		if drain {
			return errors.Join(err, s.Wait(ctx))
		}
		return err
	}
	s.closed = true
	s.mu.Unlock()

	err := s.sub.Unsubscribe()
	s.q.Close()
	if drain {
		if waitErr := s.pool.Wait(ctx); waitErr != nil {
			err = errors.Join(err, waitErr)
			s.cancel()
		} else {
			s.cancel()
		}
	} else {
		s.cancel()
	}
	if s.conn != nil {
		s.conn.unregisterSub(s)
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	return err
}

// Wait blocks until all worker goroutines have exited or ctx is canceled.
// Pair with Unsubscribe (or ctx cancellation) for graceful shutdown.
func (s *Sub) Wait(ctx context.Context) error {
	return s.pool.Wait(ctx)
}

// Stopped returns a channel that closes when all workers have exited.
func (s *Sub) Stopped() <-chan struct{} { return s.pool.Stopped() }

// Publish sends data on subject (passthrough to nats.Conn.Publish).
func (c *Conn) Publish(subject string, data []byte) error {
	return c.nc.Publish(subject, data)
}

// PublishMsg sends a fully-formed *nats.Msg (lets caller set Header, Reply).
func (c *Conn) PublishMsg(m *natsio.Msg) error {
	if m == nil {
		return errors.New("nats: PublishMsg requires a non-nil msg")
	}
	return c.nc.PublishMsg(m)
}

// Request performs a synchronous request/reply round-trip. ctx bounds how
// long we wait for the reply.
func (c *Conn) Request(ctx context.Context, subject string, data []byte) (*natsio.Msg, error) {
	raw, err := c.nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("nats request %q: %w", subject, err)
	}
	return raw, nil
}
