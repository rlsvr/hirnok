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
	URL       string        // required, e.g. "nats://localhost:4222"
	Name      string        // optional client name
	QueueSize int           // per-subscription buffer; default 64
	Workers   int           // per-subscription worker count; default 1
	AckBuffer time.Duration // JS only: handler ctx expires this much before AckWait; default = AckWait/10
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

// Conn wraps a *natsio.Conn and tracks every Sub it owns so Close can stop
// them all.
type Conn struct {
	nc  *natsio.Conn
	cfg Config
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
	nc, err := natsio.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	return &Conn{nc: nc, cfg: cfg}, nil
}

// Raw returns the underlying nats.go connection for advanced use.
func (c *Conn) Raw() *natsio.Conn { return c.nc }

// Close drains and closes the underlying NATS connection. Subs and JS
// Consumers owned by callers should be stopped first; Close does not wait
// for in-flight handlers.
func (c *Conn) Close() {
	if c.nc != nil {
		_ = c.nc.Drain()
	}
}

// Message embeds *natsio.Msg so every field/method (Data, Subject, Header,
// Reply, Respond, ...) is reachable directly from a *Message.
type Message struct {
	*natsio.Msg
}

// Handler is the core-NATS handler signature. Return nil to drop, return
// an error to indicate failure — the wrapper does nothing with the error
// for core NATS (no ack/retry exists), but the signature is symmetric with
// JetHandler.
type Handler func(ctx context.Context, m *Message) error

// Sub is a running subscription with a worker pool.
type Sub struct {
	sub    *natsio.Subscription
	q      *queue.Queue[*Message]
	cancel context.CancelFunc
	pool   *worker.Pool
}

// Subscribe registers handler on subject and starts a worker pool. ctx
// is the parent for every handler invocation; cancelling it stops the pool.
func (c *Conn) Subscribe(ctx context.Context, subject string, h Handler) (*Sub, error) {
	return c.startSub(ctx, subject, "", h)
}

// QueueSubscribe joins a NATS queue group so messages are load-balanced
// across all subscribers sharing the same `group`.
func (c *Conn) QueueSubscribe(ctx context.Context, subject, group string, h Handler) (*Sub, error) {
	if group == "" {
		return nil, errors.New("nats: QueueSubscribe requires a non-empty group")
	}
	return c.startSub(ctx, subject, group, h)
}

func (c *Conn) startSub(ctx context.Context, subject, group string, h Handler) (*Sub, error) {
	if h == nil {
		return nil, errors.New("nats: handler is required")
	}
	subCtx, cancel := context.WithCancel(ctx)
	q := queue.New[*Message](c.cfg.queueSize())

	cb := func(raw *natsio.Msg) {
		_ = q.Push(subCtx, &Message{Msg: raw})
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

	pool := worker.Run(subCtx, c.cfg.workers(), q, func(ctx context.Context, m *Message) {
		_ = h(ctx, m)
	})

	return &Sub{
		sub:    ns,
		q:      q,
		cancel: cancel,
		pool:   pool,
	}, nil
}

// Unsubscribe stops the subscription and signals workers to exit. Returns
// once the NATS subscription is gone. In-flight handlers may still be
// running until they observe ctx cancellation — call Wait to block until
// they have actually exited.
func (s *Sub) Unsubscribe() error {
	err := s.sub.Unsubscribe()
	s.cancel()
	s.q.Close()
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

// PublishMsg sends a fully-formed *Message (lets caller set Header, Reply).
func (c *Conn) PublishMsg(m *Message) error {
	if m == nil || m.Msg == nil {
		return errors.New("nats: PublishMsg requires a non-nil Message")
	}
	return c.nc.PublishMsg(m.Msg)
}

// Request performs a synchronous request/reply round-trip. ctx bounds how
// long we wait for the reply.
func (c *Conn) Request(ctx context.Context, subject string, data []byte) (*Message, error) {
	raw, err := c.nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return nil, fmt.Errorf("nats request %q: %w", subject, err)
	}
	return &Message{Msg: raw}, nil
}
