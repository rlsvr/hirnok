package nats

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	natsio "github.com/nats-io/nats.go"
)

// ChanSub wraps a Sub and exposes a read-only Go channel carrying messages.
// It is intended for stream consumers that consume messages via a channel
// instead of a callback handler.
type ChanSub[T any] struct {
	ch        <-chan T
	rawCh     chan T
	done      chan struct{}
	sub       *Sub
	closeOnce sync.Once
}

// C returns the receive-only channel carrying messages. The channel is closed
// when the subscription is stopped or drained and all workers have exited.
func (s *ChanSub[T]) C() <-chan T { return s.ch }

// Sub returns the underlying *Sub.
func (s *ChanSub[T]) Sub() *Sub { return s.sub }

// Stop stops the subscription and signals workers to exit. The message channel
// is closed once all workers have exited.
func (s *ChanSub[T]) Stop() error {
	return s.sub.Stop()
}

// Unsubscribe is an alias for Stop.
func (s *ChanSub[T]) Unsubscribe() error {
	return s.Stop()
}

// Drain stops accepting new messages, flushes remaining queued items into the
// channel, waits for workers to finish, and closes the channel.
func (s *ChanSub[T]) Drain(ctx context.Context) error {
	if err := s.sub.Drain(ctx); err != nil {
		return err
	}
	return s.Wait(ctx)
}

// Wait blocks until all worker goroutines have exited and the channel has closed,
// or until ctx is cancelled.
func (s *ChanSub[T]) Wait(ctx context.Context) error {
	if err := s.sub.Wait(ctx); err != nil {
		return err
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stopped returns a channel that is closed when all workers have exited and
// the message channel has been closed.
func (s *ChanSub[T]) Stopped() <-chan struct{} {
	return s.done
}

// SubscribeChan subscribes to subject and streams raw *natsio.Msg messages into
// a buffered Go channel. Message order is preserved by default (1 worker).
func SubscribeChan(
	ctx context.Context,
	conn *Conn,
	subject string,
	bufferSize int,
	opts ...Option,
) (*ChanSub[*natsio.Msg], error) {
	return SubscribeChanTransform(ctx, conn, subject, bufferSize,
		func(_ context.Context, m *natsio.Msg) (*natsio.Msg, bool, error) {
			return m, true, nil
		},
		opts...,
	)
}

// SubscribeChanJSON subscribes to subject, unmarshals each JSON payload into T,
// and streams decoded items into a buffered Go channel. Messages that fail
// JSON decoding are skipped.
func SubscribeChanJSON[T any](
	ctx context.Context,
	conn *Conn,
	subject string,
	bufferSize int,
	opts ...Option,
) (*ChanSub[T], error) {
	return SubscribeChanTransform(ctx, conn, subject, bufferSize,
		func(_ context.Context, m *natsio.Msg) (T, bool, error) {
			var val T
			var data []byte
			if m != nil {
				data = m.Data
			}
			if err := json.Unmarshal(data, &val); err != nil {
				return val, false, err
			}
			return val, true, nil
		},
		opts...,
	)
}

// SubscribeChanTransform subscribes to subject, applies transform to each message,
// and streams results for which keep is true into a buffered Go channel.
func SubscribeChanTransform[T any](
	ctx context.Context,
	conn *Conn,
	subject string,
	bufferSize int,
	transform func(context.Context, *natsio.Msg) (T, bool, error),
	opts ...Option,
) (*ChanSub[T], error) {
	if conn == nil {
		return nil, errors.New("nats: conn is required")
	}
	if subject == "" {
		return nil, errors.New("nats: subject is required")
	}
	if transform == nil {
		return nil, errors.New("nats: transform is required")
	}

	if bufferSize <= 0 {
		bufferSize = 64
	}

	combinedOpts := make([]Option, 0, 2+len(opts))
	combinedOpts = append(combinedOpts, WithWorkers(1), WithQueueSize(bufferSize))
	combinedOpts = append(combinedOpts, opts...)
	run := conn.cfg.runOptions(combinedOpts)

	rawCh := make(chan T, run.queueSize)
	cs := &ChanSub[T]{
		ch:    rawCh,
		rawCh: rawCh,
		done:  make(chan struct{}),
	}

	handler := func(hCtx context.Context, m *natsio.Msg) error {
		val, keep, err := transform(hCtx, m)
		if err != nil || !keep {
			return err
		}
		select {
		case rawCh <- val:
			return nil
		case <-hCtx.Done():
			return hCtx.Err()
		}
	}

	sub, err := conn.Subscribe(ctx, subject, handler, combinedOpts...)
	if err != nil {
		close(rawCh)
		close(cs.done)
		return nil, err
	}
	cs.sub = sub

	go func() {
		<-sub.Stopped()
		cs.closeOnce.Do(func() {
			close(cs.rawCh)
			close(cs.done)
		})
	}()

	return cs, nil
}
