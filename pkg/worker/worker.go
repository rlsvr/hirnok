// Package worker provides a small generic worker pool that drains a
// queue.Queue[T] by calling a dispatch func on each item until the queue
// is closed or the context is canceled. It is the natural companion to
// pkg/queue: anywhere you have a bounded queue and want N workers
// processing items from it, Run gives you that pool plus a lifecycle
// signal (Stopped / Wait).
package worker

import (
	"context"

	"github.com/rlsvr/hirnok/pkg/queue"
)

// Pool is a running set of N workers fanning items out of a queue into
// a dispatch func. Pool stops when the queue is closed or the context
// passed to Run is canceled.
type Pool struct {
	stopped chan struct{}
}

// Run spawns n goroutines that each pop from q and call dispatch(ctx, item).
// dispatch is expected to handle its own errors; the pool itself only cares
// about lifecycle. Each worker exits when q.Pop returns an error (ctx
// canceled or queue closed).
//
// n must be > 0. Panics if not.
func Run[T any](ctx context.Context, n int, q *queue.Queue[T], dispatch func(context.Context, T)) *Pool {
	if n <= 0 {
		panic("worker: n must be > 0")
	}
	p := &Pool{stopped: make(chan struct{})}
	done := make(chan struct{}, n)
	for range n {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				item, err := q.Pop(ctx)
				if err != nil {
					return
				}
				dispatch(ctx, item)
			}
		}()
	}
	go func() {
		for range n {
			<-done
		}
		close(p.stopped)
	}()
	return p
}

// Stopped returns a channel that is closed once all workers have exited.
// Workers exit after returning from their current dispatch call, so this
// fires only after in-flight work has completed.
func (p *Pool) Stopped() <-chan struct{} { return p.stopped }

// Wait blocks until all workers have exited, or ctx is canceled. Returns
// nil on clean shutdown, ctx.Err() if the wait was canceled.
func (p *Pool) Wait(ctx context.Context) error {
	select {
	case <-p.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
