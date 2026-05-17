// Sustained backpressure: slow handler + fast producer for `duration`,
// then graceful shutdown. Reports how the queue behaved at saturation
// and verifies clean exit (Wait returns nil, all in-flight handlers
// observe their ctx).
//
//	./test/setup.sh                                          # in another terminal
//	go run ./test/stress/backpressure --duration 5s --handler-delay 5ms
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

func main() {
	var (
		url           = flag.String("url", envOr("NATS_URL", "nats://localhost:4222"), "NATS URL")
		workers       = flag.Int("workers", 2, "consumer worker goroutines")
		queueSize     = flag.Int("queue-size", 16, "per-subscription queue buffer")
		handlerDelay  = flag.Duration("handler-delay", 5*time.Millisecond, "per-message handler delay (slow handler)")
		duration      = flag.Duration("duration", 5*time.Second, "publish duration")
		shutdownGrace = flag.Duration("shutdown", 10*time.Second, "max time to wait for graceful shutdown")
	)
	flag.Parse()

	if err := run(*url, *workers, *queueSize, *handlerDelay, *duration, *shutdownGrace); err != nil {
		fmt.Fprintf(os.Stderr, "stress/backpressure: %v\n", err)
		os.Exit(1)
	}
}

func run(url string, workers, queueSize int, handlerDelay, duration, shutdownGrace time.Duration) error {
	conn, err := qpnats.Connect(qpnats.Config{
		URL:       url,
		Name:      "hirnok-stress-bp",
		QueueSize: queueSize,
		Workers:   workers,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	var processed atomic.Int64
	var ctxCanceledHandlers atomic.Int64

	sub, err := conn.Subscribe(context.Background(), "stress.bp", func(ctx context.Context, _ *natsio.Msg) error {
		select {
		case <-time.After(handlerDelay):
			processed.Add(1)
		case <-ctx.Done():
			ctxCanceledHandlers.Add(1)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	log.Printf("workers=%d queue=%d handler-delay=%v duration=%v", workers, queueSize, handlerDelay, duration)
	log.Printf("publishing for %v ...", duration)

	var published atomic.Int64
	stop := time.After(duration)
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := conn.Publish("stress.bp", []byte("x")); err != nil {
				log.Printf("publish err: %v", err)
				return
			}
			published.Add(1)
		}
	}()
	<-pubDone

	log.Printf("publish window closed: %d published, %d processed so far", published.Load(), processed.Load())
	log.Printf("graceful shutdown (Unsubscribe + Wait, %v max)...", shutdownGrace)

	shutdownStart := time.Now()
	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	waitErr := sub.Wait(waitCtx)
	shutdownTook := time.Since(shutdownStart)

	fmt.Println()
	fmt.Println("results:")
	fmt.Printf("  duration:          %v\n", duration)
	fmt.Printf("  published:         %d\n", published.Load())
	fmt.Printf("  processed:         %d\n", processed.Load())
	fmt.Printf("  ctx-canceled mid:  %d\n", ctxCanceledHandlers.Load())
	fmt.Printf("  drop ratio:        %.2f%%\n", percent(published.Load()-processed.Load()-ctxCanceledHandlers.Load(), published.Load()))
	fmt.Printf("  shutdown took:     %v\n", shutdownTook.Round(time.Millisecond))
	fmt.Printf("  Wait err:          %v\n", waitErr)

	if waitErr != nil {
		return fmt.Errorf("shutdown did not complete cleanly: %w", waitErr)
	}
	return nil
}

func percent(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return 100 * float64(num) / float64(den)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
