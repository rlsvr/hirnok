// Core NATS throughput stress: N producers publish K messages total,
// M workers drain the subscription, we report msgs/sec.
//
//	./test/setup.sh                        # in another terminal
//	go run ./test/stress/core --producers 4 --workers 8 --messages 200000
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

func main() {
	var (
		url       = flag.String("url", envOr("NATS_URL", "nats://localhost:4222"), "NATS URL")
		producers = flag.Int("producers", 4, "publisher goroutines")
		workers   = flag.Int("workers", 8, "consumer worker goroutines")
		total     = flag.Int("messages", 200000, "total messages to publish")
		queueSize = flag.Int("queue-size", 1024, "per-subscription queue buffer")
		payload   = flag.Int("payload", 64, "payload size in bytes")
	)
	flag.Parse()

	if err := run(*url, *producers, *workers, *total, *queueSize, *payload); err != nil {
		fmt.Fprintf(os.Stderr, "stress/core: %v\n", err)
		os.Exit(1)
	}
}

func run(url string, producers, workers, total, queueSize, payloadSize int) error {
	conn, err := qpnats.Connect(qpnats.Config{
		URL:       url,
		Name:      "hirnok-stress-core",
		QueueSize: queueSize,
		Workers:   workers,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	var received atomic.Int64
	sub, err := conn.Subscribe(context.Background(), "stress.core", func(_ context.Context, _ *natsio.Msg) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	log.Printf("producers=%d workers=%d total=%d queue=%d payload=%dB", producers, workers, total, queueSize, payloadSize)
	log.Printf("publishing...")

	start := time.Now()
	perProducer := total / producers
	leftover := total - perProducer*producers

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		n := perProducer
		if p == 0 {
			n += leftover
		}
		go func() {
			defer wg.Done()
			for range n {
				if err := conn.Publish("stress.core", payload); err != nil {
					log.Printf("publish err: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	publishElapsed := time.Since(start)
	log.Printf("publish done in %v (%.0f msgs/sec)", publishElapsed.Round(time.Millisecond), float64(total)/publishElapsed.Seconds())

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() >= int64(total) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	got := received.Load()

	fmt.Println()
	fmt.Println("results:")
	fmt.Printf("  duration:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  published:   %d\n", total)
	fmt.Printf("  received:    %d\n", got)
	fmt.Printf("  throughput:  %.0f msgs/sec\n", float64(got)/elapsed.Seconds())

	if got != int64(total) {
		return fmt.Errorf("only received %d / %d", got, total)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
