// JetStream throughput stress: publish K messages async to a JS stream,
// then consume them via worker pool, report msgs/sec for each phase.
//
//	./test/setup.sh                              # in another terminal
//	go run ./test/stress/jetstream --messages 50000 --workers 8
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	qpnats "github.com/rlsvr/hirnok/pkg/nats"
)

func main() {
	var (
		url       = flag.String("url", envOr("NATS_URL", "nats://localhost:4222"), "NATS URL")
		workers   = flag.Int("workers", 8, "consumer worker goroutines")
		total     = flag.Int("messages", 50000, "total messages to publish")
		queueSize = flag.Int("queue-size", 1024, "per-subscription queue buffer")
		payload   = flag.Int("payload", 128, "payload size in bytes")
		ackWait   = flag.Duration("ackwait", 30*time.Second, "JS AckWait")
	)
	flag.Parse()

	if err := run(*url, *workers, *total, *queueSize, *payload, *ackWait); err != nil {
		fmt.Fprintf(os.Stderr, "stress/jetstream: %v\n", err)
		os.Exit(1)
	}
}

func run(url string, workers, total, queueSize, payloadSize int, ackWait time.Duration) error {
	conn, err := qpnats.Connect(qpnats.Config{
		URL:       url,
		Name:      "hirnok-stress-js",
		QueueSize: queueSize,
		Workers:   workers,
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	js, err := conn.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	streamName := "STRESS_JS"
	subject := "stress.js"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if _, err := js.Raw().CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	// Best-effort: purge previous run so counts are clean.
	if s, err := js.Raw().Stream(ctx, streamName); err == nil {
		_ = s.Purge(ctx)
	}

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	log.Printf("workers=%d total=%d queue=%d payload=%dB", workers, total, queueSize, payloadSize)
	log.Printf("publishing async...")

	pubStart := time.Now()
	futures := make([]jetstream.PubAckFuture, 0, total)
	for range total {
		f, err := js.PublishAsync(subject, payload)
		if err != nil {
			return fmt.Errorf("publishAsync: %w", err)
		}
		futures = append(futures, f)
	}
	for i, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("publish ack %d: %w", i, err)
		case <-time.After(60 * time.Second):
			return errors.New("publish ack timeout")
		}
	}
	pubElapsed := time.Since(pubStart)
	log.Printf("publish done in %v (%.0f msgs/sec)", pubElapsed.Round(time.Millisecond), float64(total)/pubElapsed.Seconds())

	var received atomic.Int64
	conStart := time.Now()
	cons, err := js.NewConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "stress-consumer",
		FilterSubject: subject,
		AckWait:       ackWait,
	}, func(_ context.Context, _ jetstream.Msg) error {
		received.Add(1)
		return nil
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	defer cons.Stop()

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if received.Load() >= int64(total) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	conElapsed := time.Since(conStart)
	got := received.Load()

	fmt.Println()
	fmt.Println("results:")
	fmt.Printf("  publish:     %v  (%.0f msgs/sec)\n", pubElapsed.Round(time.Millisecond), float64(total)/pubElapsed.Seconds())
	fmt.Printf("  consume:     %v  (%.0f msgs/sec)\n", conElapsed.Round(time.Millisecond), float64(got)/conElapsed.Seconds())
	fmt.Printf("  total:       %d\n", total)
	fmt.Printf("  received:    %d\n", got)

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
