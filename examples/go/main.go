// Command example shows a producer and a pool of consumers using the qsrv Go
// client: ordering groups, a delayed message, retries with backoff, a
// dead-letter queue, and lease heartbeats for slow jobs.
//
//	export QSRV_API_KEYS=$(openssl rand -hex 32)
//	go run ./cmd/qsrv -data-dir /tmp/qsrv-example &
//	QSRV_ADDR=http://localhost:8080 QSRV_API_KEY=$QSRV_API_KEYS go run ./examples/go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

const (
	queue = "example-orders"
	dlq   = "example-orders-dlq"
)

// OrderEvent is our payload. qsrv bodies are opaque bytes; JSON is a choice.
type OrderEvent struct {
	OrderID  int    `json:"order_id"`
	Customer string `json:"customer"`
	Kind     string `json:"kind"` // created, paid, shipped, reminder, broken
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := client.New(env("QSRV_ADDR", "http://localhost:8080"), os.Getenv("QSRV_API_KEY"))
	if err := setup(ctx, c); err != nil {
		log.Fatal(err)
	}

	// --- Producer ---------------------------------------------------------
	// Events of one customer share a group: qsrv delivers them strictly in
	// order, one at a time. Different customers are processed in parallel.
	events := []OrderEvent{
		{1, "alice", "created"}, {1, "alice", "paid"}, {1, "alice", "shipped"},
		{2, "bob", "created"}, {2, "bob", "paid"},
		{3, "carol", "broken"}, // fails every time: ends in the DLQ
	}
	for _, e := range events {
		send(ctx, c, e, client.SendOptions{GroupID: e.Customer})
	}
	// Not deliverable for 2s: scheduled work without a cron.
	send(ctx, c, OrderEvent{2, "bob", "reminder"}, client.SendOptions{Delay: 2 * time.Second})

	// --- Consumers --------------------------------------------------------
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var done atomic.Int32
	want := int32(len(events)) // 5 good + 1 reminder; the broken one goes to the DLQ
	var wg sync.WaitGroup
	for w := 1; w <= 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consume(ctx, c, w, func() {
				if done.Add(1) == want {
					cancel() // everything handled: stop the workers
				}
			})
		}()
	}
	wg.Wait()

	// --- Result -----------------------------------------------------------
	d, err := c.GetQueue(context.Background(), dlq)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nhandled %d/%d messages, %d in the DLQ\n", done.Load(), want, d.Stats.Messages)
	if done.Load() != want || d.Stats.Messages != 1 {
		os.Exit(1)
	}
}

// setup (re)creates the queues. Retries: up to 3 deliveries with 200ms,
// 400ms backoff; then the message moves to the DLQ (and stays in history).
func setup(ctx context.Context, c *client.Client) error {
	for _, q := range []string{queue, dlq} { // a DLQ can't be deleted while in use
		if err := c.DeleteQueue(ctx, q); err != nil && !client.IsNotFound(err) {
			return err
		}
	}
	if _, err := c.CreateQueue(ctx, dlq, api.QueueConfig{}); err != nil {
		return err
	}
	_, err := c.CreateQueue(ctx, queue, api.QueueConfig{
		VisibilityTimeout: api.Duration(5 * time.Second), // lease length; heartbeat for longer jobs
		Retry: &api.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: api.Duration(200 * time.Millisecond),
			MaxBackoff:     api.Duration(time.Second),
			Multiplier:     2,
		},
		DeadLetterQueue: dlq,
	})
	return err
}

func send(ctx context.Context, c *client.Client, e OrderEvent, o client.SendOptions) {
	body, _ := json.Marshal(e)
	m, err := c.SendWith(ctx, queue, body, o)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sent     %s %+v", m.ID[:8], e)
}

// consume long-polls, handles, and acks or nacks. Handlers must be
// idempotent: delivery is at-least-once.
func consume(ctx context.Context, c *client.Client, worker int, handled func()) {
	for ctx.Err() == nil {
		d, err := c.Receive(ctx, queue, 5*time.Second) // returns as soon as a message is ready
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("worker %d: receive: %v", worker, err)
				time.Sleep(time.Second) // server down or restarting: back off
			}
			continue
		}
		if d == nil {
			continue // nothing within the wait
		}

		var e OrderEvent
		err = json.Unmarshal(d.Message.Bytes(), &e)
		if err == nil {
			err = withHeartbeat(ctx, c, d.LeaseID, func() error { return handle(e) })
		}
		// Settle with a fresh context: a shutdown must not lose the ack.
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err != nil {
			log.Printf("worker %d: FAILED  %s %+v attempt %d: %v", worker, d.Message.ID[:8], e, d.Message.Attempts, err)
			_ = c.Nack(sctx, queue, d.LeaseID) // retry policy decides when it comes back
		} else if err := c.Ack(sctx, queue, d.LeaseID); client.IsLeaseGone(err) {
			log.Printf("worker %d: lease lost for %s: it will be redelivered", worker, d.Message.ID[:8])
		} else if err == nil {
			log.Printf("worker %d: handled %s %+v", worker, d.Message.ID[:8], e)
			handled()
		}
		scancel()
	}
}

func handle(e OrderEvent) error {
	switch e.Kind {
	case "broken":
		return errors.New("cannot process this order")
	case "shipped":
		time.Sleep(6 * time.Second) // longer than the 5s lease: the heartbeat keeps it
	}
	return nil
}

// withHeartbeat extends the lease while fn runs, so slow jobs are not
// redelivered to another worker in the middle.
func withHeartbeat(ctx context.Context, c *client.Client, lease string, fn func() error) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		t := time.NewTicker(2 * time.Second) // well under the 5s visibility timeout
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if _, err := c.Extend(ctx, queue, lease, 5*time.Second); err != nil {
					log.Printf("heartbeat: %v", err)
				}
			}
		}
	}()
	return fn()
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
