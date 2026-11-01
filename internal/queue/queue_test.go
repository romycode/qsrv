package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func setup(t *testing.T, cfg Config) (*Broker, *Queue, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := newTestBroker(t, clk)
	q, err := b.Create("q", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b, q, clk
}

func recv(t *testing.T, q *Queue) Delivery {
	t.Helper()
	d, err := q.Receive(context.Background(), 0)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	return d
}

func expectEmpty(t *testing.T, q *Queue) {
	t.Helper()
	if _, err := q.Receive(context.Background(), 0); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected empty, got %v", err)
	}
}

func TestFIFO(t *testing.T) {
	_, q, _ := setup(t, Config{})
	for i := range 5 {
		q.Send([]byte(fmt.Sprint(i)), "")
	}
	for i := range 5 {
		if got := string(recv(t, q).Message.Body); got != fmt.Sprint(i) {
			t.Fatalf("want %d got %s", i, got)
		}
	}
	expectEmpty(t, q)
}

func TestNackReturnsToOriginalPosition(t *testing.T) {
	_, q, _ := setup(t, Config{Retry: RetryPolicy{MaxAttempts: 5}}) // no backoff
	q.Send([]byte("a"), "")
	q.Send([]byte("b"), "")
	d := recv(t, q)
	if err := q.Nack(d.LeaseID); err != nil {
		t.Fatal(err)
	}
	d2 := recv(t, q)
	if string(d2.Message.Body) != "a" || d2.Message.Attempts != 2 {
		t.Fatalf("want a/2, got %s/%d", d2.Message.Body, d2.Message.Attempts)
	}
}

func TestBackoff(t *testing.T) {
	_, q, clk := setup(t, Config{Retry: RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Second, Multiplier: 2}})
	q.Send([]byte("a"), "")
	q.Nack(recv(t, q).LeaseID) // backoff 1s
	expectEmpty(t, q)
	clk.Advance(time.Second)
	q.Nack(recv(t, q).LeaseID) // backoff 2s
	clk.Advance(1999 * time.Millisecond)
	expectEmpty(t, q)
	clk.Advance(time.Millisecond)
	if d := recv(t, q); d.Message.Attempts != 3 {
		t.Fatalf("attempts %d", d.Message.Attempts)
	}
}

func TestLeaseExpiryRedelivers(t *testing.T) {
	_, q, clk := setup(t, Config{VisibilityTimeout: 5 * time.Second, Retry: RetryPolicy{MaxAttempts: 3}})
	q.Send([]byte("a"), "")
	d := recv(t, q)
	clk.Advance(6 * time.Second)
	if err := q.Ack(d.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("late ack must fail, got %v", err)
	}
	if d2 := recv(t, q); d2.Message.ID != d.Message.ID || d2.Message.Attempts != 2 {
		t.Fatalf("expected redelivery, got %+v", d2.Message)
	}
}

func TestExtend(t *testing.T) {
	_, q, clk := setup(t, Config{VisibilityTimeout: 5 * time.Second})
	q.Send([]byte("a"), "")
	d := recv(t, q)
	clk.Advance(4 * time.Second)
	if _, err := q.Extend(d.LeaseID, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(9 * time.Second)
	if err := q.Ack(d.LeaseID); err != nil {
		t.Fatalf("ack after extend: %v", err)
	}
}

func TestDoubleAck(t *testing.T) {
	_, q, _ := setup(t, Config{})
	q.Send([]byte("a"), "")
	d := recv(t, q)
	if err := q.Ack(d.LeaseID); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(d.LeaseID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("double ack: %v", err)
	}
}

func TestDeadLetter(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBroker(t, clk)
	dlq, _ := b.Create("dlq", Config{})
	q, err := b.Create("q", Config{Retry: RetryPolicy{MaxAttempts: 2}, DeadLetterQueue: "dlq"})
	if err != nil {
		t.Fatal(err)
	}
	q.Send([]byte("poison"), "")
	q.Nack(recv(t, q).LeaseID)
	q.Nack(recv(t, q).LeaseID)
	expectEmpty(t, q)
	d := recv(t, dlq)
	if string(d.Message.Body) != "poison" || d.Message.Attempts != 1 {
		t.Fatalf("dlq got %+v", d.Message)
	}
}

func TestLongPollWakesOnSend(t *testing.T) {
	b := newTestBroker(t, nil)
	q, _ := b.Create("q", Config{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Send([]byte("x"), "")
	}()
	start := time.Now()
	d, err := q.Receive(context.Background(), 2*time.Second)
	if err != nil || string(d.Message.Body) != "x" {
		t.Fatalf("got %v %v", d, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("long poll did not wake on send")
	}
}

func TestDeleteUnblocksPollers(t *testing.T) {
	b := newTestBroker(t, nil)
	q, _ := b.Create("q", Config{})
	errc := make(chan error)
	go func() { _, err := q.Receive(context.Background(), 5*time.Second); errc <- err }()
	time.Sleep(20 * time.Millisecond)
	b.Delete("q")
	select {
	case err := <-errc:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("poller not released")
	}
}

// Many producers / consumers; every message must be acked exactly once.
func TestConcurrentExactlyOnceAck(t *testing.T) {
	b := newTestBroker(t, nil)
	q, _ := b.Create("q", Config{VisibilityTimeout: time.Minute, Persistent: true})
	const producers, perProducer, consumers = 8, 2000, 8
	total := producers * perProducer

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perProducer {
				q.Send([]byte(fmt.Sprintf("%d-%d", p, i)), "")
			}
		}()
	}

	var seen sync.Map
	var acked atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cwg sync.WaitGroup
	for range consumers {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for acked.Load() < int64(total) {
				d, err := q.Receive(ctx, 50*time.Millisecond)
				if errors.Is(err, ErrEmpty) {
					continue
				}
				if err != nil {
					return
				}
				if _, dup := seen.LoadOrStore(d.Message.ID, true); dup {
					t.Errorf("duplicate delivery %s", d.Message.ID)
				}
				if err := q.Ack(d.LeaseID); err != nil {
					t.Errorf("ack: %v", err)
				}
				acked.Add(1)
			}
		}()
	}
	wg.Wait()
	cwg.Wait()
	if acked.Load() != int64(total) {
		t.Fatalf("acked %d/%d", acked.Load(), total)
	}
	if s := q.Stats(); live(s) != (Stats{}) || s.History != total {
		t.Fatalf("queue not drained: %+v", s)
	}
}

func TestDeadLetterSkipsRetries(t *testing.T) {
	b := newTestBroker(t, nil)
	dlq := mustCreate(t, b, "dlq", Config{})
	q := mustCreate(t, b, "q", Config{Retry: RetryPolicy{MaxAttempts: 5}, DeadLetterQueue: "dlq"})
	q.Send([]byte("a"), "G")
	q.Send([]byte("b"), "G")
	if err := q.DeadLetter(recv(t, q).LeaseID); err != nil {
		t.Fatal(err)
	}
	if d := recv(t, dlq); string(d.Message.Body) != "a" || d.Message.GroupID != "G" {
		t.Fatalf("dlq got %+v", d.Message)
	}
	if got := string(recv(t, q).Message.Body); got != "b" {
		t.Fatalf("group not unblocked: got %q", got)
	}
	if err := q.DeadLetter("nope"); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("unknown lease: %v", err)
	}

	noDLQ := mustCreate(t, b, "nodlq", Config{Retry: RetryPolicy{MaxAttempts: 5}, Persistent: true})
	noDLQ.Send([]byte("x"), "")
	noDLQ.DeadLetter(recv(t, noDLQ).LeaseID)
	if s := noDLQ.Stats(); s.Messages != 0 || s.History != 1 {
		t.Fatalf("without a DLQ it must be dropped into history: %+v", s)
	}
}
