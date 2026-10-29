package queue

import (
	"context"
	"testing"
	"time"
)

func TestGroupBlocksUntilAck(t *testing.T) {
	_, q, _ := setup(t, Config{})
	q.Send([]byte("a1"), "A")
	q.Send([]byte("a2"), "A")
	q.Send([]byte("b1"), "B")
	q.Send([]byte("free"), "")

	d1 := recv(t, q) // a1
	d2 := recv(t, q) // b1: other group not blocked by A
	d3 := recv(t, q) // free
	if string(d1.Message.Body) != "a1" || string(d2.Message.Body) != "b1" || string(d3.Message.Body) != "free" {
		t.Fatalf("got %s %s %s", d1.Message.Body, d2.Message.Body, d3.Message.Body)
	}
	expectEmpty(t, q) // a2 blocked behind a1
	q.Ack(d1.LeaseID)
	if d := recv(t, q); string(d.Message.Body) != "a2" {
		t.Fatalf("got %s", d.Message.Body)
	}
}

func TestGroupNackKeepsOrder(t *testing.T) {
	_, q, clk := setup(t, Config{Retry: RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Second}})
	q.Send([]byte("1"), "G")
	q.Send([]byte("2"), "G")
	q.Nack(recv(t, q).LeaseID)
	expectEmpty(t, q) // head backing off, 2 must not overtake it
	clk.Advance(time.Second)
	if d := recv(t, q); string(d.Message.Body) != "1" || d.Message.Attempts != 2 {
		t.Fatalf("got %s/%d", d.Message.Body, d.Message.Attempts)
	}
}

func TestGroupDeadLetterUnblocks(t *testing.T) {
	b := newTestBroker(t, &fakeClock{t: time.Unix(0, 0)})
	b.Create("dlq", Config{})
	q, _ := b.Create("q", Config{Retry: RetryPolicy{MaxAttempts: 1}, DeadLetterQueue: "dlq"})
	q.Send([]byte("poison"), "G")
	q.Send([]byte("next"), "G")
	q.Nack(recv(t, q).LeaseID)
	if d := recv(t, q); string(d.Message.Body) != "next" {
		t.Fatalf("got %s", d.Message.Body)
	}
	dlq, _ := b.Get("dlq")
	if d := recv(t, dlq); d.Message.GroupID != "G" {
		t.Fatalf("group not preserved in DLQ: %q", d.Message.GroupID)
	}
}

func TestStrictOrdering(t *testing.T) {
	_, q, _ := setup(t, Config{Ordering: OrderingStrict})
	q.Send([]byte("1"), "")
	q.Send([]byte("2"), "whatever") // group ignored: whole queue is one group
	d := recv(t, q)
	expectEmpty(t, q)
	q.Ack(d.LeaseID)
	if d := recv(t, q); string(d.Message.Body) != "2" {
		t.Fatalf("got %s", d.Message.Body)
	}
}

func TestGroupLongPollWakesOnAck(t *testing.T) {
	b := newTestBroker(t, nil)
	q, _ := b.Create("q", Config{})
	q.Send([]byte("1"), "G")
	q.Send([]byte("2"), "G")
	d, _ := q.Receive(context.Background(), 0)
	go func() { time.Sleep(50 * time.Millisecond); q.Ack(d.LeaseID) }()
	start := time.Now()
	d2, err := q.Receive(context.Background(), 2*time.Second)
	if err != nil || string(d2.Message.Body) != "2" || time.Since(start) > time.Second {
		t.Fatalf("got %v %v after %v", d2, err, time.Since(start))
	}
}

func TestMaxMessages(t *testing.T) {
	_, q, _ := setup(t, Config{MaxMessages: 1})
	if _, err := q.Send([]byte("a"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Send([]byte("b"), ""); err != ErrQueueFull {
		t.Fatalf("got %v", err)
	}
}
