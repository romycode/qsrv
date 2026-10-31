package queue

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func ids(p PeekPage) []string {
	var out []string
	for _, m := range p.Messages {
		out = append(out, m.ID)
	}
	return out
}

func TestPeek(t *testing.T) {
	b := newTestBroker(t, nil)
	dlq, _ := b.Create("dlq", Config{})
	q, _ := b.Create("q", Config{Retry: RetryPolicy{MaxAttempts: 1}, DeadLetterQueue: "dlq"})

	m1, _ := q.Send([]byte("one"), "g1")
	m2, _ := q.Send([]byte("two"), "")
	m3, _ := q.SendDelayed([]byte("three"), "", time.Hour)
	m4, _ := q.Send([]byte("four"), "")

	d := recv(t, q) // one -> in flight
	before, _ := q.Peek(PeekRequest{})
	if !slices.Equal(ids(before), []string{m1.ID, m2.ID, m3.ID, m4.ID}) {
		t.Fatalf("live order %v", ids(before))
	}
	byID := map[string]MessageInfo{}
	for _, m := range before.Messages {
		byID[m.ID] = m
	}
	if m := byID[m1.ID]; m.State != "in_flight" || m.Attempts != 1 || m.LeaseExpiresAt.IsZero() || m.GroupID != "g1" || m.Size != 3 {
		t.Fatalf("in flight message %+v", m)
	}
	if m := byID[m3.ID]; m.State != "delayed" || m.AvailableAt.IsZero() {
		t.Fatalf("delayed message %+v", m)
	}
	if m := byID[m2.ID]; m.State != "ready" || m.Attempts != 0 {
		t.Fatalf("ready message %+v", m)
	}

	// Peeking changed nothing: the next receive is still "two".
	d2 := recv(t, q)
	if got := string(d2.Message.Body); got != "two" {
		t.Fatalf("receive after peek: %q", got)
	}

	// Pagination.
	p1, _ := q.Peek(PeekRequest{Limit: 2})
	p2, _ := q.Peek(PeekRequest{Limit: 2, Cursor: p1.Next})
	if len(p1.Messages) != 2 || p1.Next == 0 || len(p2.Messages) != 2 || p2.Next != 0 ||
		!slices.Equal(append(ids(p1), ids(p2)...), []string{m1.ID, m2.ID, m3.ID, m4.ID}) {
		t.Fatalf("pages %v (next %d) %v (next %d)", ids(p1), p1.Next, ids(p2), p2.Next)
	}
	if only, _ := q.Peek(PeekRequest{States: []string{"delayed"}}); !slices.Equal(ids(only), []string{m3.ID}) {
		t.Fatalf("state filter %v", ids(only))
	}

	// History: acked and dead-lettered, newest first.
	q.Ack(d.LeaseID)   // one -> acked
	q.Nack(d2.LeaseID) // two -> dead lettered (1 attempt)
	h, err := q.Peek(PeekRequest{History: true})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ids(h), []string{m2.ID, m1.ID}) {
		t.Fatalf("history %v", ids(h))
	}
	if h.Messages[0].State != "dead_lettered" || h.Messages[0].MovedTo != "dlq" || h.Messages[0].DoneAt.IsZero() {
		t.Fatalf("dead lettered %+v", h.Messages[0])
	}
	if h.Messages[1].State != "acked" {
		t.Fatalf("acked %+v", h.Messages[1])
	}
	hp, _ := q.Peek(PeekRequest{History: true, Limit: 1})
	hp2, _ := q.Peek(PeekRequest{History: true, Limit: 1, Cursor: hp.Next})
	if !slices.Equal(append(ids(hp), ids(hp2)...), []string{m2.ID, m1.ID}) || hp2.Next != 0 {
		t.Fatalf("history pages %v %v", ids(hp), ids(hp2))
	}
	if dl, _ := dlq.Peek(PeekRequest{}); len(dl.Messages) != 1 || dl.Messages[0].State != "ready" {
		t.Fatalf("dlq %+v", dl)
	}

	// One message, with body, live or processed.
	info, body, err := q.PeekMessage(m1.ID)
	if err != nil || string(body) != "one" || info.State != "acked" {
		t.Fatalf("peek message: %+v %q %v", info, body, err)
	}
	if _, _, err := q.PeekMessage("nope"); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("missing message: %v", err)
	}

	// Errors.
	if _, err := q.Peek(PeekRequest{States: []string{"acked"}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("history state in live view: %v", err)
	}
	if _, err := q.Peek(PeekRequest{Limit: MaxPeekLimit + 1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("limit: %v", err)
	}
}

// Messages behind the head of their ordering group are "waiting", not
// "ready": they are stored as ready, but can't be delivered yet.
func TestPeekWaitingInGroup(t *testing.T) {
	b := newTestBroker(t, nil)
	q, _ := b.Create("q", Config{})
	h, _ := q.Send([]byte("head"), "g")
	w, _ := q.Send([]byte("behind"), "g")
	n, _ := q.Send([]byte("no group"), "")

	states := func(p PeekPage) []string {
		var out []string
		for _, m := range p.Messages {
			out = append(out, m.ID+"="+m.State)
		}
		return out
	}
	all, _ := q.Peek(PeekRequest{})
	if want := []string{h.ID + "=ready", w.ID + "=waiting", n.ID + "=ready"}; !slices.Equal(states(all), want) {
		t.Fatalf("states %v, want %v", states(all), want)
	}
	if p, _ := q.Peek(PeekRequest{States: []string{"waiting"}}); !slices.Equal(ids(p), []string{w.ID}) {
		t.Fatalf("waiting only: %v", ids(p))
	}
	if p, _ := q.Peek(PeekRequest{States: []string{"ready"}}); !slices.Equal(ids(p), []string{h.ID, n.ID}) {
		t.Fatalf("ready only: %v", ids(p))
	}
	if m, _, _ := q.PeekMessage(w.ID); m.State != "waiting" {
		t.Fatalf("one message: %s", m.State)
	}
	q.Ack(recv(t, q).LeaseID) // the head goes: "behind" is the head now
	if m, _, _ := q.PeekMessage(w.ID); m.State != "ready" {
		t.Fatalf("after the head is acked: %s", m.State)
	}
}
