package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDelayedSend(t *testing.T) {
	_, q, clk := setup(t, Config{})
	if _, err := q.SendDelayed([]byte("later"), "", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	q.Send([]byte("now"), "")
	now := recv(t, q)
	if got := string(now.Message.Body); got != "now" {
		t.Fatalf("a delayed message must not block later ones, got %q", got)
	}
	q.Ack(now.LeaseID)
	expectEmpty(t, q)
	if s := q.Stats(); s.Delayed != 1 || s.Messages != 1 {
		t.Fatalf("stats %+v", s)
	}
	clk.Advance(9999 * time.Millisecond)
	expectEmpty(t, q)
	clk.Advance(time.Millisecond)
	if got := string(recv(t, q).Message.Body); got != "later" {
		t.Fatalf("got %q", got)
	}
}

func TestDelayedKeepsSendOrderOnceDue(t *testing.T) {
	_, q, clk := setup(t, Config{})
	q.SendDelayed([]byte("a"), "", 5*time.Second)
	q.SendDelayed([]byte("b"), "", time.Second)
	clk.Advance(10 * time.Second)
	for _, want := range []string{"a", "b"} {
		if got := string(recv(t, q).Message.Body); got != want {
			t.Fatalf("want %s got %s", want, got)
		}
	}
}

func TestDelayedInGroup(t *testing.T) {
	_, q, clk := setup(t, Config{})
	q.Send([]byte("g1"), "G")
	q.SendDelayed([]byte("g2"), "G", 10*time.Second) // non-head, delayed
	q.Send([]byte("g3"), "G")

	q.Ack(recv(t, q).LeaseID) // g1 done -> g2 becomes head but is not due
	expectEmpty(t, q)         // and g3 must wait behind it
	clk.Advance(10 * time.Second)
	for _, want := range []string{"g2", "g3"} {
		d := recv(t, q)
		if got := string(d.Message.Body); got != want {
			t.Fatalf("want %s got %s", want, got)
		}
		q.Ack(d.LeaseID)
	}
}

func TestDelayedSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	q, _ := b.Create("q", Config{})
	q.SendDelayed([]byte("d"), "", time.Minute)
	q.Send([]byte("h"), "G")
	q.SendDelayed([]byte("g"), "G", time.Minute) // non-head delayed
	b.Close()

	b = openB(t, dir, clk)
	defer b.Close()
	q = mustGet(t, b, "q")
	h := recv(t, q)
	if string(h.Message.Body) != "h" {
		t.Fatalf("got %q", h.Message.Body)
	}
	q.Ack(h.LeaseID)
	expectEmpty(t, q)
	clk.Advance(time.Minute)
	got := map[string]bool{}
	for range 2 {
		got[string(recv(t, q).Message.Body)] = true
	}
	if !got["d"] || !got["g"] {
		t.Fatalf("delays lost on restart: %v", got)
	}
}

func TestDelayedWakesLongPoll(t *testing.T) {
	b := newTestBroker(t, nil) // real clock
	q, _ := b.Create("q", Config{})
	start := time.Now()
	go func() {
		time.Sleep(20 * time.Millisecond)
		q.SendDelayed([]byte("x"), "", 100*time.Millisecond)
	}()
	d, err := q.Receive(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 100*time.Millisecond || el > 2*time.Second {
		t.Fatalf("delivered after %v, want ~120ms", el)
	}
	if string(d.Message.Body) != "x" {
		t.Fatalf("got %q", d.Message.Body)
	}
}

func TestDelayedValidation(t *testing.T) {
	_, q, _ := setup(t, Config{})
	for _, d := range []time.Duration{-time.Second, MaxSendDelay + time.Second} {
		if _, err := q.SendDelayed([]byte("x"), "", d); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("delay %v: %v", d, err)
		}
	}
}
