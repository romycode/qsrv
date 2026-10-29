package queue

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func mustCreate(t *testing.T, b *Broker, name string, cfg Config) *Queue {
	t.Helper()
	q, err := b.Create(name, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestTopicFanOut(t *testing.T) {
	b := newTestBroker(t, nil)
	billing := mustCreate(t, b, "billing", Config{})
	email := mustCreate(t, b, "email", Config{})
	if _, err := b.CreateTopic("orders", []string{"email", "billing", "email"}); err != nil {
		t.Fatal(err)
	}
	pub, err := b.Publish("orders", []byte("order 1"), "customer-42", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pub) != 2 || pub[0].Queue != "billing" || pub[1].Queue != "email" || pub[0].Message.ID == pub[1].Message.ID {
		t.Fatalf("published %+v", pub)
	}
	for _, q := range []*Queue{billing, email} {
		d := recv(t, q)
		if string(d.Message.Body) != "order 1" || d.Message.GroupID != "customer-42" {
			t.Fatalf("%s got %+v", q.Name(), d.Message)
		}
		expectEmpty(t, q)
	}
}

func TestTopicPublishIsAllOrNothing(t *testing.T) {
	b := newTestBroker(t, nil)
	a := mustCreate(t, b, "a", Config{})
	full := mustCreate(t, b, "full", Config{MaxMessages: 1})
	full.Send([]byte("x"), "")
	b.CreateTopic("t", []string{"a", "full"})
	if _, err := b.Publish("t", []byte("y"), "", 0); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want queue full, got %v", err)
	}
	if s := a.Stats(); s.Messages != 0 {
		t.Fatalf("a got a copy of a rejected publish: %+v", s)
	}
}

func TestTopicDelayAndGroupsPerQueue(t *testing.T) {
	_, q, clk := setup(t, Config{})
	b := q.b
	b.CreateTopic("t", []string{"q"})
	b.Publish("t", []byte("later"), "", time.Minute)
	b.Publish("t", []byte("g1"), "G", 0)
	b.Publish("t", []byte("g2"), "G", 0)
	d := recv(t, q)
	if string(d.Message.Body) != "g1" {
		t.Fatalf("got %s", d.Message.Body)
	}
	expectEmpty(t, q) // g2 waits for g1, "later" for its delay
	q.Ack(d.LeaseID)
	q.Ack(recv(t, q).LeaseID) // g2
	clk.Advance(time.Minute)
	if got := string(recv(t, q).Message.Body); got != "later" {
		t.Fatalf("got %s", got)
	}
}

func TestTopicSubscriptions(t *testing.T) {
	b := newTestBroker(t, nil)
	mustCreate(t, b, "a", Config{})
	mustCreate(t, b, "b", Config{})

	if _, err := b.CreateTopic("t", []string{"missing"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("subscribing a missing queue: %v", err)
	}
	if _, err := b.CreateTopic("bad name", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := b.CreateTopic("t", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateTopic("t", nil); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if pub, err := b.Publish("t", []byte("x"), "", 0); err != nil || len(pub) != 0 {
		t.Fatalf("no subscribers: %v %v", pub, err)
	}

	b.Subscribe("t", "b")
	top, _ := b.Subscribe("t", "a")
	if again, _ := b.Subscribe("t", "a"); !slices.Equal(top.Queues, []string{"a", "b"}) || !slices.Equal(again.Queues, top.Queues) {
		t.Fatalf("subscriptions %v", top.Queues)
	}
	if _, err := b.Subscribe("t", "missing"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("subscribe missing queue: %v", err)
	}
	if _, err := b.Subscribe("nope", "a"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("unknown topic: %v", err)
	}

	if err := b.Delete("a"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("deleting a subscribed queue must fail, got %v", err)
	}
	top, _ = b.Unsubscribe("t", "a")
	b.Unsubscribe("t", "a")
	if !slices.Equal(top.Queues, []string{"b"}) {
		t.Fatalf("after unsubscribe %v", top.Queues)
	}
	if err := b.Delete("a"); err != nil {
		t.Fatalf("delete after unsubscribe: %v", err)
	}
	if got := b.Topics(); len(got) != 1 || got[0].Name != "t" {
		t.Fatalf("topics %+v", got)
	}
	if err := b.DeleteTopic("t"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("t", []byte("x"), "", 0); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("publish to deleted topic: %v", err)
	}
	if err := b.Delete("b"); err != nil {
		t.Fatalf("queue of a deleted topic must be deletable: %v", err)
	}
}

func TestTopicSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	b.Create("a", Config{})
	b.Create("b", Config{})
	b.CreateTopic("t", []string{"a", "b"})
	b.CreateTopic("gone", nil)
	b.Publish("t", []byte("x"), "", 0)
	b.DeleteTopic("gone")
	b.Unsubscribe("t", "b")
	b.Close()

	b = openB(t, dir, clk)
	defer b.Close()
	if top, err := b.GetTopic("t"); err != nil || !slices.Equal(top.Queues, []string{"a"}) {
		t.Fatalf("after restart: %+v %v", top, err)
	}
	if _, err := b.GetTopic("gone"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("deleted topic came back: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if got := string(recv(t, mustGet(t, b, name)).Message.Body); got != "x" {
			t.Fatalf("%s: %q", name, got)
		}
	}
}

// Publishers to overlapping topics, senders and consumers at once: no
// deadlock, every copy delivered exactly once per queue. Run with -race.
func TestTopicConcurrent(t *testing.T) {
	b := newTestBroker(t, nil)
	names := []string{"q1", "q2", "q3"}
	for _, n := range names {
		mustCreate(t, b, n, Config{})
	}
	b.CreateTopic("t12", []string{"q2", "q1"})
	b.CreateTopic("t23", []string{"q3", "q2"})
	b.CreateTopic("all", names)

	const per = 200
	var wg sync.WaitGroup
	for _, topic := range []string{"t12", "t23", "all"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				if _, err := b.Publish(topic, fmt.Appendf(nil, "%s-%d", topic, i), "", 0); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		q, _ := b.Get("q2")
		for range per {
			q.Send([]byte("direct"), "")
		}
	}()
	wg.Wait()

	// q1: t12+all, q2: t12+t23+all+direct, q3: t23+all
	want := map[string]int{"q1": 2 * per, "q2": 4 * per, "q3": 2 * per}
	for n, w := range want {
		q, _ := b.Get(n)
		if got := q.Stats().Messages; got != w {
			t.Fatalf("%s: %d messages, want %d", n, got, w)
		}
	}
}

// The copies of one publish are committed in a single transaction.
func TestTopicPublishIsOneCommit(t *testing.T) {
	var mu sync.Mutex
	var batches []int
	b, err := Open(t.TempDir(), Options{NoSync: true, JanitorEvery: time.Hour, Hooks: Hooks{OnCommit: func(_ time.Duration, ops int) {
		mu.Lock()
		batches = append(batches, ops)
		mu.Unlock()
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	for _, n := range []string{"a", "b", "c"} {
		mustCreate(t, b, n, Config{})
	}
	b.CreateTopic("t", []string{"a", "b", "c"})
	mu.Lock()
	batches = nil
	mu.Unlock()
	if _, err := b.Publish("t", []byte("x"), "", 0); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(batches, []int{3}) {
		t.Fatalf("commits per publish: %v, want one commit of 3 ops", batches)
	}
}
