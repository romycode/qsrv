package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestBroker opens a broker on a temp dir without fsync (fast).
func newTestBroker(tb testing.TB, clk Clock) *Broker {
	tb.Helper()
	b, err := Open(tb.TempDir(), Options{Clock: clk, NoSync: true, JanitorEvery: time.Hour, OnFatal: func(err error) { panic(err) }})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { b.Close() })
	return b
}

// openB opens a broker on dir with real fsync, for restart tests.
func openB(t *testing.T, dir string, clk Clock) *Broker {
	t.Helper()
	b, err := Open(dir, Options{Clock: clk, JanitorEvery: time.Hour, OnFatal: func(err error) { panic(err) }})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustGet(t *testing.T, b *Broker, name string) *Queue {
	t.Helper()
	q, err := b.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func live(s Stats) Stats { s.History = 0; return s }

func TestPersistLifecycle(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	b.Create("dlq", Config{})
	q, err := b.Create("q", Config{
		VisibilityTimeout: 30 * time.Second,
		Retry:             RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Second},
		DeadLetterQueue:   "dlq",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"a", "b", "c", "d"} {
		q.Send([]byte(s), "")
	}
	q.Ack(recv(t, q).LeaseID)  // a done
	held := recv(t, q)         // b in flight across restart
	q.Nack(recv(t, q).LeaseID) // c backing off 10s
	b.Close()

	b = openB(t, dir, clk)
	q = mustGet(t, b, "q")
	if s := q.Stats(); live(s) != (Stats{Messages: 3, Ready: 1, Delayed: 1, InFlight: 1}) || s.History != 1 {
		t.Fatalf("after restart: %+v", s)
	}
	if err := q.Ack(held.LeaseID); err != nil {
		t.Fatalf("lease must survive restart: %v", err)
	}
	d := recv(t, q)
	if string(d.Message.Body) != "d" {
		t.Fatalf("want d got %s", d.Message.Body)
	}
	q.Ack(d.LeaseID)
	clk.Advance(10 * time.Second)
	c := recv(t, q)
	if string(c.Message.Body) != "c" || c.Message.Attempts != 2 {
		t.Fatalf("got %s/%d", c.Message.Body, c.Message.Attempts)
	}
	q.Nack(c.LeaseID) // exhausted -> dlq
	b.Close()

	b = openB(t, dir, clk)
	defer b.Close()
	if s := mustGet(t, b, "q").Stats(); s.Messages != 0 || s.History != 4 {
		t.Fatalf("q: %+v", s)
	}
	if d := recv(t, mustGet(t, b, "dlq")); string(d.Message.Body) != "c" || d.Message.ID != c.Message.ID {
		t.Fatalf("dlq got %+v", d.Message)
	}
}

func TestPersistGroupsAndExpiry(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	q, _ := b.Create("q", Config{VisibilityTimeout: 5 * time.Second, Retry: RetryPolicy{MaxAttempts: 3}})
	q.Send([]byte("g1"), "G")
	q.Send([]byte("g2"), "G")
	first := recv(t, q)
	b.Close()

	b = openB(t, dir, clk)
	defer b.Close()
	q = mustGet(t, b, "q")
	expectEmpty(t, q) // group still blocked by in-flight g1
	clk.Advance(6 * time.Second)
	d := recv(t, q) // lease expired during downtime -> g1 redelivered
	if d.Message.ID != first.Message.ID || d.Message.Attempts != 2 {
		t.Fatalf("got %+v", d.Message)
	}
	q.Ack(d.LeaseID)
	if d := recv(t, q); string(d.Message.Body) != "g2" {
		t.Fatalf("got %s", d.Message.Body)
	}
}

func TestPersistDeleteAndRecreate(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, realClock{})
	q, _ := b.Create("q", Config{})
	q.Send([]byte("old"), "")
	b.Delete("q")
	q, _ = b.Create("q", Config{VisibilityTimeout: time.Minute})
	q.Send([]byte("new"), "")
	b.Close()

	b = openB(t, dir, realClock{})
	defer b.Close()
	q = mustGet(t, b, "q")
	if q.Config().VisibilityTimeout != time.Minute || q.Stats().Messages != 1 {
		t.Fatalf("cfg %+v stats %+v", q.Config(), q.Stats())
	}
	if d := recv(t, q); string(d.Message.Body) != "new" {
		t.Fatalf("got %s", d.Message.Body)
	}
}

func TestDeleteRefusedWhileUsedAsDLQ(t *testing.T) {
	b := newTestBroker(t, nil)
	b.Create("dlq", Config{})
	b.Create("q", Config{DeadLetterQueue: "dlq"})
	if err := b.Delete("dlq"); err == nil {
		t.Fatal("expected refusal")
	}
}

func TestPersistConcurrentSendsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, realClock{})
	q, _ := b.Create("q", Config{})
	var wg sync.WaitGroup
	for p := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 250 {
				q.Send([]byte(fmt.Sprintf("%d-%d", p, i)), "")
			}
		}()
	}
	wg.Wait()
	b.Close()

	b = openB(t, dir, realClock{})
	defer b.Close()
	if n := mustGet(t, b, "q").Stats().Messages; n != 2000 {
		t.Fatalf("recovered %d/2000", n)
	}
}

func TestRetentionPurge(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := newTestBroker(t, clk)
	q, _ := b.Create("q", Config{Retention: time.Hour})
	keep, _ := b.Create("forever", Config{}) // retention 0 = forever

	for _, qq := range []*Queue{q, keep} {
		qq.Send([]byte("old"), "")
		qq.Ack(recv(t, qq).LeaseID)
	}
	clk.Advance(2 * time.Hour)
	q.Send([]byte("new"), "")
	q.Ack(recv(t, q).LeaseID)
	q.Send([]byte("pending"), "") // live messages are never purged

	b.Purge(context.Background())
	if s := q.Stats(); s.History != 1 || s.Messages != 1 {
		t.Fatalf("q after purge: %+v", s)
	}
	if s := keep.Stats(); s.History != 1 {
		t.Fatalf("forever queue purged: %+v", s)
	}
}

func TestReplay(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := newTestBroker(t, clk)
	src, _ := b.Create("orders", Config{})
	dst, _ := b.Create("orders-replay", Config{})

	t0 := clk.Now()
	var ids []string
	for i := range 5 {
		g := "A"
		if i%2 == 1 {
			g = "B"
		}
		m, _ := src.Send([]byte(fmt.Sprint(i)), g)
		ids = append(ids, m.ID)
		src.Ack(recv(t, src).LeaseID)
		clk.Advance(time.Minute)
	}
	src.Send([]byte("live"), "") // not history: must not be replayed

	// paginate 2 by 2 over everything
	var got []string
	var after uint64
	for {
		res, err := b.Replay("orders", ReplayRequest{To: "orders-replay", After: after, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		after = res.Next
		for range res.Replayed {
			d := recv(t, dst)
			got = append(got, fmt.Sprintf("%s:%s:%s", d.Message.Body, d.Message.GroupID, d.Message.ReplayOf))
			dst.Ack(d.LeaseID)
		}
		if after == 0 {
			break
		}
	}
	want := []string{"0:A:" + ids[0], "1:B:" + ids[1], "2:A:" + ids[2], "3:B:" + ids[3], "4:A:" + ids[4]}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}

	// filters: group B, enqueued in [t0+1m, t0+4m)
	res, err := b.Replay("orders", ReplayRequest{To: "orders-replay", GroupID: "B", From: t0.Add(time.Minute), Until: t0.Add(4 * time.Minute)})
	if err != nil || res.Replayed != 2 || res.Next != 0 {
		t.Fatalf("filtered: %+v %v", res, err)
	}

	// replays are themselves history of the destination, replayable again
	if s := dst.Stats(); s.History != 5 {
		t.Fatalf("dst history %+v", s)
	}
}

func TestReplayRespectsCapacityAtomically(t *testing.T) {
	b := newTestBroker(t, nil)
	src, _ := b.Create("src", Config{})
	b.Create("dst", Config{MaxMessages: 2})
	for range 3 {
		src.Send([]byte("x"), "")
		src.Ack(recv(t, src).LeaseID)
	}
	if _, err := b.Replay("src", ReplayRequest{To: "dst"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	if s := mustGet(t, b, "dst").Stats(); s.Messages != 0 {
		t.Fatalf("partial replay: %+v", s)
	}
}

func TestReplayStateFilterAndPersistence(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, realClock{})
	src, _ := b.Create("src", Config{Retry: RetryPolicy{MaxAttempts: 1}})
	b.Create("dst", Config{})
	src.Send([]byte("ok"), "")
	src.Ack(recv(t, src).LeaseID)
	src.Send([]byte("poison"), "")
	src.Nack(recv(t, src).LeaseID) // no DLQ -> dropped

	res, err := b.Replay("src", ReplayRequest{To: "dst", States: []string{"dropped"}})
	if err != nil || res.Replayed != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	b.Close()

	b = openB(t, dir, realClock{})
	defer b.Close()
	if d := recv(t, mustGet(t, b, "dst")); string(d.Message.Body) != "poison" || d.Message.ReplayOf == "" {
		t.Fatalf("replayed copy after restart: %+v", d.Message)
	}
	if _, err := b.Replay("src", ReplayRequest{To: "dst", States: []string{"nope"}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad state accepted: %v", err)
	}
}
