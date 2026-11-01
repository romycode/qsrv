package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
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
		Persistent:        true,
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

// Non-persistent queues delete a message when it is processed: acked,
// dropped or dead-lettered. Only the DLQ copy remains.
func TestAckDeletesUnlessPersistent(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, nil)
	mustCreate(t, b, "dlq", Config{})
	q := mustCreate(t, b, "q", Config{Retry: RetryPolicy{MaxAttempts: 1}, DeadLetterQueue: "dlq"})
	drop := mustCreate(t, b, "drop", Config{Retry: RetryPolicy{MaxAttempts: 1}})
	keep := mustCreate(t, b, "keep", Config{Persistent: true})

	q.Send([]byte("ok"), "")
	q.Ack(recv(t, q).LeaseID)
	q.Send([]byte("bad"), "")
	q.Nack(recv(t, q).LeaseID) // dead-lettered
	drop.Send([]byte("bad"), "")
	drop.Nack(recv(t, drop).LeaseID) // dropped
	keep.Send([]byte("ok"), "")
	keep.Ack(recv(t, keep).LeaseID)

	check := func(b *Broker) {
		t.Helper()
		for name, want := range map[string]Stats{
			"q":    {},
			"drop": {},
			"keep": {History: 1},
			"dlq":  {Messages: 1, Ready: 1},
		} {
			if s := mustGet(t, b, name).Stats(); s != want {
				t.Fatalf("%s: %+v, want %+v", name, s, want)
			}
		}
		var rows int
		b.store.r.QueryRow(`SELECT COUNT(*) FROM messages WHERE queue IN ('q', 'drop')`).Scan(&rows)
		if rows != 0 {
			t.Fatalf("%d processed rows left on disk", rows)
		}
	}
	check(b)
	b.Close()
	b = openB(t, dir, nil)
	defer b.Close()
	check(b)

	if _, err := b.Replay("q", ReplayRequest{To: "dlq"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("replay from a non-persistent queue: %v", err)
	}
}

func TestRetentionExpiresLiveMessages(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	q := mustCreate(t, b, "q", Config{Retention: time.Hour, VisibilityTimeout: 2 * time.Hour})
	keep := mustCreate(t, b, "keep", Config{Persistent: true})

	old, _ := q.Send([]byte("old"), "")
	q.Send([]byte("leased"), "")
	q.SendDelayed([]byte("delayed"), "", 30*time.Minute)
	keep.Send([]byte("old"), "")
	recv(t, q) // takes "old": in flight
	clk.Advance(90 * time.Minute)
	young, _ := q.Send([]byte("young"), "")

	b.Expire(context.Background())
	// "old" is in flight: its lease decides; "leased" and "delayed" expire.
	if s := q.Stats(); s.Messages != 2 || s.InFlight != 1 || s.Ready != 1 {
		t.Fatalf("after expiry: %+v", s)
	}
	if s := keep.Stats(); s.Messages != 1 {
		t.Fatalf("persistent queue expired messages: %+v", s)
	}
	clk.Advance(2 * time.Hour) // lease of "old" runs out; "young" is now old too
	b.Expire(context.Background())
	if s := q.Stats(); s != (Stats{}) {
		t.Fatalf("after second pass: %+v", s)
	}
	b.Close()
	b = openB(t, dir, clk)
	defer b.Close()
	if s := mustGet(t, b, "q").Stats(); s != (Stats{}) {
		t.Fatalf("expired messages back after restart: %+v (old %s, young %s)", s, old.ID, young.ID)
	}
}

func TestRetentionExpiryKeepsGroupOrder(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := newTestBroker(t, clk)
	q := mustCreate(t, b, "q", Config{Retention: time.Hour})
	q.Send([]byte("g1"), "g")
	q.Send([]byte("g2"), "g")
	clk.Advance(30 * time.Minute)
	q.Send([]byte("g3"), "g")
	clk.Advance(31 * time.Minute) // g1, g2 expire; g3 doesn't
	b.Expire(context.Background())
	d := recv(t, q)
	if string(d.Message.Body) != "g3" {
		t.Fatalf("group head after expiry: %q", d.Message.Body)
	}
	if s := q.Stats(); s.Messages != 1 || s.Groups != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestRetentionConfig(t *testing.T) {
	b := newTestBroker(t, nil)
	if q := mustCreate(t, b, "d", Config{}); q.Config().Retention != DefaultRetention {
		t.Fatalf("default retention %v", q.Config().Retention)
	}
	if q := mustCreate(t, b, "p", Config{Persistent: true, Retention: time.Hour}); q.Config().Retention != 0 {
		t.Fatalf("persistent queue keeps a retention: %v", q.Config().Retention)
	}
	if _, err := b.Create("short", Config{Retention: time.Second}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("retention under the minimum: %v", err)
	}
	q := mustCreate(t, b, "h", Config{Retention: time.Hour})
	if _, err := q.SendDelayed([]byte("x"), "", time.Hour); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("delay as long as the retention: %v", err)
	}
	if _, err := mustGet(t, b, "p").SendDelayed([]byte("x"), "", 2*time.Hour); err != nil {
		t.Fatalf("persistent queue refused a delay: %v", err)
	}
}

func TestReplay(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := newTestBroker(t, clk)
	src, _ := b.Create("orders", Config{Persistent: true})
	dst, _ := b.Create("orders-replay", Config{Persistent: true})

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
	src, _ := b.Create("src", Config{Persistent: true})
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
	src, _ := b.Create("src", Config{Retry: RetryPolicy{MaxAttempts: 1}, Persistent: true})
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

// Databases from before schema v2 kept history in every queue and never
// expired live messages: their queues come back persistent, history intact.
func TestMigrateV1QueuesBecomePersistent(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, nil)
	q := mustCreate(t, b, "q", Config{Persistent: true})
	q.Send([]byte("done"), "")
	q.Ack(recv(t, q).LeaseID)
	q.Send([]byte("live"), "")
	b.Close()

	// Turn it back into a v1 database: v1 configs have no Persistent field.
	db, err := sql.Open(driverName, dsn(filepath.Join(dir, dbFile), false, false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE queues SET config = '{"VisibilityTimeout":30000000000,"Retention":7776000000000000}'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE messages DROP COLUMN attributes`); err != nil { // added in v3
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	b = openB(t, dir, nil)
	defer b.Close()
	q = mustGet(t, b, "q")
	if c := q.Config(); !c.Persistent || c.Retention != 0 {
		t.Fatalf("migrated config %+v", c)
	}
	if s := q.Stats(); s.Messages != 1 || s.History != 1 {
		t.Fatalf("migrated stats %+v", s)
	}
	var v int
	b.store.r.QueryRow(`PRAGMA user_version`).Scan(&v)
	if v != schemaVersion {
		t.Fatalf("user_version %d", v)
	}
}
