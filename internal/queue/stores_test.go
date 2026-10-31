package queue

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func countRows(t *testing.T, st *store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := st.r.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestOneDatabasePerTenant(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	a, _ := b.Create("acme/orders", Config{Retention: time.Hour})
	g, _ := b.Create("globex/orders", Config{})
	p, _ := b.Create("plain", Config{})
	b.Create("acme/billing", Config{})
	if _, err := b.CreateTopic("acme/events", []string{"acme/orders", "acme/billing"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("acme/events", []byte("evt"), "", 0); err != nil {
		t.Fatal(err)
	}
	g.Send([]byte("g1"), "")
	p.Send([]byte("p1"), "")
	a.Ack(recv(t, a).LeaseID) // "evt" in acme/orders -> history

	for _, tn := range []string{"acme", "globex"} {
		if _, err := os.Stat(filepath.Join(dir, "tenants", tn, "qsrv.db")); err != nil {
			t.Fatalf("database of %s: %v", tn, err)
		}
	}
	if got := countRows(t, b.storeOf(""), `SELECT COUNT(*) FROM queues`); got != 1 {
		t.Fatalf("root database has %d queues, want only %q", got, "plain")
	}
	if got := countRows(t, b.storeOf("acme/x"), `SELECT COUNT(*) FROM messages`); got != 2 {
		t.Fatalf("acme database has %d messages, want 2 (one per subscriber)", got)
	}
	if files := b.StoreFiles(); len(files) != 3 || files[0].Tenant != "" || files[1].Tenant != "acme" || files[2].Tenant != "globex" {
		t.Fatalf("store files %+v", files)
	}
	if c, ops := b.CommitStats(); c == 0 || ops < 9 {
		t.Fatalf("commit stats summed over tenants: %d commits, %d ops", c, ops)
	}
	b.Close()

	b = openB(t, dir, clk)
	defer b.Close()
	if got := b.List(); !slices.Equal(got, []string{"acme/billing", "acme/orders", "globex/orders", "plain"}) {
		t.Fatalf("queues after restart %v", got)
	}
	for q, want := range map[string]string{"acme/billing": "evt", "globex/orders": "g1", "plain": "p1"} {
		if got := string(recv(t, mustGet(t, b, q)).Message.Body); got != want {
			t.Fatalf("%s: got %q, want %q", q, got, want)
		}
	}
	if top, err := b.GetTopic("acme/events"); err != nil || len(top.Queues) != 2 {
		t.Fatalf("topic after restart: %+v %v", top, err)
	}
	a = mustGet(t, b, "acme/orders")
	if res, err := b.Replay("acme/orders", ReplayRequest{To: "acme/orders"}); err != nil || res.Replayed != 1 {
		t.Fatalf("replay within the tenant database: %+v %v", res, err)
	}
	if got := string(recv(t, a).Message.Body); got != "evt" {
		t.Fatalf("replayed body %q", got)
	}

	clk.Advance(2 * time.Hour)
	b.Purge(t.Context())
	if h := a.Stats().History; h != 0 {
		t.Fatalf("retention purge in the tenant database left %d rows", h)
	}

	if err := b.Delete("globex/orders"); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, b.storeOf("globex/x"), `SELECT COUNT(*) FROM messages`); got != 0 {
		t.Fatalf("delete left %d messages in the globex database", got)
	}
}

// legacyRoot writes data the way qsrv did before one database per tenant:
// everything in data-dir/qsrv.db.
func legacyRoot(t *testing.T, dir string) {
	t.Helper()
	st, err := openStore(filepath.Join(dir, "qsrv.db"), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	cfg := Config{VisibilityTimeout: 30 * time.Second, Ordering: OrderingAvailable}
	withDLQ := cfg
	withDLQ.DeadLetterQueue = "acme_1/dlq"
	msg := func(id string, seq uint64, body string) msgRecord {
		return msgRecord{ID: id, Seq: seq, Body: []byte(body), EnqueuedAt: now, State: stateReady}
	}
	st.appendAll([]op{
		{kind: opCreate, queue: "acme_1/dlq", cfg: cfg, at: now},
		{kind: opCreate, queue: "acme_1/orders", cfg: withDLQ, at: now},
		{kind: opCreate, queue: "acme-1/orders", cfg: cfg, at: now}, // '_' must not match '-' (no LIKE)
		{kind: opCreate, queue: "plain", cfg: cfg, at: now},
		{kind: opTopicSet, queue: "acme_1/events", subs: []string{"acme_1/orders"}, at: now},
		{kind: opInsert, queue: "acme_1/orders", msg: msg("m1", 1, "done")},
		{kind: opInsert, queue: "acme_1/orders", msg: msg("m2", 2, "live")},
		{kind: opInsert, queue: "acme-1/orders", msg: msg("m1", 1, "other")},
		{kind: opInsert, queue: "plain", msg: msg("m1", 1, "root")},
		{kind: opDeliver, queue: "acme_1/orders", id: "m1", lease: "l1", at: now.Add(time.Minute)},
		{kind: opAck, queue: "acme_1/orders", id: "m1", at: now},
	})
	st.sync()
	if err := st.close(); err != nil {
		t.Fatal(err)
	}
}

func checkMigrated(t *testing.T, b *Broker) {
	t.Helper()
	root := b.storeOf("")
	if n := countRows(t, root, `SELECT COUNT(*) FROM messages WHERE queue <> 'plain'`); n != 0 {
		t.Fatalf("root database still has %d tenant messages", n)
	}
	if n := countRows(t, root, `SELECT COUNT(*) FROM queues`) + countRows(t, root, `SELECT COUNT(*) FROM topics`); n != 1 {
		t.Fatalf("root database has %d queues+topics, want only %q", n, "plain")
	}
	if n := countRows(t, b.storeOf("acme_1/x"), `SELECT COUNT(*) FROM messages`); n != 2 {
		t.Fatalf("acme_1 database has %d messages, want 2", n)
	}
	if got := b.List(); !slices.Equal(got, []string{"acme-1/orders", "acme_1/dlq", "acme_1/orders", "plain"}) {
		t.Fatalf("queues %v", got)
	}
	o := mustGet(t, b, "acme_1/orders")
	if o.Config().DeadLetterQueue != "acme_1/dlq" || o.Stats().History != 1 {
		t.Fatalf("orders: dlq=%q history=%d", o.Config().DeadLetterQueue, o.Stats().History)
	}
	if top, err := b.GetTopic("acme_1/events"); err != nil || !slices.Equal(top.Queues, []string{"acme_1/orders"}) {
		t.Fatalf("topic: %+v %v", top, err)
	}
	for q, want := range map[string]string{"acme_1/orders": "live", "acme-1/orders": "other", "plain": "root"} {
		if got := string(recv(t, mustGet(t, b, q)).Message.Body); got != want {
			t.Fatalf("%s: got %q, want %q", q, got, want)
		}
	}
	// New messages continue the sequence, after the migrated ones.
	m, err := o.Send([]byte("new"), "")
	if err != nil {
		t.Fatal(err)
	}
	b.storeOf(o.name).sync()
	if seq := countRows(t, b.storeOf(o.name), `SELECT seq FROM messages WHERE queue = ? AND id = ?`, o.name, m.ID); seq != 3 {
		t.Fatalf("send after migration got seq %d, want 3", seq)
	}
}

func TestMigrateSingleDatabaseToTenants(t *testing.T) {
	dir := t.TempDir()
	legacyRoot(t, dir)
	b := openB(t, dir, nil)
	checkMigrated(t, b)
	b.Close()

	b = openB(t, dir, nil) // nothing left to migrate; state as left
	defer b.Close()
	if s := mustGet(t, b, "acme_1/orders").Stats(); s.Messages != 2 || s.InFlight != 1 || s.History != 1 {
		t.Fatalf("after second open: %+v", s) // "live" leased, "new" ready, "done" in history
	}
}

// A crash after a tenant's rows were copied but before they were deleted from
// the root database: the next start copies again without duplicating.
func TestMigrationResumesAfterCrash(t *testing.T) {
	dir := t.TempDir()
	legacyRoot(t, dir)

	root, err := openStore(filepath.Join(dir, "qsrv.db"), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "tenants", "acme_1"), 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := openStore(storePath(dir, "acme_1"), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	if n, err := copyTenant(st, filepath.Join(dir, "qsrv.db"), "acme_1"); err != nil || n != 2 {
		t.Fatalf("copy: %d %v", n, err)
	}
	st.close()
	root.close() // "crash": rows are now in both databases

	b := openB(t, dir, nil)
	defer b.Close()
	checkMigrated(t, b)
}
