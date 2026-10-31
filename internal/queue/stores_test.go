package queue

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// A database holds only its own tenant's names: anything else (a file copied
// into the wrong directory) is refused instead of loaded under the wrong tenant.
func TestRefusesNamesOfAnotherTenant(t *testing.T) {
	dir := t.TempDir()
	st, err := openStore(filepath.Join(dir, "qsrv.db"), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	st.waitSync(st.append(op{kind: opCreate, queue: "acme/orders", cfg: Config{VisibilityTimeout: time.Second}}))
	st.close()

	if _, err := Open(dir, Options{OnFatal: func(err error) { panic(err) }}); err == nil || !strings.Contains(err.Error(), `holds queue "acme/orders"`) {
		t.Fatalf("open: %v", err)
	}
}
