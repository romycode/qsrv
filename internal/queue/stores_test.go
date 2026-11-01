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

func TestRestoreTenant(t *testing.T) {
	dir, bdir := t.TempDir(), t.TempDir()
	clk := &fakeClock{t: time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC)}
	open := func() *Broker {
		b, err := Open(dir, Options{Clock: clk, JanitorEvery: time.Hour, OnFatal: func(err error) { panic(err) },
			Backup: BackupOptions{Dir: bdir, Every: time.Hour}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	b := open()
	a, _ := b.Create("acme/orders", Config{Persistent: true})
	b.Create("acme_1/orders", Config{}) // '_' must not match like LIKE would
	b.CreateTopic("acme/events", []string{"acme/orders"})
	g, _ := b.Create("globex/orders", Config{})
	a.Send([]byte("a-before"), "")
	a.Ack(recv(t, a).LeaseID) // history
	a.Send([]byte("a-live"), "")
	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snap.Tenants, []string{"acme", "acme_1", "globex"}) {
		t.Fatalf("manifest tenants %v", snap.Tenants)
	}

	// After the snapshot: acme loses data, gains a queue; globex moves on.
	recv(t, a)
	a.Send([]byte("a-after"), "")
	b.Create("acme/new", Config{})
	b.DeleteTopic("acme/events")
	g.Send([]byte("g-after"), "")
	b.Close()

	if _, err := RestoreTenant(dir, filepath.Join(bdir, snap.Name), "acme"); err != nil {
		t.Fatal(err)
	}

	b = open()
	defer b.Close()
	if got := b.List(); !slices.Equal(got, []string{"acme/orders", "acme_1/orders", "globex/orders"}) {
		t.Fatalf("queues after restore %v", got)
	}
	a = mustGet(t, b, "acme/orders")
	if s := a.Stats(); s.Messages != 1 || s.History != 1 {
		t.Fatalf("acme/orders restored to %+v, want 1 live + 1 history", s)
	}
	if got := string(recv(t, a).Message.Body); got != "a-live" {
		t.Fatalf("acme message %q", got)
	}
	if _, err := b.GetTopic("acme/events"); err != nil {
		t.Fatalf("topic not restored: %v", err)
	}
	if got := string(recv(t, mustGet(t, b, "globex/orders")).Message.Body); got != "g-after" {
		t.Fatalf("globex was touched: %q", got)
	}
	// New sends continue after the restored sequence.
	if _, err := a.Send([]byte("next"), ""); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreTenantRejectsOtherSchemaVersions(t *testing.T) {
	dir, snap := t.TempDir(), t.TempDir()
	b := openB(t, dir, nil)
	b.Close()
	st, err := openStore(filepath.Join(snap, dbFile), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	st.w.Exec(`PRAGMA user_version = 99`)
	st.close()
	if _, err := RestoreTenant(dir, snap, "acme"); err == nil || !strings.Contains(err.Error(), "schema version 99") {
		t.Fatalf("restore from another schema version: %v", err)
	}
	if _, err := RestoreTenant(dir, filepath.Join(snap, "missing"), "acme"); err == nil || !os.IsNotExist(errorsUnwrapAll(err)) {
		t.Fatalf("missing snapshot: %v", err)
	}
}

func errorsUnwrapAll(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok || u.Unwrap() == nil {
			return err
		}
		err = u.Unwrap()
	}
}
