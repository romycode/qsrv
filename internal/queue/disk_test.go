package queue

import (
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStorageGuard(t *testing.T) {
	var free atomic.Uint64
	free.Store(1 << 30)
	b, err := Open(t.TempDir(), Options{NoSync: true, JanitorEvery: time.Hour, MinFreeDisk: 100 << 20,
		FreeDisk: func(string) (uint64, error) { return free.Load(), nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	q, _ := b.Create("t/q", Config{})
	b.Create("t/other", Config{})
	b.CreateTopic("t/topic", []string{"t/q"})
	q.Send([]byte("before"), "")

	free.Store(10 << 20)
	b.diskCheck()
	if !b.StorageLow() {
		t.Fatal("guard not engaged")
	}
	if _, err := q.Send([]byte("x"), ""); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("send: %v", err)
	}
	if _, err := b.Publish("t/topic", []byte("x"), "", 0); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("publish: %v", err)
	}
	if _, err := b.Create("t/new", Config{}); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("create: %v", err)
	}
	if _, err := b.CreateTopic("t/new", nil); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := b.Replay("t/q", ReplayRequest{To: "t/other"}); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("replay: %v", err)
	}
	// Draining keeps working.
	d := recv(t, q)
	if err := q.Ack(d.LeaseID); err != nil {
		t.Fatalf("ack while low: %v", err)
	}
	if err := b.Delete("t/other"); err != nil {
		t.Fatalf("delete while low: %v", err)
	}

	free.Store(1 << 30)
	b.diskCheck()
	if _, err := q.Send([]byte("after"), ""); err != nil {
		t.Fatalf("send after recovery: %v", err)
	}
}

func TestStorageGuardAtStartup(t *testing.T) {
	b, err := Open(t.TempDir(), Options{NoSync: true, JanitorEvery: time.Hour, MinFreeDisk: 1,
		FreeDisk: func(string) (uint64, error) { return 0, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.Create("t/q", Config{}); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("create right after a start on a full disk: %v", err)
	}
}

func TestSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, nil)
	b.Create("t/q", Config{})
	if v := countRows(t, b.storeOf("t/q"), `PRAGMA user_version`); v != schemaVersion {
		t.Fatalf("user_version %d, want %d", v, schemaVersion)
	}
	b.Close()

	st, err := openStore(storePath(dir, "t"), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	st.w.Exec(`PRAGMA user_version = 99`)
	st.close()
	_, err = Open(dir, Options{OnFatal: func(err error) { panic(err) }})
	if err == nil || !strings.Contains(err.Error(), "newer than this qsrv supports") || !strings.Contains(err.Error(), filepath.Join("tenants", "t")) {
		t.Fatalf("open a database from a newer version: %v", err)
	}
}
