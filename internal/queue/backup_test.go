package queue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSnapshots(t *testing.T) {
	dir, bdir := t.TempDir(), t.TempDir()
	clk := &fakeClock{t: time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC)}
	os.MkdirAll(filepath.Join(bdir, snapshotTmpPrefix+"20200101T000000Z"), 0o755) // crashed snapshot
	b, err := Open(dir, Options{Clock: clk, JanitorEvery: time.Hour, OnFatal: func(err error) { panic(err) },
		Backup: BackupOptions{Dir: bdir, Every: time.Hour, Keep: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := os.Stat(filepath.Join(bdir, snapshotTmpPrefix+"20200101T000000Z")); !os.IsNotExist(err) {
		t.Fatal("leftover temporary snapshot not removed")
	}
	a, _ := b.Create("acme/orders", Config{})
	g, _ := b.Create("globex/orders", Config{})
	a.Send([]byte("a1"), "")
	a.Ack(recv(t, a).LeaseID) // history is kept too
	a.Send([]byte("a2"), "")
	g.Send([]byte("g1"), "")

	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.Name != "20261031T180000Z" || !slices.Equal(snap.Tenants, []string{"", "acme", "globex"}) || snap.Bytes == 0 {
		t.Fatalf("snapshot %+v", snap)
	}
	var m Snapshot
	raw, err := os.ReadFile(filepath.Join(bdir, snap.Name, manifestFile))
	if err != nil || json.Unmarshal(raw, &m) != nil || m.SchemaVersion != schemaVersion {
		t.Fatalf("manifest %s: %v", raw, err)
	}
	if st := b.BackupStatus(); !st.LastSuccess.Equal(clk.Now()) || st.Failures != 0 || st.LastBytes != snap.Bytes {
		t.Fatalf("status %+v", st)
	}

	// Restoring is using the snapshot as a data dir.
	r, err := Open(filepath.Join(bdir, snap.Name), Options{Clock: clk, JanitorEvery: time.Hour, OnFatal: func(err error) { panic(err) }})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(recv(t, mustGet(t, r, "acme/orders")).Message.Body); got != "a2" {
		t.Fatalf("restored acme: %q", got)
	}
	if got := string(recv(t, mustGet(t, r, "globex/orders")).Message.Body); got != "g1" {
		t.Fatalf("restored globex: %q", got)
	}
	if h := mustGet(t, r, "acme/orders").Stats().History; h != 1 {
		t.Fatalf("restored history %d", h)
	}
	r.Close()

	// Rotation keeps the newest 2.
	for range 2 {
		clk.Advance(time.Hour)
		if _, err := b.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	names, _ := Snapshots(bdir)
	if !slices.Equal(names, []string{"20261031T190000Z", "20261031T200000Z"}) {
		t.Fatalf("kept %v", names)
	}
	if _, err := b.Snapshot(); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("same second twice: %v", err)
	}
	if st := b.BackupStatus(); st.Failures != 1 {
		t.Fatalf("failures %d", st.Failures)
	}
}

func TestSnapshotsDisabled(t *testing.T) {
	b := newTestBroker(t, nil)
	if _, err := b.Snapshot(); err == nil {
		t.Fatal("snapshot without a backup dir")
	}
	if b.BackupStatus().Enabled {
		t.Fatal("enabled")
	}
}
