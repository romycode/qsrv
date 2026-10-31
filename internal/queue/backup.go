package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// Snapshots. Every BackupEvery the broker copies every tenant's database
// into BackupDir/<UTC timestamp>/, in the data dir's layout, so restoring is
// copying a snapshot over an empty data dir (all tenants, or one tenant's
// file). Each copy is VACUUM INTO: a consistent, compacted copy taken from a
// read transaction, without blocking writers. A snapshot is written under a
// temporary name, fsynced, and renamed when complete: a directory without
// the prefix is always whole. The newest BackupKeep are kept.
//
// Snapshots taken at different moments: each tenant's copy is consistent on
// its own (tenants never reference each other), not across tenants.

const (
	snapshotTmpPrefix = ".tmp-"
	snapshotLayout    = "20060102T150405Z"
	manifestFile      = "manifest.json"
)

// BackupOptions configures periodic snapshots. Dir empty disables them.
type BackupOptions struct {
	Dir   string
	Every time.Duration // default 1h
	Keep  int           // snapshots to keep; default 24
}

// BackupStatus describes the last snapshots (for metrics).
type BackupStatus struct {
	Enabled     bool
	Every       time.Duration
	LastSuccess time.Time
	LastTook    time.Duration
	LastBytes   int64
	Failures    uint64
}

type backups struct {
	opts BackupOptions
	run  sync.Mutex // one snapshot at a time

	mu     sync.Mutex
	status BackupStatus
}

// Snapshot is a finished snapshot's manifest.
type Snapshot struct {
	Name          string    `json:"name"`
	TakenAt       time.Time `json:"taken_at"`
	SchemaVersion int       `json:"schema_version"`
	Tenants       []string  `json:"tenants"` // "" is the root database
	Bytes         int64     `json:"bytes"`
}

func (b *Broker) startBackups(o BackupOptions) error {
	if o.Dir == "" {
		return nil
	}
	if o.Every <= 0 {
		o.Every = time.Hour
	}
	if o.Keep <= 0 {
		o.Keep = 24
	}
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return fmt.Errorf("backup dir: %w", err)
	}
	if abs, err := filepath.Abs(o.Dir); err == nil {
		if data, err := filepath.Abs(b.dataDir); err == nil && (abs == data || strings.HasPrefix(abs, data+string(filepath.Separator))) {
			slog.Warn("backup dir is inside the data dir: snapshots share the disk they protect", "backup_dir", abs)
		}
	}
	// Leftovers of a snapshot interrupted by a crash.
	if entries, err := os.ReadDir(o.Dir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), snapshotTmpPrefix) {
				os.RemoveAll(filepath.Join(o.Dir, e.Name()))
			}
		}
	}
	b.backups = &backups{opts: o, status: BackupStatus{Enabled: true, Every: o.Every}}
	go func() {
		t := time.NewTicker(o.Every)
		defer t.Stop()
		for {
			select {
			case <-b.stop:
				return
			case <-t.C:
				if _, err := b.Snapshot(); err != nil && !errors.Is(err, ErrClosed) {
					slog.Error("snapshot failed", "err", err)
				}
			}
		}
	}()
	slog.Info("snapshots enabled", "dir", o.Dir, "every", o.Every, "keep", o.Keep)
	return nil
}

// BackupStatus reports the snapshot state (zero value when disabled).
func (b *Broker) BackupStatus() BackupStatus {
	if b.backups == nil {
		return BackupStatus{}
	}
	b.backups.mu.Lock()
	defer b.backups.mu.Unlock()
	return b.backups.status
}

// Snapshot takes a snapshot now. Requires snapshots to be enabled.
func (b *Broker) Snapshot() (Snapshot, error) {
	bk := b.backups
	if bk == nil {
		return Snapshot{}, errors.New("snapshots are not enabled")
	}
	bk.run.Lock()
	defer bk.run.Unlock()

	start := time.Now()
	snap, err := b.snapshot(bk.opts.Dir, b.clock.Now().UTC())
	bk.mu.Lock()
	if err != nil {
		bk.status.Failures++
	} else {
		bk.status.LastSuccess, bk.status.LastTook, bk.status.LastBytes = snap.TakenAt, time.Since(start), snap.Bytes
	}
	bk.mu.Unlock()
	if err != nil {
		return Snapshot{}, err
	}
	slog.Info("snapshot", "name", snap.Name, "tenants", len(snap.Tenants), "bytes", snap.Bytes, "took", time.Since(start).Round(time.Millisecond))
	if err := pruneSnapshots(bk.opts.Dir, bk.opts.Keep); err != nil {
		slog.Warn("prune snapshots", "err", err)
	}
	return snap, nil
}

func (b *Broker) snapshot(dir string, now time.Time) (Snapshot, error) {
	snap := Snapshot{Name: now.Format(snapshotLayout), TakenAt: now, SchemaVersion: schemaVersion}
	final := filepath.Join(dir, snap.Name)
	if _, err := os.Stat(final); err == nil {
		return Snapshot{}, fmt.Errorf("snapshot %s already exists", snap.Name)
	}
	tmp := filepath.Join(dir, snapshotTmpPrefix+snap.Name)
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(tmp)
		}
	}()

	b.storesMu.Lock()
	if b.closed {
		b.storesMu.Unlock()
		return Snapshot{}, ErrClosed
	}
	stores := make(map[string]*store, len(b.stores))
	for t, st := range b.stores {
		stores[t] = st
		snap.Tenants = append(snap.Tenants, t)
	}
	b.storesMu.Unlock()
	slices.Sort(snap.Tenants)

	for _, t := range snap.Tenants {
		rel, _ := filepath.Rel(b.dataDir, storePath(b.dataDir, t))
		out := filepath.Join(tmp, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return Snapshot{}, err
		}
		if _, err := stores[t].r.Exec(`VACUUM INTO ?`, out); err != nil {
			if b.isClosed() {
				return Snapshot{}, ErrClosed
			}
			return Snapshot{}, fmt.Errorf("tenant %q: %w", t, err)
		}
		n, err := syncFile(out)
		if err != nil {
			return Snapshot{}, fmt.Errorf("tenant %q: %w", t, err)
		}
		snap.Bytes += n
	}

	m, _ := json.MarshalIndent(snap, "", "  ")
	if err := os.WriteFile(filepath.Join(tmp, manifestFile), append(m, '\n'), 0o644); err != nil {
		return Snapshot{}, err
	}
	if _, err := syncFile(filepath.Join(tmp, manifestFile)); err != nil {
		return Snapshot{}, err
	}
	if err := syncDirs(tmp); err != nil {
		return Snapshot{}, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return Snapshot{}, err
	}
	ok = true
	return snap, syncDir(dir)
}

func (b *Broker) isClosed() bool {
	b.storesMu.Lock()
	defer b.storesMu.Unlock()
	return b.closed
}

// Snapshots lists finished snapshots in dir, oldest first.
func Snapshots(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), snapshotTmpPrefix) {
			continue
		}
		if _, err := time.Parse(snapshotLayout, e.Name()); err != nil {
			continue // not ours
		}
		if _, err := os.Stat(filepath.Join(dir, e.Name(), manifestFile)); err == nil {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out, nil
}

func pruneSnapshots(dir string, keep int) error {
	names, err := Snapshots(dir)
	if err != nil {
		return err
	}
	var errs []error
	for len(names) > keep {
		errs = append(errs, os.RemoveAll(filepath.Join(dir, names[0])))
		names = names[1:]
	}
	return errors.Join(errs...)
}

func syncFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return 0, err
	}
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// syncDirs fsyncs root and every directory below it, so the new entries
// survive a crash before the rename makes the snapshot visible.
func syncDirs(root string) error {
	return filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		return syncDir(p)
	})
}
