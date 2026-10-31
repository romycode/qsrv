package queue

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// One SQLite database per tenant. Each has its own writer goroutine (group
// commit), WAL and fsyncs, so a busy tenant doesn't queue behind another's
// commits, and a tenant can be backed up, moved or deleted as one file.
// Every reference between queues (dead letter queues, topics, replay) stays
// within a tenant, so every op lands in exactly one database.
//
// Layout of the data dir:
//
//	qsrv.db                  names without a tenant (tests, embedders)
//	tenants/<tenant>/qsrv.db one per tenant, created with its first queue or topic

const (
	dbFile     = "qsrv.db"
	tenantsDir = "tenants"
)

// ErrClosed is returned by operations on a closed broker.
var ErrClosed = errors.New("queue: broker closed")

// StoreFile is a tenant's database file ("" is the root database).
type StoreFile struct {
	Tenant string
	Path   string
}

func storePath(dataDir, tenant string) string {
	if tenant == "" {
		return filepath.Join(dataDir, dbFile)
	}
	return filepath.Join(dataDir, tenantsDir, tenant, dbFile)
}

// tenantDirs lists the tenants that have a database under dataDir.
func tenantDirs(dataDir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(dataDir, tenantsDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if !ValidTenant(e.Name()) {
			slog.Warn("ignoring directory that is not a tenant name", "dir", filepath.Join(dataDir, tenantsDir, e.Name()))
			continue
		}
		if _, err := os.Stat(storePath(dataDir, e.Name())); err == nil {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func (b *Broker) openStore(tenant string) (*store, error) {
	path := storePath(b.dataDir, tenant)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	st, err := openStore(path, b.opts.NoSync, b.opts.OnFatal)
	if err != nil {
		return nil, fmt.Errorf("open database of tenant %q: %w", tenant, err)
	}
	st.extraLatency = b.opts.SimulatedSyncLatency
	st.onCommit = b.opts.Hooks.commit
	return st, nil
}

// storeFor returns the store of name's tenant, opening (and creating) it on
// first use. Must not be called with persist or b.mu held: opening a
// database takes milliseconds and only blocks other first uses.
func (b *Broker) storeFor(name string) (*store, error) {
	tenant, _ := SplitName(name)
	b.storesMu.Lock()
	defer b.storesMu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if st := b.stores[tenant]; st != nil {
		return st, nil
	}
	st, err := b.openStore(tenant)
	if err != nil {
		return nil, err
	}
	b.stores[tenant] = st
	slog.Info("opened tenant database", "tenant", tenant, "path", storePath(b.dataDir, tenant))
	return st, nil
}

// storeOf returns the store of an existing queue or topic's tenant.
func (b *Broker) storeOf(name string) *store {
	tenant, _ := SplitName(name)
	b.storesMu.Lock()
	defer b.storesMu.Unlock()
	return b.stores[tenant]
}

func (b *Broker) allStores() []*store {
	b.storesMu.Lock()
	defer b.storesMu.Unlock()
	out := make([]*store, 0, len(b.stores))
	for _, st := range b.stores {
		out = append(out, st)
	}
	return out
}

// StoreFiles lists the database file of every open tenant, sorted.
func (b *Broker) StoreFiles() []StoreFile {
	b.storesMu.Lock()
	out := make([]StoreFile, 0, len(b.stores))
	for t := range b.stores {
		out = append(out, StoreFile{Tenant: t, Path: storePath(b.dataDir, t)})
	}
	b.storesMu.Unlock()
	slices.SortFunc(out, func(x, y StoreFile) int { return strings.Compare(x.Tenant, y.Tenant) })
	return out
}
