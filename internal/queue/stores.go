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
//	qsrv.db                  names without a tenant (tests, embedders, and
//	                         data from before tenants; see migrateTenants)
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

// tenantRange is the [lo, hi) range of names in tenant t: every name starts
// with "t/", and '0' is the byte after '/'. Uses the name indexes, and
// unlike LIKE needs no escaping of '_'.
func tenantRange(t string) (lo, hi string) { return t + TenantSep, t + "0" }

// migrateTenants moves the queues, topics and messages of every tenant out of
// the root database into the tenant's own (data written before databases
// were split per tenant, or renamed by AssignTenant). For each tenant it
// first copies into the tenant database and commits, then deletes from the
// root: a crash in between leaves rows in both, and the next run copies them
// again with INSERT OR IGNORE (same keys) and deletes them. Runs before
// anything is loaded or served.
func migrateTenants(root *store, open func(tenant string) (*store, error), rootPath string) ([]string, error) {
	rows, err := root.r.Query(`SELECT name FROM queues WHERE instr(name, ?) > 0
		UNION SELECT name FROM topics WHERE instr(name, ?) > 0`, TenantSep, TenantSep)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var tenants []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		if t, _ := SplitName(name); !seen[t] {
			seen[t] = true
			tenants = append(tenants, t)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.Sort(tenants)

	for _, t := range tenants {
		if !ValidTenant(t) {
			return nil, fmt.Errorf("root database has names in invalid tenant %q", t)
		}
		st, err := open(t)
		if err != nil {
			return nil, err
		}
		n, err := copyTenant(st, rootPath, t)
		if err != nil {
			return nil, fmt.Errorf("migrate tenant %q: %w", t, err)
		}
		if err := deleteTenant(root, t); err != nil {
			return nil, fmt.Errorf("migrate tenant %q: clean root database: %w", t, err)
		}
		slog.Info("moved tenant to its own database", "tenant", t, "messages", n)
	}
	return tenants, nil
}

// copyTenant copies tenant t's rows from the root database (attached on the
// tenant store's single writer connection) in one transaction.
func copyTenant(st *store, rootPath, t string) (int64, error) {
	if _, err := st.w.Exec(`ATTACH DATABASE ? AS src`, rootPath); err != nil {
		return 0, err
	}
	defer st.w.Exec(`DETACH DATABASE src`)

	lo, hi := tenantRange(t)
	tx, err := st.w.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT OR IGNORE INTO main.queues(name, config, created_at)
		SELECT name, config, created_at FROM src.queues WHERE name >= ? AND name < ?`, lo, hi); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO main.topics(name, queues, created_at)
		SELECT name, queues, created_at FROM src.topics WHERE name >= ? AND name < ?`, lo, hi); err != nil {
		return 0, err
	}
	res, err := tx.Exec(`INSERT OR IGNORE INTO main.messages(queue, id, seq, group_id, body, enqueued_at, attempts,
			state, available_at, lease_id, deadline, done_at, moved_to, replay_of)
		SELECT queue, id, seq, group_id, body, enqueued_at, attempts,
			state, available_at, lease_id, deadline, done_at, moved_to, replay_of
		FROM src.messages WHERE queue >= ? AND queue < ? ORDER BY pk`, lo, hi)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, tx.Commit()
}

func deleteTenant(root *store, t string) error {
	lo, hi := tenantRange(t)
	tx, err := root.w.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`DELETE FROM messages WHERE queue >= ? AND queue < ?`,
		`DELETE FROM queues WHERE name >= ? AND name < ?`,
		`DELETE FROM topics WHERE name >= ? AND name < ?`,
	} {
		if _, err := tx.Exec(q, lo, hi); err != nil {
			return err
		}
	}
	return tx.Commit()
}
