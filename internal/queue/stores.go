package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// All tenants share one SQLite database, data-dir/qsrv.db: concurrent
// commits of every tenant go in one transaction with one fsync (group
// commit). Measured against one database per tenant (100 tenants, 10KB
// messages): ~1.6x lower latency, half the CPU, a quarter of the memory.
// Tenants are separated by the "tenant/" prefix of every name; a tenant can
// still be restored alone from a snapshot (RestoreTenant).

const dbFile = "qsrv.db"

// ErrClosed is returned by operations on a closed broker.
var ErrClosed = errors.New("queue: broker closed")

// tenantRange is the [lo, hi) range of names in tenant t: every name starts
// with "t/", and '0' is the byte after '/'. Uses the name indexes, and
// unlike LIKE needs no escaping of '_'.
func tenantRange(t string) (lo, hi string) { return t + TenantSep, t + "0" }

// RestoreResult counts what RestoreTenant put back.
type RestoreResult struct {
	Queues, Topics, Messages int64
}

// RestoreTenant replaces everything of tenant in dataDir (queues, topics,
// live messages and history) with its state in the snapshot at snapshotDir,
// in one transaction. Other tenants are untouched. Run it with qsrv
// stopped: the broker loads the database only on start.
//
// A queue of the tenant that doesn't exist in the snapshot is deleted, as it
// didn't exist at that moment.
func RestoreTenant(dataDir, snapshotDir, tenant string) (RestoreResult, error) {
	if !ValidTenant(tenant) {
		return RestoreResult{}, fmt.Errorf("%w: invalid tenant %q", ErrInvalidConfig, tenant)
	}
	src := filepath.Join(snapshotDir, dbFile)
	if _, err := os.Stat(src); err != nil {
		return RestoreResult{}, fmt.Errorf("snapshot: %w", err)
	}
	if err := checkSnapshotVersion(src); err != nil {
		return RestoreResult{}, err
	}
	path := filepath.Join(dataDir, dbFile)
	if _, err := os.Stat(path); err != nil {
		return RestoreResult{}, fmt.Errorf("data dir: %w", err)
	}
	db, err := sql.Open(driverName, dsn(path, false, false))
	if err != nil {
		return RestoreResult{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // ATTACH is per connection
	if err := initSchema(db); err != nil {
		return RestoreResult{}, err
	}
	if _, err := db.Exec(`ATTACH DATABASE ? AS snap`, src); err != nil {
		return RestoreResult{}, err
	}
	defer db.Exec(`DETACH DATABASE snap`)

	lo, hi := tenantRange(tenant)
	tx, err := db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`DELETE FROM main.messages WHERE queue >= ? AND queue < ?`,
		`DELETE FROM main.queues WHERE name >= ? AND name < ?`,
		`DELETE FROM main.topics WHERE name >= ? AND name < ?`,
	} {
		if _, err := tx.Exec(q, lo, hi); err != nil {
			return RestoreResult{}, err
		}
	}
	var r RestoreResult
	steps := []struct {
		n   *int64
		sql string
	}{
		{&r.Queues, `INSERT INTO main.queues(name, config, created_at)
			SELECT name, config, created_at FROM snap.queues WHERE name >= ? AND name < ?`},
		{&r.Topics, `INSERT INTO main.topics(name, queues, created_at)
			SELECT name, queues, created_at FROM snap.topics WHERE name >= ? AND name < ?`},
		{&r.Messages, `INSERT INTO main.messages(queue, id, seq, group_id, body, enqueued_at, attempts,
				state, available_at, lease_id, deadline, done_at, moved_to, replay_of, attributes)
			SELECT queue, id, seq, group_id, body, enqueued_at, attempts,
				state, available_at, lease_id, deadline, done_at, moved_to, replay_of, attributes
			FROM snap.messages WHERE queue >= ? AND queue < ? ORDER BY pk`},
	}
	for _, st := range steps {
		res, err := tx.Exec(st.sql, lo, hi)
		if err != nil {
			return RestoreResult{}, err
		}
		*st.n, _ = res.RowsAffected()
	}
	return r, tx.Commit()
}

func checkSnapshotVersion(path string) error {
	db, err := sql.Open(driverName, dsn(path, true, false))
	if err != nil {
		return err
	}
	defer db.Close()
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if v != schemaVersion {
		return fmt.Errorf("snapshot has schema version %d, this qsrv uses %d: restore it with the qsrv version that took it", v, schemaVersion)
	}
	return nil
}
