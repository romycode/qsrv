package queue

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// Multitenancy. Queue and topic names are "tenant/name". The HTTP API scopes
// every request to the tenant of its API key, so tenants never see or reach
// each other's queues; the broker additionally refuses any reference across
// tenants (a dead letter queue, a topic subscription, a replay destination).
//
// The broker itself also accepts names without a tenant (used by tests and
// embedders); qsrv refuses to start while any exist (see Untenanted).

// TenantSep separates the tenant from the queue or topic name.
const TenantSep = "/"

var (
	validTenant = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,39}$`)
	validLocal  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,80}$`)
)

// ValidTenant reports whether t can be used as a tenant name: lowercase
// letters, digits, '-' and '_', starting with a letter or digit, up to 40.
func ValidTenant(t string) bool { return validTenant.MatchString(t) }

// ValidLocalName reports whether n is a valid queue or topic name inside a
// tenant: letters, digits, '.', '-' and '_', up to 80.
func ValidLocalName(n string) bool { return validLocal.MatchString(n) }

// SplitName splits "tenant/name" into its parts; tenant is "" without one.
func SplitName(full string) (tenant, name string) {
	if t, n, ok := strings.Cut(full, TenantSep); ok {
		return t, n
	}
	return "", full
}

// JoinName builds the full name of name in tenant.
func JoinName(tenant, name string) string {
	if tenant == "" {
		return name
	}
	return tenant + TenantSep + name
}

func validFullName(full string) bool {
	t, n := SplitName(full)
	return (t == "" && !strings.Contains(full, TenantSep) || ValidTenant(t)) && ValidLocalName(n)
}

func sameTenant(a, b string) bool {
	ta, _ := SplitName(a)
	tb, _ := SplitName(b)
	return ta == tb
}

func crossTenant(what, from, to string) error {
	return fmt.Errorf("%w: %s %q is in another tenant than %q", ErrInvalidConfig, what, to, from)
}

// Untenanted lists queues and topics whose name has no tenant.
func (b *Broker) Untenanted() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []string
	for n := range b.queues {
		if t, _ := SplitName(n); t == "" {
			out = append(out, "queue "+n)
		}
	}
	for n := range b.topics {
		if t, _ := SplitName(n); t == "" {
			out = append(out, "topic "+n)
		}
	}
	return out
}

// AssignTenant moves every queue and topic without a tenant (and their
// messages and history) into tenant, in one transaction. Run it with qsrv
// stopped. Returns how many queues and topics were renamed.
func AssignTenant(dataDir, tenant string) (queues, topics int, err error) {
	if !ValidTenant(tenant) {
		return 0, 0, fmt.Errorf("%w: invalid tenant %q", ErrInvalidConfig, tenant)
	}
	db, err := sql.Open(driverName, dsn(filepath.Join(dataDir, "qsrv.db"), false, false))
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	if _, err := db.Exec(schema); err != nil {
		return 0, 0, err
	}
	tx, err := db.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	prefix := tenant + TenantSep
	rename := func(n string) string {
		if n == "" || strings.Contains(n, TenantSep) {
			return n
		}
		return prefix + n
	}

	// Queues: name, and the DLQ inside the stored config.
	rows, err := tx.Query(`SELECT name, config FROM queues`)
	if err != nil {
		return 0, 0, err
	}
	type qrow struct {
		name string
		cfg  Config
	}
	var qs []qrow
	for rows.Next() {
		var r qrow
		var raw []byte
		if err := rows.Scan(&r.name, &raw); err != nil {
			rows.Close()
			return 0, 0, err
		}
		if err := json.Unmarshal(raw, &r.cfg); err != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("queue %s config: %w", r.name, err)
		}
		qs = append(qs, r)
	}
	rows.Close()
	for _, r := range qs {
		if strings.Contains(r.name, TenantSep) {
			if t, _ := SplitName(r.cfg.DeadLetterQueue); r.cfg.DeadLetterQueue != "" && t == "" {
				return 0, 0, fmt.Errorf("queue %q has dead letter queue %q without a tenant: can't assign it to %q across tenants", r.name, r.cfg.DeadLetterQueue, tenant)
			}
			continue
		}
		r.cfg.DeadLetterQueue = rename(r.cfg.DeadLetterQueue)
		cfg, _ := json.Marshal(r.cfg)
		if _, err := tx.Exec(`UPDATE queues SET name = ?, config = ? WHERE name = ?`, rename(r.name), cfg, r.name); err != nil {
			return 0, 0, err
		}
		queues++
	}
	if _, err := tx.Exec(`UPDATE messages SET queue = ? || queue WHERE instr(queue, ?) = 0`, prefix, TenantSep); err != nil {
		return 0, 0, err
	}
	if _, err := tx.Exec(`UPDATE messages SET moved_to = ? || moved_to WHERE moved_to <> '' AND instr(moved_to, ?) = 0`, prefix, TenantSep); err != nil {
		return 0, 0, err
	}

	// Topics: name and subscribed queues.
	rows, err = tx.Query(`SELECT name, queues FROM topics`)
	if err != nil {
		return 0, 0, err
	}
	subs := map[string][]string{}
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			rows.Close()
			return 0, 0, err
		}
		var qs []string
		if err := json.Unmarshal(raw, &qs); err != nil {
			rows.Close()
			return 0, 0, err
		}
		subs[name] = qs
	}
	rows.Close()
	for name, qs := range subs {
		if strings.Contains(name, TenantSep) {
			continue
		}
		for i := range qs {
			qs[i] = rename(qs[i])
		}
		raw, _ := json.Marshal(qs)
		if _, err := tx.Exec(`UPDATE topics SET name = ?, queues = ? WHERE name = ?`, rename(name), raw, name); err != nil {
			return 0, 0, err
		}
		topics++
	}
	return queues, topics, tx.Commit()
}
