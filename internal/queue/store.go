package queue

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// msgState values are stored in SQLite: never renumber.
type msgState uint8

const (
	stateReady msgState = iota
	stateDelayed
	stateInFlight
	stateAcked        // processed; kept as history until retention
	stateDropped      // exhausted retries, no DLQ
	stateDeadLettered // exhausted retries, copied to the DLQ
)

// HistoryStates names the terminal states that can be replayed.
var HistoryStates = map[string]msgState{
	"acked":         stateAcked,
	"dropped":       stateDropped,
	"dead_lettered": stateDeadLettered,
}

const schema = `
CREATE TABLE IF NOT EXISTS queues (
	name       TEXT PRIMARY KEY,
	config     TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	pk           INTEGER PRIMARY KEY,
	queue        TEXT    NOT NULL,
	id           TEXT    NOT NULL,
	seq          INTEGER NOT NULL,
	group_id     TEXT    NOT NULL DEFAULT '',
	body         BLOB    NOT NULL,
	enqueued_at  INTEGER NOT NULL,
	attempts     INTEGER NOT NULL DEFAULT 0,
	state        INTEGER NOT NULL,
	available_at INTEGER NOT NULL DEFAULT 0,
	lease_id     TEXT    NOT NULL DEFAULT '',
	deadline     INTEGER NOT NULL DEFAULT 0,
	done_at      INTEGER NOT NULL DEFAULT 0,
	moved_to     TEXT    NOT NULL DEFAULT '',
	replay_of    TEXT    NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS messages_queue_id  ON messages(queue, id);
CREATE INDEX        IF NOT EXISTS messages_queue_seq ON messages(queue, seq);
CREATE INDEX        IF NOT EXISTS messages_history   ON messages(queue, state, done_at);
`

// msgRecord is the persisted view of a live message.
type msgRecord struct {
	ID          string
	GroupID     string
	Body        []byte
	EnqueuedAt  time.Time
	Seq         uint64
	Attempts    int
	State       msgState
	AvailableAt time.Time
	LeaseID     string
	Deadline    time.Time
	ReplayOf    string
}

type opKind uint8

const (
	opCreate opKind = iota + 1
	opDelete
	opInsert  // new message with body
	opDeliver // lease granted
	opAck
	opDrop   // exhausted, no DLQ
	opFail   // nack/expiry: ready or delayed (at)
	opExtend // lease deadline moved
	opMove   // dead-letter: copy row (with body) into dst, mark src dead_lettered
	opCopy   // replay: copy historical row as a new message into dst
)

// op is one state change, applied by the writer in append order.
type op struct {
	kind  opKind
	queue string    // queue the op is about (source for move/copy)
	id    string    // message id in queue
	lease string    // deliver
	at    time.Time // deadline / available_at / done_at / created_at
	cfg   Config    // create
	msg   msgRecord // insert: full row; move/copy: the new row in dst (no body)
	dst   string    // move/copy
}

// store persists ops through a single writer goroutine that commits every
// pending op in one transaction (group commit). Readers use a separate pool;
// SQLite WAL mode lets them run concurrently with the writer.
type store struct {
	w, r    *sql.DB
	onFatal func(error)

	mu        sync.Mutex
	work      *sync.Cond // writer: ops pending
	synced    *sync.Cond // waiters: commit progressed
	pending   []op
	appended  uint64
	committed uint64
	closing   bool
	done      chan struct{}

	extraLatency time.Duration // benchmarking only
	onCommit     func(time.Duration, int)
	batches      uint64 // commits done (metrics)
	batchedOps   uint64 // ops committed (metrics)
}

func openStore(path string, noSync bool, onFatal func(error)) (*store, error) {
	w, err := sql.Open(driverName, dsn(path, false, noSync))
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1) // SQLite has one writer anyway; avoids SQLITE_BUSY between our own conns
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	r, err := sql.Open(driverName, dsn(path, true, noSync))
	if err != nil {
		w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(8)

	s := &store{w: w, r: r, onFatal: onFatal, done: make(chan struct{})}
	s.work = sync.NewCond(&s.mu)
	s.synced = sync.NewCond(&s.mu)
	go s.loop()
	return s, nil
}

func (s *store) append(o op) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		s.onFatal(errors.New("store: append after close"))
	}
	s.pending = append(s.pending, o)
	s.appended++
	s.work.Signal()
	return s.appended
}

func (s *store) waitSync(pos uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.committed < pos {
		s.synced.Wait()
	}
}

// sync waits until every op appended so far is committed.
func (s *store) sync() {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos := s.appended
	for s.committed < pos {
		s.synced.Wait()
	}
}

func (s *store) loop() {
	defer close(s.done)
	for {
		s.mu.Lock()
		for len(s.pending) == 0 && !s.closing {
			s.work.Wait()
		}
		if len(s.pending) == 0 {
			s.mu.Unlock()
			return
		}
		batch, target := s.pending, s.appended
		s.pending = nil
		s.mu.Unlock()

		t0 := time.Now()
		err := s.commit(batch)
		if err == nil && s.extraLatency > 0 {
			time.Sleep(s.extraLatency)
		}
		if err != nil {
			// The in-memory state is now ahead of disk; the only safe move is
			// to crash and recover from the last committed state.
			s.onFatal(fmt.Errorf("store: commit failed: %w", err))
			return
		}

		if s.onCommit != nil {
			s.onCommit(time.Since(t0), len(batch))
		}
		s.mu.Lock()
		s.batches++
		s.batchedOps += uint64(len(batch))
		s.committed = target
		s.synced.Broadcast()
		s.mu.Unlock()
	}
}

func (s *store) commit(batch []op) error {
	tx, err := s.w.Begin()
	if err != nil {
		return err
	}
	for i := range batch {
		if err := apply(tx, &batch[i]); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func ns(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v)
}

func apply(tx *sql.Tx, o *op) error {
	var (
		res sql.Result
		err error
	)
	switch o.kind {
	case opCreate:
		var cfg []byte
		if cfg, err = json.Marshal(o.cfg); err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO queues(name, config, created_at) VALUES (?, ?, ?)`, o.queue, cfg, ns(o.at))
		return err
	case opDelete:
		if _, err = tx.Exec(`DELETE FROM queues WHERE name = ?`, o.queue); err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM messages WHERE queue = ?`, o.queue)
		return err
	case opInsert:
		m := &o.msg
		_, err = tx.Exec(`INSERT INTO messages(queue, id, seq, group_id, body, enqueued_at, state, replay_of)
			VALUES (?, ?, ?, ?, ?, ?, 0, ?)`, o.queue, m.ID, m.Seq, m.GroupID, m.Body, ns(m.EnqueuedAt), m.ReplayOf)
		return err
	case opDeliver:
		res, err = tx.Exec(`UPDATE messages SET attempts = attempts + 1, state = ?, lease_id = ?, deadline = ?
			WHERE queue = ? AND id = ?`, stateInFlight, o.lease, ns(o.at), o.queue, o.id)
	case opAck, opDrop:
		st := stateAcked
		if o.kind == opDrop {
			st = stateDropped
		}
		res, err = tx.Exec(`UPDATE messages SET state = ?, lease_id = '', deadline = 0, done_at = ?
			WHERE queue = ? AND id = ?`, st, ns(o.at), o.queue, o.id)
	case opFail:
		st := stateReady
		if !o.at.IsZero() {
			st = stateDelayed
		}
		res, err = tx.Exec(`UPDATE messages SET state = ?, available_at = ?, lease_id = '', deadline = 0
			WHERE queue = ? AND id = ?`, st, ns(o.at), o.queue, o.id)
	case opExtend:
		res, err = tx.Exec(`UPDATE messages SET deadline = ? WHERE queue = ? AND id = ?`, ns(o.at), o.queue, o.id)
	case opMove:
		m := &o.msg
		if res, err = tx.Exec(`INSERT INTO messages(queue, id, seq, group_id, body, enqueued_at, state, replay_of)
			SELECT ?, id, ?, group_id, body, ?, 0, replay_of FROM messages WHERE queue = ? AND id = ?`,
			o.dst, m.Seq, ns(m.EnqueuedAt), o.queue, o.id); err != nil {
			return err
		}
		if err = expectOne(res, o); err != nil {
			return err
		}
		res, err = tx.Exec(`UPDATE messages SET state = ?, lease_id = '', deadline = 0, done_at = ?, moved_to = ?
			WHERE queue = ? AND id = ?`, stateDeadLettered, ns(m.EnqueuedAt), o.dst, o.queue, o.id)
	case opCopy:
		m := &o.msg
		res, err = tx.Exec(`INSERT INTO messages(queue, id, seq, group_id, body, enqueued_at, state, replay_of)
			SELECT ?, ?, ?, group_id, body, ?, 0, id FROM messages WHERE queue = ? AND id = ?`,
			o.dst, m.ID, m.Seq, ns(m.EnqueuedAt), o.queue, o.id)
	default:
		return fmt.Errorf("unknown op %d", o.kind)
	}
	if err != nil {
		return err
	}
	return expectOne(res, o)
}

// expectOne logs (does not fail on) ops that matched no row. It would mean
// memory and disk diverged; failing the whole batch would lose good ops.
func expectOne(res sql.Result, o *op) error {
	if n, err := res.RowsAffected(); err == nil && n != 1 {
		slog.Error("store: op matched unexpected row count", "op", o.kind, "queue", o.queue, "id", o.id, "rows", n)
	}
	return nil
}

func (s *store) pendingLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// commitStats returns commits and ops committed so far (avg batch = ops/commits).
func (s *store) commitStats() (commits, ops uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batches, s.batchedOps
}

func (s *store) close() error {
	s.mu.Lock()
	s.closing = true
	s.work.Signal()
	s.mu.Unlock()
	<-s.done
	return errors.Join(s.r.Close(), s.w.Close())
}

// --- reads ---

type loadedQueue struct {
	name   string
	cfg    Config
	msgs   []*msgRecord // live messages, seq order
	maxSeq uint64       // over all rows, history included
}

func (s *store) load() ([]*loadedQueue, error) {
	rows, err := s.r.Query(`SELECT name, config FROM queues ORDER BY created_at, rowid`)
	if err != nil {
		return nil, err
	}
	var out []*loadedQueue
	byName := map[string]*loadedQueue{}
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		lq := &loadedQueue{name: name}
		if err := json.Unmarshal(raw, &lq.cfg); err != nil {
			rows.Close()
			return nil, fmt.Errorf("queue %s config: %w", name, err)
		}
		out = append(out, lq)
		byName[name] = lq
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.r.Query(`SELECT queue, MAX(seq) FROM messages GROUP BY queue`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var q string
		var mx uint64
		if err := rows.Scan(&q, &mx); err != nil {
			rows.Close()
			return nil, err
		}
		if lq := byName[q]; lq != nil {
			lq.maxSeq = mx
		}
	}
	rows.Close()

	rows, err = s.r.Query(`SELECT queue, id, seq, group_id, enqueued_at, attempts, state, available_at, lease_id, deadline, replay_of
		FROM messages WHERE state < ? ORDER BY queue, seq`, stateAcked)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			q                    string
			m                    msgRecord
			enq, avail, deadline int64
		)
		if err := rows.Scan(&q, &m.ID, &m.Seq, &m.GroupID, &enq, &m.Attempts, &m.State, &avail, &m.LeaseID, &deadline, &m.ReplayOf); err != nil {
			return nil, err
		}
		m.EnqueuedAt, m.AvailableAt, m.Deadline = fromNS(enq), fromNS(avail), fromNS(deadline)
		if lq := byName[q]; lq != nil {
			lq.msgs = append(lq.msgs, &m)
		}
	}
	return out, rows.Err()
}

func (s *store) body(queue, id string) ([]byte, error) {
	var b []byte
	err := s.r.QueryRow(`SELECT body FROM messages WHERE queue = ? AND id = ?`, queue, id).Scan(&b)
	return b, err
}

func (s *store) historyCount(queue string) int {
	var n int
	if err := s.r.QueryRow(`SELECT COUNT(*) FROM messages WHERE queue = ? AND state >= ?`, queue, stateAcked).Scan(&n); err != nil {
		slog.Warn("history count", "queue", queue, "err", err)
	}
	return n
}

type historyRow struct {
	ID      string
	GroupID string
	Seq     uint64
}

func (s *store) history(queue string, states []msgState, from, until time.Time, groupID string, after uint64, limit int) ([]historyRow, error) {
	var sb strings.Builder
	args := []any{queue, after}
	sb.WriteString(`SELECT id, group_id, seq FROM messages WHERE queue = ? AND seq > ? AND state IN (`)
	for i, st := range states {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
		args = append(args, st)
	}
	sb.WriteByte(')')
	if !from.IsZero() {
		sb.WriteString(` AND enqueued_at >= ?`)
		args = append(args, ns(from))
	}
	if !until.IsZero() {
		sb.WriteString(` AND enqueued_at < ?`)
		args = append(args, ns(until))
	}
	if groupID != "" {
		sb.WriteString(` AND group_id = ?`)
		args = append(args, groupID)
	}
	sb.WriteString(` ORDER BY seq LIMIT ?`)
	args = append(args, limit)

	rows, err := s.r.Query(sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []historyRow
	for rows.Next() {
		var h historyRow
		if err := rows.Scan(&h.ID, &h.GroupID, &h.Seq); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// purge deletes history older than cutoff in bounded chunks so the writer
// is never blocked for long. Runs on the writer pool (single connection),
// so it interleaves with group commits instead of contending with them.
func (s *store) purge(queue string, cutoff time.Time) (int64, error) {
	const chunk = 5000
	var total int64
	for {
		res, err := s.w.Exec(`DELETE FROM messages WHERE pk IN (
			SELECT pk FROM messages WHERE queue = ? AND state >= ? AND done_at > 0 AND done_at < ? LIMIT ?)`,
			queue, stateAcked, ns(cutoff), chunk)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
		if n < chunk {
			return total, nil
		}
	}
}
