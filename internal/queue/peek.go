package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Peek: read messages without receiving them (no lease, no attempt, no
// effect on order), for inspection tools such as the admin UI. Reads come
// from the database on the reader pool, so they never hold queue locks.
// They first wait for pending writes, so every operation that returned
// before the peek is visible.

var ErrMessageNotFound = errors.New("queue: message not found")

const (
	DefaultPeekLimit = 50
	MaxPeekLimit     = 500
)

// StateNames maps every message state to its API name.
var StateNames = map[msgState]string{
	stateReady:        "ready",
	stateDelayed:      "delayed",
	stateInFlight:     "in_flight",
	stateAcked:        "acked",
	stateDropped:      "dropped",
	stateDeadLettered: "dead_lettered",
}

// LiveStates are the states of messages still in the queue. "waiting" is a
// message behind the head of its ordering group: stored as ready, it is
// delivered only after the messages before it in the group.
var LiveStates = []string{"ready", "waiting", "delayed", "in_flight"}

// MessageInfo describes a message as stored, without its body.
type MessageInfo struct {
	ID             string
	GroupID        string
	Seq            uint64
	State          string
	Attempts       int
	EnqueuedAt     time.Time
	AvailableAt    time.Time // delayed: when it becomes deliverable
	LeaseExpiresAt time.Time // in_flight
	DoneAt         time.Time // acked, dropped, dead_lettered
	MovedTo        string    // dead_lettered: the dead letter queue
	ReplayOf       string
	Size           int // body bytes
	Attributes     Attributes
}

// PeekRequest selects a page of messages. Live messages come oldest first;
// history (processed messages) newest first. Cursor is the Next of the
// previous page.
type PeekRequest struct {
	History bool
	States  []string // subset of the view's states; empty = all of them
	Cursor  uint64
	Limit   int // default DefaultPeekLimit, max MaxPeekLimit
}

type PeekPage struct {
	Messages []MessageInfo
	Next     uint64 // 0 = no more
}

// Peek lists messages of the queue without receiving them.
func (q *Queue) Peek(req PeekRequest) (PeekPage, error) {
	if req.Limit <= 0 {
		req.Limit = DefaultPeekLimit
	}
	if req.Limit > MaxPeekLimit {
		return PeekPage{}, fmt.Errorf("%w: limit max is %d", ErrInvalidConfig, MaxPeekLimit)
	}
	allowed := LiveStates
	if req.History {
		allowed = []string{"acked", "dropped", "dead_lettered"}
	}
	names := req.States
	if len(names) == 0 {
		names = allowed
	}
	want := map[string]bool{}
	var states []msgState
	for _, n := range names {
		if !contains(allowed, n) {
			return PeekPage{}, fmt.Errorf("%w: state %q is not one of %s", ErrInvalidConfig, n, strings.Join(allowed, ", "))
		}
		want[n] = true
		st, _ := stateByName(n)
		if n == "waiting" {
			st = stateReady // stored as ready; told apart below
		}
		if !slices.Contains(states, st) {
			states = append(states, st)
		}
	}
	q.st.sync()
	rows, err := q.st.peek(q.name, states, req.History, req.Cursor, req.Limit+1)
	if err != nil {
		return PeekPage{}, err
	}
	page := PeekPage{Messages: []MessageInfo{}}
	if len(rows) > req.Limit {
		rows = rows[:req.Limit]
		page.Next = rows[req.Limit-1].Seq // the cursor follows the rows read, filtered or not
	}
	if !req.History {
		q.markWaiting(rows)
	}
	for _, m := range rows {
		if want[m.State] {
			page.Messages = append(page.Messages, m)
		}
	}
	return page, nil
}

// markWaiting relabels ready messages that are not the head of their
// ordering group: the database doesn't know group order, memory does.
func (q *Queue) markWaiting(ms []MessageInfo) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range ms {
		if ms[i].State != "ready" {
			continue
		}
		m := q.msgs[ms[i].ID]
		if m == nil {
			continue
		}
		if key, ok := q.groupKey(m); ok {
			if g := q.groups[key]; g != nil && len(g.msgs) > 0 && g.msgs[0] != m {
				ms[i].State = "waiting"
			}
		}
	}
}

// PeekMessage returns one message, live or processed, with its body.
func (q *Queue) PeekMessage(id string) (MessageInfo, []byte, error) {
	q.st.sync()
	m, body, err := q.st.peekOne(q.name, id)
	if err == nil {
		one := []MessageInfo{m}
		q.markWaiting(one)
		m = one[0]
	}
	return m, body, err
}

func stateByName(n string) (msgState, bool) {
	for st, name := range StateNames {
		if name == n {
			return st, true
		}
	}
	return 0, false
}

func contains(xs []string, x string) bool { return slices.Contains(xs, x) }

const peekCols = `id, group_id, seq, state, attempts, enqueued_at, available_at, deadline, done_at, moved_to, replay_of, length(body), attributes`

// scanInfo scans the peekCols columns, then extra destinations.
func scanInfo(sc interface{ Scan(...any) error }, extra ...any) (MessageInfo, error) {
	var (
		m                            MessageInfo
		st                           msgState
		enq, avail, deadline, doneAt int64
		attrs                        string
	)
	dest := append([]any{&m.ID, &m.GroupID, &m.Seq, &st, &m.Attempts, &enq, &avail, &deadline, &doneAt, &m.MovedTo, &m.ReplayOf, &m.Size, &attrs}, extra...)
	if err := sc.Scan(dest...); err != nil {
		return m, err
	}
	a, err := decodeAttributes(attrs)
	if err != nil {
		return m, err
	}
	m.Attributes = a
	m.State = StateNames[st]
	m.EnqueuedAt, m.DoneAt = fromNS(enq), fromNS(doneAt)
	if st == stateDelayed {
		m.AvailableAt = fromNS(avail)
	}
	if st == stateInFlight {
		m.LeaseExpiresAt = fromNS(deadline)
	}
	return m, nil
}

func (s *store) peek(queue string, states []msgState, history bool, cursor uint64, limit int) ([]MessageInfo, error) {
	var sb strings.Builder
	args := []any{queue}
	sb.WriteString(`SELECT ` + peekCols + ` FROM messages WHERE queue = ? AND state IN (`)
	for i, st := range states {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
		args = append(args, st)
	}
	sb.WriteByte(')')
	switch {
	case history && cursor > 0:
		sb.WriteString(` AND seq < ? ORDER BY seq DESC`)
		args = append(args, cursor)
	case history:
		sb.WriteString(` ORDER BY seq DESC`)
	default:
		sb.WriteString(` AND seq > ? ORDER BY seq`)
		args = append(args, cursor)
	}
	sb.WriteString(` LIMIT ?`)
	args = append(args, limit)

	rows, err := s.r.Query(sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MessageInfo{}
	for rows.Next() {
		m, err := scanInfo(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *store) peekOne(queue, id string) (MessageInfo, []byte, error) {
	var body []byte
	m, err := scanInfo(s.r.QueryRow(`SELECT `+peekCols+`, body FROM messages WHERE queue = ? AND id = ?`, queue, id), &body)
	if errors.Is(err, sql.ErrNoRows) {
		return MessageInfo{}, nil, ErrMessageNotFound
	}
	if err != nil {
		return MessageInfo{}, nil, err
	}
	if body == nil {
		body = []byte{}
	}
	return m, body, nil
}
