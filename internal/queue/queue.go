package queue

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrEmpty          = errors.New("queue: no message available")
	ErrLeaseNotFound  = errors.New("queue: lease not found or expired")
	ErrQueueClosed    = errors.New("queue: closed")
	ErrQueueFull      = errors.New("queue: full")
	ErrInvalidMessage = errors.New("queue: invalid message")
)

const MaxGroupIDLen = 128

// MaxSendDelay caps how far in the future a send can schedule a message.
const MaxSendDelay = 30 * 24 * time.Hour

type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Ordering controls delivery order.
//
//   - available (default): FIFO among messages that are deliverable right now.
//     A message in flight or backing off does not block later ones. Messages
//     sent with a GroupID are strictly ordered within their group: the next
//     message of a group is only delivered after the previous one is acked
//     or dead-lettered. Groups are independent of each other.
//   - strict: the whole queue is one group. Total order, one in flight.
type Ordering string

const (
	OrderingAvailable Ordering = "available"
	OrderingStrict    Ordering = "strict"
)

type Config struct {
	VisibilityTimeout time.Duration
	Retry             RetryPolicy
	DeadLetterQueue   string // empty = exhausted messages are dropped (kept as history)
	Ordering          Ordering
	MaxMessages       int           // 0 = unlimited; counts live messages only
	Retention         time.Duration // how long processed messages are kept for replay; 0 = forever
}

// Message is the public view. Live messages keep only metadata in memory;
// the body lives in SQLite and is attached on delivery.
type Message struct {
	ID         string
	GroupID    string
	Body       []byte
	Attempts   int // deliveries so far
	EnqueuedAt time.Time
	ReplayOf   string // original message id when created by a replay

	seq         uint64
	done        bool // removed from the queue (acked/dropped/dead-lettered)
	state       msgState
	availableAt time.Time
	lease       *lease
	group       *group
}

func (m *Message) public() Message {
	return Message{ID: m.ID, GroupID: m.GroupID, Attempts: m.Attempts, EnqueuedAt: m.EnqueuedAt, ReplayOf: m.ReplayOf}
}

type Delivery struct {
	Message   Message
	LeaseID   string
	ExpiresAt time.Time
}

type Stats struct {
	Messages int `json:"messages"`
	Ready    int `json:"ready"`
	Delayed  int `json:"delayed"`
	InFlight int `json:"in_flight"`
	Groups   int `json:"groups"`
	History  int `json:"history"`
}

type lease struct {
	id       string
	msg      *Message
	deadline time.Time
	active   bool
}

// group holds every live message of one ordering key; msgs[0] is the head.
// Only the head is ever ready/delayed/in flight, so order is structural.
type group struct {
	key  string
	msgs []*Message
}

type Queue struct {
	name string
	cfg  Config
	b    *Broker

	mu       sync.Mutex
	ready    *heapOf[*Message] // by seq
	delayed  *heapOf[*Message] // by availableAt
	inflight map[string]*lease // by lease id
	expiries *heapOf[*lease]   // by deadline; lazy deletion of inactive leases
	msgs     map[string]*Message
	groups   map[string]*group
	seq      uint64
	wake     chan struct{}
	closed   bool

	// order holds live messages in seq (= enqueue) order with lazy deletion,
	// so the oldest live message is found in amortized O(1).
	order     []*Message
	orderHead int
}

func newQueue(name string, cfg Config, b *Broker) *Queue {
	return &Queue{
		name:     name,
		cfg:      cfg,
		b:        b,
		ready:    newHeap(func(a, b *Message) bool { return a.seq < b.seq }),
		delayed:  newHeap(func(a, b *Message) bool { return a.availableAt.Before(b.availableAt) }),
		inflight: make(map[string]*lease),
		expiries: newHeap(func(a, b *lease) bool { return a.deadline.Before(b.deadline) }),
		msgs:     make(map[string]*Message),
		groups:   make(map[string]*group),
		wake:     make(chan struct{}),
	}
}

func (q *Queue) Name() string   { return q.name }
func (q *Queue) Config() Config { return q.cfg }

func (q *Queue) Send(body []byte, groupID string) (Message, error) {
	return q.SendDelayed(body, groupID, 0)
}

// SendDelayed enqueues a message that becomes deliverable after delay. Its
// FIFO position is still its send order. In a group it also waits for the
// messages before it: it is delivered once it is the group head and due.
func (q *Queue) SendDelayed(body []byte, groupID string, delay time.Duration) (Message, error) {
	if err := validateSend(groupID, delay); err != nil {
		return Message{}, err
	}
	body = append([]byte(nil), body...)

	q.b.persist.RLock()
	q.mu.Lock()
	if err := q.acceptLocked(1); err != nil {
		q.mu.Unlock()
		q.b.persist.RUnlock()
		return Message{}, err
	}
	m, rec := q.newMessageLocked(body, groupID, delay, q.b.clock.Now())
	pos := q.b.store.append(op{kind: opInsert, queue: q.name, msg: rec})
	q.addLocked(m)
	out := m.public()
	q.mu.Unlock()
	q.b.persist.RUnlock()

	q.b.store.waitSync(pos) // 201 only after commit
	q.b.hooks.Event(q.name, EvSent, 1)
	out.Body = body
	return out, nil
}

func validateSend(groupID string, delay time.Duration) error {
	if len(groupID) > MaxGroupIDLen || delay < 0 || delay > MaxSendDelay {
		return ErrInvalidMessage
	}
	return nil
}

// acceptLocked reports whether n more messages fit.
func (q *Queue) acceptLocked(n int) error {
	if q.closed {
		return ErrQueueClosed
	}
	if q.cfg.MaxMessages > 0 && len(q.msgs)+n > q.cfg.MaxMessages {
		return ErrQueueFull
	}
	return nil
}

// newMessageLocked builds a new message and its insert record. The caller
// appends the record to the store and then calls addLocked.
func (q *Queue) newMessageLocked(body []byte, groupID string, delay time.Duration, now time.Time) (*Message, msgRecord) {
	q.seq++
	m := &Message{ID: newID(), GroupID: groupID, EnqueuedAt: now, seq: q.seq}
	rec := msgRecord{ID: m.ID, GroupID: groupID, Body: body, EnqueuedAt: now, Seq: m.seq}
	if delay > 0 {
		m.availableAt = now.Add(delay)
		rec.State, rec.AvailableAt = stateDelayed, m.availableAt
	}
	return m, rec
}

// Receive leases the next deliverable message. With wait>0 it long-polls.
func (q *Queue) Receive(ctx context.Context, wait time.Duration) (Delivery, error) {
	until := time.Now().Add(wait)
	for {
		q.b.persist.RLock()
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			q.b.persist.RUnlock()
			return Delivery{}, ErrQueueClosed
		}
		now := q.b.clock.Now()
		dead := q.promoteLocked(now)
		if q.ready.Len() > 0 {
			d := q.leaseLocked(heap.Pop(q.ready).(*Message), now)
			q.mu.Unlock()
			q.flushDead(dead)
			q.b.persist.RUnlock()
			// Durable before handing out. Ops commit in order, so this also
			// persists the ack that unblocked this message's group.
			q.b.store.sync()
			body, err := q.b.store.body(q.name, d.Message.ID)
			if err != nil {
				// Lease stays; it will expire and the message is retried.
				return Delivery{}, fmt.Errorf("load body %s/%s: %w", q.name, d.Message.ID, err)
			}
			d.Message.Body = body
			q.b.hooks.Event(q.name, EvDelivered, 1)
			if d.Message.Attempts > 1 {
				q.b.hooks.Event(q.name, EvRedelivered, 1)
			}
			return d, nil
		}
		wake := q.wake
		next, hasNext := q.nextDueLocked()
		q.mu.Unlock()
		q.flushDead(dead)
		q.b.persist.RUnlock()

		remaining := time.Until(until)
		if wait <= 0 || remaining <= 0 {
			return Delivery{}, ErrEmpty
		}
		sleep := remaining
		if hasNext {
			if d := next.Sub(now); d < sleep {
				sleep = max(d, time.Millisecond)
			}
		}
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return Delivery{}, ctx.Err()
		case <-wake:
		case <-t.C:
		}
		t.Stop()
	}
}

func (q *Queue) Ack(leaseID string) error {
	err := q.settle(leaseID, func(m *Message, now time.Time) bool {
		q.b.store.append(op{kind: opAck, queue: q.name, id: m.ID, at: now})
		q.finishLocked(m)
		return false
	})
	if err == nil {
		q.b.hooks.Event(q.name, EvAcked, 1)
	}
	return err
}

// Nack fails the delivery now; the retry policy decides backoff or dead-lettering.
func (q *Queue) Nack(leaseID string) error {
	err := q.settle(leaseID, q.failLocked)
	if err == nil {
		q.b.hooks.Event(q.name, EvNacked, 1)
	}
	return err
}

// Extend sets the lease deadline to now+d (heartbeat for long jobs).
func (q *Queue) Extend(leaseID string, d time.Duration) (time.Time, error) {
	if d <= 0 {
		d = q.cfg.VisibilityTimeout
	}
	q.b.persist.RLock()
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		q.b.persist.RUnlock()
		return time.Time{}, ErrQueueClosed
	}
	now := q.b.clock.Now()
	dead := q.promoteLocked(now)
	old, ok := q.inflight[leaseID]
	var deadline time.Time
	if ok {
		old.active = false // heap entries are immutable; replace instead of fix-up
		nl := &lease{id: old.id, msg: old.msg, deadline: now.Add(d), active: true}
		q.inflight[leaseID] = nl
		nl.msg.lease = nl
		heap.Push(q.expiries, nl)
		deadline = nl.deadline
		q.b.store.append(op{kind: opExtend, queue: q.name, id: nl.msg.ID, at: deadline})
	}
	q.mu.Unlock()
	q.flushDead(dead)
	q.b.persist.RUnlock()
	if !ok {
		return time.Time{}, ErrLeaseNotFound
	}
	q.b.store.sync()
	return deadline, nil
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	s := Stats{
		Messages: len(q.msgs),
		Ready:    q.ready.Len(),
		Delayed:  q.delayed.Len(),
		InFlight: len(q.inflight),
		Groups:   len(q.groups),
	}
	q.mu.Unlock()
	s.History = q.b.store.historyCount(q.name)
	return s
}

// Live is a cheap snapshot for metrics: no database access.
type Live struct {
	Stats
	OldestEnqueuedAt time.Time // zero if empty
}

func (q *Queue) Live() Live {
	q.mu.Lock()
	defer q.mu.Unlock()
	l := Live{Stats: Stats{
		Messages: len(q.msgs),
		Ready:    q.ready.Len(),
		Delayed:  q.delayed.Len(),
		InFlight: len(q.inflight),
		Groups:   len(q.groups),
	}}
	if m := q.oldestLocked(); m != nil {
		l.OldestEnqueuedAt = m.EnqueuedAt
	}
	return l
}

func (q *Queue) oldestLocked() *Message {
	for q.orderHead < len(q.order) && q.order[q.orderHead].done {
		q.order[q.orderHead] = nil
		q.orderHead++
	}
	if q.orderHead > 1024 && q.orderHead > len(q.order)/2 { // compact
		q.order = append(make([]*Message, 0, len(q.order)-q.orderHead), q.order[q.orderHead:]...)
		q.orderHead = 0
	}
	if q.orderHead < len(q.order) {
		return q.order[q.orderHead]
	}
	return nil
}

func (q *Queue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.wake)
	}
}

// ingest receives a dead-lettered message from src. Requires persist.RLock.
// The DLQ ignores MaxMessages: refusing would mean losing data.
func (q *Queue) ingest(src string, old *Message) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.seq++
	m := &Message{ID: old.ID, GroupID: old.GroupID, EnqueuedAt: q.b.clock.Now(), ReplayOf: old.ReplayOf, seq: q.seq}
	q.b.store.append(op{kind: opMove, queue: src, id: old.ID, dst: q.name, msg: msgRecord{Seq: m.seq, EnqueuedAt: m.EnqueuedAt}})
	q.addLocked(m)
	q.b.hooks.Event(q.name, EvDeadLetterIn, 1)
	return true
}

// replayInsert enqueues copies of historical rows from src. Requires persist.RLock.
func (q *Queue) replayInsert(src string, rows []historyRow) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrQueueClosed
	}
	if q.cfg.MaxMessages > 0 && len(q.msgs)+len(rows) > q.cfg.MaxMessages {
		return 0, ErrQueueFull
	}
	now := q.b.clock.Now()
	for _, r := range rows {
		q.seq++
		m := &Message{ID: newID(), GroupID: r.GroupID, EnqueuedAt: now, ReplayOf: r.ID, seq: q.seq}
		q.b.store.append(op{kind: opCopy, queue: src, id: r.ID, dst: q.name, msg: msgRecord{ID: m.ID, Seq: m.seq, EnqueuedAt: now}})
		q.addLocked(m)
	}
	q.b.hooks.Event(q.name, EvReplayIn, len(rows))
	return len(rows), nil
}

// --- internals: *Locked funcs require q.mu (and persist.RLock when they log) ---

func (q *Queue) settle(leaseID string, fn func(*Message, time.Time) (dead bool)) error {
	if err := q.settleLocked(leaseID, fn); err != nil {
		return err
	}
	q.b.store.sync() // 204 on ack means it will not come back after a crash
	return nil
}

func (q *Queue) settleLocked(leaseID string, fn func(*Message, time.Time) (dead bool)) error {
	q.b.persist.RLock()
	defer q.b.persist.RUnlock()
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	now := q.b.clock.Now()
	dead := q.promoteLocked(now) // expire first: an ack after the deadline must fail
	l, ok := q.inflight[leaseID]
	if ok {
		delete(q.inflight, leaseID)
		l.active = false
		l.msg.lease = nil
		if fn(l.msg, now) {
			dead = append(dead, l.msg)
		}
	}
	q.mu.Unlock()
	q.flushDead(dead)
	if !ok {
		return ErrLeaseNotFound
	}
	return nil
}

func (q *Queue) groupKey(m *Message) (string, bool) {
	if q.cfg.Ordering == OrderingStrict {
		return "", true
	}
	return m.GroupID, m.GroupID != ""
}

// attachLocked puts m in its group; reports whether m is deliverable now.
func (q *Queue) attachLocked(m *Message) bool {
	key, grouped := q.groupKey(m)
	if !grouped {
		return true
	}
	g := q.groups[key]
	if g == nil {
		g = &group{key: key}
		q.groups[key] = g
	}
	g.msgs = append(g.msgs, m)
	m.group = g
	return len(g.msgs) == 1
}

// addLocked registers a new message and makes it deliverable if it may be.
func (q *Queue) addLocked(m *Message) {
	q.msgs[m.ID] = m
	q.order = append(q.order, m)
	m.state = stateReady
	if q.attachLocked(m) {
		q.scheduleLocked(m, q.b.clock.Now())
	}
}

// scheduleLocked makes a message that may be delivered (not blocked by its
// group) ready, or delayed if it has a future availableAt.
func (q *Queue) scheduleLocked(m *Message, now time.Time) {
	if m.availableAt.After(now) {
		m.state = stateDelayed
		heap.Push(q.delayed, m)
		q.signalLocked() // long-pollers re-arm their timer for the new due time
		return
	}
	m.state, m.availableAt = stateReady, time.Time{}
	heap.Push(q.ready, m)
	q.signalLocked()
}

// finishLocked removes a message for good and unblocks its group.
func (q *Queue) finishLocked(m *Message) {
	delete(q.msgs, m.ID)
	m.done = true
	g := m.group
	if g == nil {
		return
	}
	m.group = nil
	if len(g.msgs) == 0 || g.msgs[0] != m {
		slog.Error("queue: group invariant violated", "queue", q.name, "group", g.key, "id", m.ID)
		return
	}
	g.msgs[0] = nil
	g.msgs = g.msgs[1:]
	if len(g.msgs) == 0 {
		delete(q.groups, g.key)
		return
	}
	q.scheduleLocked(g.msgs[0], q.b.clock.Now())
}

// signalLocked wakes every long-poller (close-and-replace broadcast).
func (q *Queue) signalLocked() {
	if q.closed {
		return
	}
	close(q.wake)
	q.wake = make(chan struct{})
}

func (q *Queue) leaseLocked(m *Message, now time.Time) Delivery {
	m.Attempts++
	m.state = stateInFlight
	l := &lease{id: newID(), msg: m, deadline: now.Add(q.cfg.VisibilityTimeout), active: true}
	m.lease = l
	q.inflight[l.id] = l
	heap.Push(q.expiries, l)
	q.b.store.append(op{kind: opDeliver, queue: q.name, id: m.ID, lease: l.id, at: l.deadline})
	return Delivery{Message: m.public(), LeaseID: l.id, ExpiresAt: l.deadline}
}

// failLocked applies the retry policy to a message that just left flight.
// Returns true if it must be dead-lettered (already detached from the queue).
func (q *Queue) failLocked(m *Message, now time.Time) bool {
	if q.cfg.Retry.Exhausted(m.Attempts) {
		q.finishLocked(m)
		return true
	}
	if d := q.cfg.Retry.Delay(m.Attempts); d > 0 {
		m.state, m.availableAt = stateDelayed, now.Add(d)
		heap.Push(q.delayed, m)
		q.b.store.append(op{kind: opFail, queue: q.name, id: m.ID, at: m.availableAt})
		return false
	}
	m.state, m.availableAt = stateReady, time.Time{}
	heap.Push(q.ready, m) // original seq => original FIFO position
	q.b.store.append(op{kind: opFail, queue: q.name, id: m.ID})
	q.signalLocked()
	return false
}

// promoteLocked expires leases and releases backed-off messages whose time has come.
func (q *Queue) promoteLocked(now time.Time) []*Message {
	var dead []*Message
	for q.expiries.Len() > 0 {
		l := q.expiries.s[0]
		if l.active && l.deadline.After(now) {
			break
		}
		heap.Pop(q.expiries)
		if !l.active {
			continue
		}
		l.active = false
		l.msg.lease = nil
		delete(q.inflight, l.id)
		q.b.hooks.Event(q.name, EvExpired, 1)
		if q.failLocked(l.msg, now) {
			dead = append(dead, l.msg)
		}
	}
	moved := false
	for q.delayed.Len() > 0 && !q.delayed.s[0].availableAt.After(now) {
		m := heap.Pop(q.delayed).(*Message)
		m.state, m.availableAt = stateReady, time.Time{}
		heap.Push(q.ready, m)
		moved = true
	}
	if moved {
		q.signalLocked()
	}
	return dead
}

func (q *Queue) nextDueLocked() (time.Time, bool) {
	var next time.Time
	found := false
	if q.delayed.Len() > 0 {
		next, found = q.delayed.s[0].availableAt, true
	}
	for q.expiries.Len() > 0 && !q.expiries.s[0].active {
		heap.Pop(q.expiries)
	}
	if q.expiries.Len() > 0 {
		if d := q.expiries.s[0].deadline; !found || d.Before(next) {
			next, found = d, true
		}
	}
	return next, found
}

// flushDead runs WITHOUT q.mu (it locks the DLQ; two queue locks at once
// would deadlock on DLQ cycles) but WITH persist.RLock.
func (q *Queue) flushDead(msgs []*Message) {
	for _, m := range msgs {
		if dlq := q.cfg.DeadLetterQueue; dlq != "" {
			if dst, err := q.b.Get(dlq); err == nil && dst.ingest(q.name, m) {
				q.b.hooks.Event(q.name, EvDeadLettered, 1)
				continue
			}
		}
		q.b.store.append(op{kind: opDrop, queue: q.name, id: m.ID, at: q.b.clock.Now()})
		q.b.hooks.Event(q.name, EvDropped, 1)
		slog.Warn("message dropped after exhausting retries", "queue", q.name, "id", m.ID, "attempts", m.Attempts)
	}
}

// restore rebuilds live structures from loaded rows (seq order).
func (q *Queue) restore(lq *loadedQueue) {
	for _, r := range lq.msgs {
		m := &Message{ID: r.ID, GroupID: r.GroupID, Attempts: r.Attempts, EnqueuedAt: r.EnqueuedAt, ReplayOf: r.ReplayOf, seq: r.Seq, state: stateReady}
		if r.State == stateDelayed {
			m.availableAt = r.AvailableAt // kept for non-heads: applies once they become head
		}
		q.msgs[m.ID] = m
		q.order = append(q.order, m)
		if !q.attachLocked(m) {
			continue // non-head of its group: waits, whatever state was stored
		}
		switch {
		case r.State == stateDelayed:
			m.state, m.availableAt = stateDelayed, r.AvailableAt
			heap.Push(q.delayed, m)
		case r.State == stateInFlight && r.LeaseID != "":
			m.state = stateInFlight
			l := &lease{id: r.LeaseID, msg: m, deadline: r.Deadline, active: true}
			m.lease = l
			q.inflight[l.id] = l
			heap.Push(q.expiries, l)
		default:
			heap.Push(q.ready, m)
		}
	}
	q.seq = lq.maxSeq
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
