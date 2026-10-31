package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueExists   = errors.New("queue: already exists")
	ErrQueueNotFound = errors.New("queue: not found")
	ErrInvalidConfig = errors.New("queue: invalid config")
)

const (
	DefaultReplayLimit = 1000
	MaxReplayLimit     = 10000
)

// Broker owns all queues and the store: one SQLite database for every
// tenant, so concurrent commits of all tenants share one transaction and one
// fsync (group commit).
//
// Lock order: history -> persist -> b.mu -> q.mu -> store internals.
// Every op that mutates state and logs it holds persist.RLock throughout;
// Delete takes persist.Lock so no op on a queue can land after its delete.
type Broker struct {
	clock   Clock
	hooks   Hooks
	dataDir string

	store  *store
	closed atomic.Bool

	diskLow   atomic.Bool
	diskCheck func() // nil when the guard is off; tests call it
	backups   *backups

	history sync.RWMutex // replay (R) vs retention purge (W)
	persist sync.RWMutex

	mu     sync.RWMutex
	queues map[string]*Queue
	topics map[string][]string // topic -> subscribed queues, sorted

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

type Options struct {
	Clock        Clock
	JanitorEvery time.Duration // retention purge interval; default 1m
	// NoSync disables fsync (synchronous=OFF). Tests only: a crash loses data.
	NoSync  bool
	OnFatal func(error)
	Hooks   Hooks
	// SimulatedSyncLatency adds a sleep to every commit to emulate slower
	// disks (e.g. EBS) when benchmarking. Never set in production.
	SimulatedSyncLatency time.Duration
	// MinFreeDisk: below this many free bytes on the data dir's filesystem,
	// writes that add data fail with ErrStorageFull (see disk.go). 0 disables.
	MinFreeDisk uint64
	// FreeDisk overrides how free space is measured (tests).
	FreeDisk func(dir string) (uint64, error)
	// Backup enables periodic snapshots (see backup.go).
	Backup BackupOptions
}

// Open loads state from dataDir/qsrv.db and starts the broker.
func Open(dataDir string, o Options) (*Broker, error) {
	if o.Clock == nil {
		o.Clock = realClock{}
	}
	if o.JanitorEvery <= 0 {
		o.JanitorEvery = time.Minute
	}
	if o.OnFatal == nil {
		o.OnFatal = func(err error) {
			slog.Error("fatal storage error, exiting to protect data", "err", err)
			os.Exit(1)
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	start := time.Now()
	configureDriver(dataDir)
	st, err := openStore(filepath.Join(dataDir, dbFile), o.NoSync, o.OnFatal)
	if err != nil {
		return nil, err
	}
	st.extraLatency = o.SimulatedSyncLatency
	st.onCommit = o.Hooks.commit

	b := &Broker{
		clock: o.Clock, hooks: o.Hooks, dataDir: dataDir, store: st,
		queues: map[string]*Queue{}, stop: make(chan struct{}), done: make(chan struct{}),
	}
	fail := func(err error) (*Broker, error) {
		st.close()
		return nil, err
	}
	loaded, err := st.load()
	if err != nil {
		return fail(fmt.Errorf("load state: %w", err))
	}
	if b.topics, err = st.loadTopics(); err != nil {
		return fail(fmt.Errorf("load topics: %w", err))
	}
	total := 0
	for _, lq := range loaded {
		q := newQueue(lq.name, lq.cfg, b, st)
		q.restore(lq)
		b.queues[lq.name] = q
		total += len(lq.msgs)
	}
	slog.Info("recovered", "queues", len(b.queues), "topics", len(b.topics),
		"live_messages", total, "took", time.Since(start).Round(time.Millisecond))

	if err := b.startBackups(o.Backup); err != nil {
		return fail(err)
	}
	b.watchDisk(o.MinFreeDisk, o.FreeDisk)
	go b.janitor(o.JanitorEvery)
	return b, nil
}

func (b *Broker) Create(name string, cfg Config) (*Queue, error) {
	if !validFullName(name) {
		return nil, fmt.Errorf("%w: invalid name %q", ErrInvalidConfig, name)
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = 30 * time.Second
	}
	if cfg.Ordering == "" {
		cfg.Ordering = OrderingAvailable
	}
	switch {
	case cfg.Ordering != OrderingAvailable && cfg.Ordering != OrderingStrict:
		return nil, fmt.Errorf("%w: ordering must be %q or %q", ErrInvalidConfig, OrderingAvailable, OrderingStrict)
	case cfg.VisibilityTimeout < time.Second || cfg.VisibilityTimeout > 12*time.Hour:
		return nil, fmt.Errorf("%w: visibility_timeout must be between 1s and 12h", ErrInvalidConfig)
	case cfg.Retry.MaxAttempts < 0 || cfg.Retry.InitialBackoff < 0 || cfg.Retry.MaxBackoff < 0 || cfg.MaxMessages < 0 || cfg.Retention < 0:
		return nil, fmt.Errorf("%w: negative values", ErrInvalidConfig)
	case cfg.DeadLetterQueue == name:
		return nil, fmt.Errorf("%w: queue cannot be its own DLQ", ErrInvalidConfig)
	case cfg.DeadLetterQueue != "" && !sameTenant(name, cfg.DeadLetterQueue):
		return nil, crossTenant("dead letter queue", name, cfg.DeadLetterQueue)
	}
	if err := b.checkDisk(); err != nil {
		return nil, err
	}
	st := b.store

	b.persist.RLock()
	b.mu.Lock()
	if _, ok := b.queues[name]; ok {
		b.mu.Unlock()
		b.persist.RUnlock()
		return nil, ErrQueueExists
	}
	if cfg.DeadLetterQueue != "" {
		if _, ok := b.queues[cfg.DeadLetterQueue]; !ok {
			b.mu.Unlock()
			b.persist.RUnlock()
			return nil, fmt.Errorf("%w: dead letter queue %q does not exist", ErrInvalidConfig, cfg.DeadLetterQueue)
		}
	}
	pos := st.append(op{kind: opCreate, queue: name, cfg: cfg, at: b.clock.Now()})
	q := newQueue(name, cfg, b, st)
	b.queues[name] = q
	b.mu.Unlock()
	b.persist.RUnlock()

	st.waitSync(pos)
	return q, nil
}

func (b *Broker) Get(name string) (*Queue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

// Delete removes a queue, its live messages and its history. Refused while
// another queue uses it as DLQ or it is subscribed to a topic, so dead
// letters and published messages are never silently dropped.
func (b *Broker) Delete(name string) error {
	b.persist.Lock()
	b.mu.Lock()
	q, ok := b.queues[name]
	if !ok {
		b.mu.Unlock()
		b.persist.Unlock()
		return ErrQueueNotFound
	}
	for other, oq := range b.queues {
		if oq.cfg.DeadLetterQueue == name {
			b.mu.Unlock()
			b.persist.Unlock()
			return fmt.Errorf("%w: %q is the dead letter queue of %q", ErrInvalidConfig, name, other)
		}
	}
	if topics := b.topicsOfLocked(name); len(topics) > 0 {
		b.mu.Unlock()
		b.persist.Unlock()
		return fmt.Errorf("%w: %q is subscribed to topic(s) %v; unsubscribe it first", ErrInvalidConfig, name, topics)
	}
	delete(b.queues, name)
	q.close()
	pos := q.st.append(op{kind: opDelete, queue: name})
	b.mu.Unlock()
	b.persist.Unlock()

	q.st.waitSync(pos)
	b.hooks.deleted(name)
	return nil
}

// Event is a queue lifecycle event for observability.
type Event uint8

const (
	EvSent         Event = iota // accepted from a producer
	EvDeadLetterIn              // arrived from another queue's DLQ policy
	EvReplayIn                  // arrived via replay
	EvDelivered                 // leased to a consumer
	EvRedelivered               // leased again (attempts > 1)
	EvAcked
	EvNacked
	EvExpired      // lease timed out
	EvDeadLettered // retries exhausted, moved to DLQ
	EvDropped      // retries exhausted, no DLQ
)

// Hooks receive observability callbacks. All are optional and must be cheap
// and non-blocking: they run on hot paths (Event, Commit outside locks).
type Hooks struct {
	OnEvent        func(queue string, ev Event, n int)
	OnCommit       func(d time.Duration, ops int)
	OnQueueDeleted func(queue string)
}

func (h Hooks) Event(q string, ev Event, n int) {
	if h.OnEvent != nil {
		h.OnEvent(q, ev, n)
	}
}

func (h Hooks) commit(d time.Duration, ops int) {
	if h.OnCommit != nil {
		h.OnCommit(d, ops)
	}
}

func (h Hooks) deleted(q string) {
	if h.OnQueueDeleted != nil {
		h.OnQueueDeleted(q)
	}
}

// Now is the broker's clock (timestamps in Live are on this clock).
func (b *Broker) Now() time.Time { return b.clock.Now() }

// Queues returns live queues (for metrics).
func (b *Broker) Queues() []*Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		out = append(out, q)
	}
	return out
}

// PendingWrites is the number of ops waiting for the writer.
func (b *Broker) PendingWrites() int { return b.store.pendingLen() }

// CommitStats reports group-commit efficiency: transactions and ops committed.
func (b *Broker) CommitStats() (commits, ops uint64) { return b.store.commitStats() }

func (b *Broker) List() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.queues))
	for n := range b.queues {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// ReplayRequest selects processed messages of a source queue to re-enqueue.
type ReplayRequest struct {
	To      string    // destination queue (may equal the source)
	States  []string  // subset of HistoryStates keys; empty = all
	From    time.Time // enqueued_at >= From (zero = no bound)
	Until   time.Time // enqueued_at <  Until (zero = no bound)
	GroupID string    // only this group (empty = all)
	After   uint64    // cursor: source seq to continue after
	Limit   int       // default DefaultReplayLimit, max MaxReplayLimit
}

type ReplayResult struct {
	Replayed int
	Next     uint64 // cursor for the next call; 0 = done
}

// Replay copies history rows of src into req.To as new messages, in original
// order. Copies keep body and group_id, get a new id, and carry ReplayOf.
// Each call is atomic: either the whole page is enqueued or none of it.
func (b *Broker) Replay(src string, req ReplayRequest) (ReplayResult, error) {
	if req.Limit <= 0 {
		req.Limit = DefaultReplayLimit
	}
	if req.Limit > MaxReplayLimit {
		return ReplayResult{}, fmt.Errorf("%w: limit max is %d", ErrInvalidConfig, MaxReplayLimit)
	}
	if req.To == "" {
		return ReplayResult{}, fmt.Errorf("%w: destination queue (to) is required", ErrInvalidConfig)
	}
	states := make([]msgState, 0, len(HistoryStates))
	if len(req.States) == 0 {
		states = append(states, stateAcked, stateDropped, stateDeadLettered)
	}
	for _, s := range req.States {
		st, ok := HistoryStates[s]
		if !ok {
			return ReplayResult{}, fmt.Errorf("%w: unknown state %q (acked, dropped, dead_lettered)", ErrInvalidConfig, s)
		}
		states = append(states, st)
	}
	if !sameTenant(src, req.To) {
		return ReplayResult{}, crossTenant("replay destination", src, req.To)
	}
	if err := b.checkDisk(); err != nil {
		return ReplayResult{}, err
	}
	srcQ, err := b.Get(src)
	if err != nil {
		return ReplayResult{}, err
	}
	dst, err := b.Get(req.To)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("destination: %w", err)
	}

	b.history.RLock() // rows can't be purged between read and copy
	defer b.history.RUnlock()

	rows, err := srcQ.st.history(src, states, req.From, req.Until, req.GroupID, req.After, req.Limit)
	if err != nil {
		return ReplayResult{}, err
	}
	if len(rows) == 0 {
		return ReplayResult{}, nil
	}

	b.persist.RLock()
	n, err := dst.replayInsert(src, rows)
	b.persist.RUnlock()
	if err != nil {
		return ReplayResult{}, err
	}
	dst.st.sync()

	res := ReplayResult{Replayed: n}
	if len(rows) == req.Limit {
		res.Next = rows[len(rows)-1].Seq
	}
	slog.Info("replay", "from", src, "to", req.To, "messages", n, "next", res.Next)
	return res, nil
}

// Purge applies retention once, for every queue. Exposed for tests/ops.
func (b *Broker) Purge(ctx context.Context) {
	b.history.Lock()
	defer b.history.Unlock()
	b.mu.RLock()
	type job struct {
		name string
		ret  time.Duration
		st   *store
	}
	jobs := make([]job, 0, len(b.queues))
	for name, q := range b.queues {
		if q.cfg.Retention > 0 {
			jobs = append(jobs, job{name, q.cfg.Retention, q.st})
		}
	}
	b.mu.RUnlock()

	now := b.clock.Now()
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		n, err := j.st.purge(j.name, now.Add(-j.ret))
		if err != nil {
			slog.Error("retention purge", "queue", j.name, "err", err)
			continue
		}
		if n > 0 {
			slog.Info("retention purge", "queue", j.name, "deleted", n)
		}
	}
}

func (b *Broker) janitor(every time.Duration) {
	defer close(b.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-b.stop; cancel() }()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
			b.Purge(ctx)
		}
	}
}

// Close wakes long-pollers, stops the janitor and commits pending ops. Idempotent.
func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.mu.RLock()
		for _, q := range b.queues {
			q.close()
		}
		b.mu.RUnlock()
		close(b.stop)
		<-b.done
		b.persist.Lock() // wait for in-flight operations
		defer b.persist.Unlock()
		b.closed.Store(true)
		b.closeErr = b.store.close()
	})
	return b.closeErr
}
