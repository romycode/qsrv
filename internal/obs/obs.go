// Package obs wires qsrv's Prometheus metrics: queue events, store commits,
// HTTP traffic, queue/disk gauges and process stats.
package obs

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/romycode/qsrv/internal/metrics"
	"github.com/romycode/qsrv/internal/queue"
)

var eventNames = map[queue.Event]string{
	queue.EvSent:         "sent",
	queue.EvDeadLetterIn: "dead_letter_in",
	queue.EvReplayIn:     "replay_in",
	queue.EvDelivered:    "delivered",
	queue.EvRedelivered:  "redelivered",
	queue.EvAcked:        "acked",
	queue.EvNacked:       "nacked",
	queue.EvExpired:      "expired",
	queue.EvDeadLettered: "dead_lettered",
	queue.EvDropped:      "dropped",
}

type Obs struct {
	Reg       *metrics.Registry
	events    *metrics.CounterVec
	commitDur *metrics.HistogramVec
	commitOps *metrics.HistogramVec
	httpReqs  *metrics.CounterVec
	httpDur   *metrics.HistogramVec
	start     time.Time
	scrapeMu  sync.Mutex // gauges share a per-scrape snapshot
}

func New() *Obs {
	r := metrics.NewRegistry()
	return &Obs{
		Reg:    r,
		start:  time.Now(),
		events: r.Counter("qsrv_messages_total", "Message lifecycle events per queue."),
		commitDur: r.Histogram("qsrv_store_commit_duration_seconds",
			"SQLite transaction commit time including fsync (one commit = one group of ops).",
			metrics.ExponentialBuckets(0.0001, 2, 16)), // 100µs .. ~3.3s
		commitOps: r.Histogram("qsrv_store_commit_batch_ops",
			"Operations per commit (group commit efficiency).",
			metrics.ExponentialBuckets(1, 2, 11)), // 1 .. 1024
		httpReqs: r.Counter("qsrv_http_requests_total", "HTTP requests by route, method and status code."),
		httpDur: r.Histogram("qsrv_http_request_duration_seconds",
			"HTTP request latency by route. Receive includes long-poll wait time.",
			metrics.ExponentialBuckets(0.0005, 2, 16)), // 0.5ms .. ~16s
	}
}

// Hooks returns the callbacks to pass to queue.Open.
func (o *Obs) Hooks() queue.Hooks {
	return queue.Hooks{
		OnEvent: func(q string, ev queue.Event, n int) {
			o.events.Add(uint64(n), metrics.L("queue", q), metrics.L("event", eventNames[ev]))
		},
		OnCommit: func(d time.Duration, ops int) {
			o.commitDur.With().Observe(d.Seconds())
			o.commitOps.With().Observe(float64(ops))
		},
		// Every event counter of a queue exists, at 0, from the start: rate()
		// and increase() need a sample before the first event, or the first
		// dead letters of a queue would never show up (nor alert).
		OnQueueCreated: func(q string) {
			for _, name := range eventNames {
				o.events.With(metrics.L("queue", q), metrics.L("event", name))
			}
		},
		OnQueueDeleted: func(q string) { o.Reg.Forget("queue", q) },
	}
}

// RegisterBroker adds scrape-time gauges. Call once the broker is open.
func (o *Obs) RegisterBroker(b *queue.Broker, dataDir, version string) {
	L := metrics.L
	o.Reg.Gauge("qsrv_build_info", "Build information.", func(emit func(float64, ...metrics.Label)) {
		emit(1, L("version", version), L("driver", queue.DriverInfo), L("go", runtime.Version()))
	})
	o.Reg.Gauge("qsrv_start_time_seconds", "Process start time (unix).", func(emit func(float64, ...metrics.Label)) {
		emit(float64(o.start.Unix()))
	})

	// One snapshot per scrape, shared by the per-queue gauges below.
	type qsnap struct {
		name string
		cfg  queue.Config
		live queue.Live
	}
	var snaps []qsnap
	var at time.Time
	o.Reg.Gauge("qsrv_queue_messages", "Live messages by state: ready, delayed, in_flight, and waiting (behind the head of their ordering group). They add up to the queue's messages.", func(emit func(float64, ...metrics.Label)) {
		snaps, at = snaps[:0], b.Now()
		for _, q := range b.Queues() {
			snaps = append(snaps, qsnap{q.Name(), q.Config(), q.Live()})
		}
		for _, s := range snaps {
			emit(float64(s.live.Ready), L("queue", s.name), L("state", "ready"))
			emit(float64(s.live.Delayed), L("queue", s.name), L("state", "delayed"))
			emit(float64(s.live.InFlight), L("queue", s.name), L("state", "in_flight"))
			emit(float64(max(0, s.live.Messages-s.live.Ready-s.live.Delayed-s.live.InFlight)), L("queue", s.name), L("state", "waiting"))
		}
	})
	o.Reg.Gauge("qsrv_queue_groups", "Ordering groups with live messages.", func(emit func(float64, ...metrics.Label)) {
		for _, s := range snaps {
			emit(float64(s.live.Groups), L("queue", s.name))
		}
	})
	o.Reg.Gauge("qsrv_queue_oldest_message_age_seconds", "Age of the oldest live message (0 if empty). The key backlog/latency signal.", func(emit func(float64, ...metrics.Label)) {
		for _, s := range snaps {
			age := 0.0
			if !s.live.OldestEnqueuedAt.IsZero() {
				age = max(at.Sub(s.live.OldestEnqueuedAt).Seconds(), 0)
			}
			emit(age, L("queue", s.name))
		}
	})
	o.Reg.Gauge("qsrv_queue_max_messages", "Configured depth limit (only queues that have one).", func(emit func(float64, ...metrics.Label)) {
		for _, s := range snaps {
			if s.cfg.MaxMessages > 0 {
				emit(float64(s.cfg.MaxMessages), L("queue", s.name))
			}
		}
	})
	o.Reg.Gauge("qsrv_queue_info", "Queue configuration (value is always 1).", func(emit func(float64, ...metrics.Label)) {
		for _, s := range snaps {
			emit(1, L("queue", s.name), L("dead_letter_queue", s.cfg.DeadLetterQueue),
				L("ordering", string(s.cfg.Ordering)), L("max_attempts", strconv.Itoa(s.cfg.Retry.MaxAttempts)))
		}
	})
	o.Reg.Gauge("qsrv_store_pending_writes", "Ops queued for the writer (sustained > 0 means the disk can't keep up).", func(emit func(float64, ...metrics.Label)) {
		emit(float64(b.PendingWrites()))
	})

	o.Reg.Gauge("qsrv_db_size_bytes", "SQLite files on disk.", func(emit func(float64, ...metrics.Label)) {
		for _, f := range []struct{ suffix, label string }{{"", "db"}, {"-wal", "wal"}} {
			if st, err := os.Stat(filepath.Join(dataDir, "qsrv.db"+f.suffix)); err == nil {
				emit(float64(st.Size()), L("file", f.label))
			}
		}
	})
	o.Reg.Gauge("qsrv_backup_last_success_timestamp_seconds", "Unix time of the last complete snapshot (0 = none yet; absent when snapshots are off).", func(emit func(float64, ...metrics.Label)) {
		if st := b.BackupStatus(); st.Enabled {
			v := 0.0
			if !st.LastSuccess.IsZero() {
				v = float64(st.LastSuccess.Unix())
			}
			emit(v)
		}
	})
	o.Reg.Gauge("qsrv_backup_interval_seconds", "Configured interval between snapshots.", func(emit func(float64, ...metrics.Label)) {
		if st := b.BackupStatus(); st.Enabled {
			emit(st.Every.Seconds())
		}
	})
	o.Reg.Gauge("qsrv_backup_last_duration_seconds", "How long the last complete snapshot took.", func(emit func(float64, ...metrics.Label)) {
		if st := b.BackupStatus(); st.Enabled && !st.LastSuccess.IsZero() {
			emit(st.LastTook.Seconds())
		}
	})
	o.Reg.Gauge("qsrv_backup_last_size_bytes", "Size of the last complete snapshot.", func(emit func(float64, ...metrics.Label)) {
		if st := b.BackupStatus(); st.Enabled && !st.LastSuccess.IsZero() {
			emit(float64(st.LastBytes))
		}
	})
	o.Reg.Gauge("qsrv_backup_failures", "Snapshots that failed since qsrv started.", func(emit func(float64, ...metrics.Label)) {
		if st := b.BackupStatus(); st.Enabled {
			emit(float64(st.Failures))
		}
	})
	o.Reg.Gauge("qsrv_storage_low", "1 while free disk is below -min-free-disk and sends are refused (503 storage_full).", func(emit func(float64, ...metrics.Label)) {
		v := 0.0
		if b.StorageLow() {
			v = 1
		}
		emit(v)
	})
	o.Reg.Gauge("qsrv_disk_free_bytes", "Free bytes on the data-dir filesystem (available to the process).", func(emit func(float64, ...metrics.Label)) {
		if free, _, err := diskUsage(dataDir); err == nil {
			emit(float64(free))
		}
	})
	o.Reg.Gauge("qsrv_disk_size_bytes", "Total bytes of the data-dir filesystem.", func(emit func(float64, ...metrics.Label)) {
		if _, total, err := diskUsage(dataDir); err == nil {
			emit(float64(total))
		}
	})

	o.Reg.Gauge("qsrv_go_goroutines", "Goroutines.", func(emit func(float64, ...metrics.Label)) {
		emit(float64(runtime.NumGoroutine()))
	})
	o.Reg.Gauge("qsrv_go_heap_inuse_bytes", "Go heap in use.", func(emit func(float64, ...metrics.Label)) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		emit(float64(ms.HeapInuse))
	})
	o.Reg.Gauge("qsrv_process_resident_memory_bytes", "Resident memory (includes wasm/C heaps).", func(emit func(float64, ...metrics.Label)) {
		if rss, err := residentMemory(); err == nil {
			emit(float64(rss))
		}
	})
}

// Handler serves /metrics.
func (o *Obs) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		o.scrapeMu.Lock()
		defer o.scrapeMu.Unlock()
		o.Reg.Write(w)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

// HTTPMiddleware records per-route counts and latency. The route label is the
// ServeMux pattern (bounded cardinality), never the raw path.
func (o *Obs) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		t0 := time.Now()
		next.ServeHTTP(rec, r)
		route := r.Pattern // set in place by the inner ServeMux
		if route == "" || route == "/" {
			route = "unmatched"
		}
		o.httpReqs.Inc(metrics.L("route", route), metrics.L("method", r.Method), metrics.L("code", strconv.Itoa(rec.code)))
		o.httpDur.With(metrics.L("route", route)).Observe(time.Since(t0).Seconds())
	})
}
