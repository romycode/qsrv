package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Performance suite. All durable benchmarks use real fsync (group commit),
// so absolute numbers depend on the disk; compare runs on the same machine
// (CI compares base vs head on the same runner with benchstat).
//
//	go test -run '^$' -bench . -benchmem ./internal/queue/

func openDurable(tb testing.TB, dir string) *Broker {
	tb.Helper()
	b, err := Open(dir, Options{JanitorEvery: time.Hour, OnFatal: func(err error) { panic(err) }})
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func mustQueue(tb testing.TB, b *Broker, name string, cfg Config) *Queue {
	tb.Helper()
	q, err := b.Create(name, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return q
}

// Durable send throughput under concurrency, by body size.
func BenchmarkSend(b *testing.B) {
	for _, size := range []int{128, 1 << 10, 16 << 10} {
		b.Run(fmt.Sprintf("body=%d", size), func(b *testing.B) {
			br := openDurable(b, b.TempDir())
			defer br.Close()
			q := mustQueue(b, br, "q", Config{})
			body := make([]byte, size)
			b.SetBytes(int64(size))
			b.SetParallelism(8)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := q.Send(body, ""); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// One producer, no group commit help: per-op cost of a durable send.
func BenchmarkSendSequential(b *testing.B) {
	br := openDurable(b, b.TempDir())
	defer br.Close()
	q := mustQueue(b, br, "q", Config{})
	body := make([]byte, 1<<10)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := q.Send(body, ""); err != nil {
			b.Fatal(err)
		}
	}
}

// Drain a pre-filled backlog: receive (body read from SQLite) + durable ack.
func BenchmarkReceiveAck(b *testing.B) {
	br := openDurable(b, b.TempDir())
	defer br.Close()
	q := mustQueue(b, br, "q", Config{})
	body := make([]byte, 1<<10)
	for range b.N {
		if _, err := q.Send(body, ""); err != nil {
			b.Fatal(err)
		}
	}
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			d, err := q.Receive(ctx, time.Second)
			if err != nil {
				b.Error(err)
				return
			}
			if err := q.Ack(d.LeaseID); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// Full cycle with per-entity ordering: 64 groups, strict order within each.
func BenchmarkGroupsCycle(b *testing.B) {
	br := openDurable(b, b.TempDir())
	defer br.Close()
	q := mustQueue(b, br, "q", Config{})
	body := make([]byte, 512)
	groups := make([]string, 64)
	for i := range groups {
		groups[i] = fmt.Sprintf("g%02d", i)
	}
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		i := 0
		for pb.Next() {
			i++
			if _, err := q.Send(body, groups[i%len(groups)]); err != nil {
				b.Error(err)
				return
			}
			d, err := q.Receive(ctx, time.Second)
			if err != nil {
				b.Error(err)
				return
			}
			if err := q.Ack(d.LeaseID); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// seed fills dir without fsync (fast setup) and closes it cleanly;
// the benchmark then reopens it with real fsync.
func seed(tb testing.TB, dir string, fill func(*Broker)) {
	tb.Helper()
	b, err := Open(dir, Options{NoSync: true, JanitorEvery: time.Hour})
	if err != nil {
		tb.Fatal(err)
	}
	fill(b)
	if err := b.Close(); err != nil {
		tb.Fatal(err)
	}
}

// Re-enqueue history in pages of 1000 messages (one op = one page).
func BenchmarkReplay(b *testing.B) {
	const history, page = 5_000, 1_000
	dir := b.TempDir()
	seed(b, dir, func(br *Broker) {
		src := mustQueue(b, br, "src", Config{Retention: 0})
		mustQueue(b, br, "dst", Config{})
		body := make([]byte, 512)
		ctx := context.Background()
		for range history {
			if _, err := src.Send(body, ""); err != nil {
				b.Fatal(err)
			}
			d, err := src.Receive(ctx, time.Second)
			if err != nil {
				b.Fatal(err)
			}
			if err := src.Ack(d.LeaseID); err != nil {
				b.Fatal(err)
			}
		}
	})
	br := openDurable(b, dir)
	defer br.Close()
	b.ReportAllocs()
	var cursor uint64
	replayed := 0
	start := time.Now()
	for b.Loop() {
		res, err := br.Replay("src", ReplayRequest{To: "dst", After: cursor, Limit: page})
		if err == nil && res.Replayed == 0 && cursor != 0 { // history exhausted: wrap around
			cursor = 0
			res, err = br.Replay("src", ReplayRequest{To: "dst", Limit: page})
		}
		if err != nil {
			b.Fatal(err)
		}
		if res.Replayed == 0 {
			b.Fatal("replay returned nothing")
		}
		replayed += res.Replayed
		cursor = res.Next
	}
	b.ReportMetric(float64(replayed)/time.Since(start).Seconds(), "msgs/s")
}

// Startup cost: open a data dir holding 10k live messages.
func BenchmarkOpen10kLive(b *testing.B) {
	dir := b.TempDir()
	seed(b, dir, func(br *Broker) {
		q := mustQueue(b, br, "q", Config{})
		body := make([]byte, 512)
		for range 10_000 {
			if _, err := q.Send(body, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportAllocs()
	for b.Loop() {
		br := openDurable(b, dir)
		if err := br.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
