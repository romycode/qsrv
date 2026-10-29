package queue

import (
	"context"
	"testing"
	"time"
)

// Full cycle per op: send + receive + ack, each fsynced (group commit).
// Run with -cpu to vary concurrency: go test -bench Cycle -cpu 1,8,64
func BenchmarkPersistentCycle(b *testing.B) {
	br, err := Open(b.TempDir(), Options{JanitorEvery: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer br.Close()
	q, _ := br.Create("q", Config{})
	body := make([]byte, 512)
	b.SetParallelism(8) // goroutines = 8 * GOMAXPROCS
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := q.Send(body, ""); err != nil {
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

func BenchmarkNoSyncCycle(b *testing.B) {
	q, _ := newTestBroker(b, nil).Create("q", Config{})
	body := make([]byte, 512)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			q.Send(body, "")
			d, _ := q.Receive(ctx, time.Second)
			q.Ack(d.LeaseID)
		}
	})
}
