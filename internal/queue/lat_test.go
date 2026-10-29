package queue

import (
	"slices"
	"testing"
	"time"
)

// Sequential (no group commit help): pure per-op cost of a durable send.
func TestSequentialSendLatency(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	b, err := Open(t.TempDir(), Options{JanitorEvery: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	q, _ := b.Create("q", Config{})
	body := make([]byte, 1024)
	var d []time.Duration
	for range 500 {
		t0 := time.Now()
		q.Send(body, "")
		d = append(d, time.Since(t0))
	}
	slices.Sort(d)
	t.Logf("sequential durable send: p50=%v p99=%v", d[len(d)/2], d[len(d)*99/100])
}
