package obs_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/obs"
	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func scrape(t *testing.T, o *obs.Obs) string {
	t.Helper()
	rec := httptest.NewRecorder()
	o.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

func TestMetricsEndToEnd(t *testing.T) {
	clk := &clock{t: time.Now()}
	o := obs.New()
	dir := t.TempDir()
	b, err := queue.Open(dir, queue.Options{Clock: clk, NoSync: true, Hooks: o.Hooks()})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	o.RegisterBroker(b, dir, "test")
	srv := httptest.NewServer(o.HTTPMiddleware(httpapi.New(b, httpapi.Options{})))
	defer srv.Close()

	ctx := context.Background()
	c := client.New(srv.URL, "") // no auth = admin: full tenant/name names
	c.CreateQueue(ctx, "t/dlq", api.QueueConfig{})
	c.CreateQueue(ctx, "t/jobs", api.QueueConfig{
		VisibilityTimeout: api.Duration(5 * time.Second),
		Retry:             &api.RetryPolicy{MaxAttempts: 2},
		DeadLetterQueue:   "t/dlq",
		MaxMessages:       100,
	})
	for range 3 {
		c.Send(ctx, "t/jobs", []byte("x"), "")
	}
	d1, _ := c.Receive(ctx, "t/jobs", 0)
	c.Ack(ctx, "t/jobs", d1.LeaseID)
	d2, _ := c.Receive(ctx, "t/jobs", 0)
	c.Nack(ctx, "t/jobs", d2.LeaseID) // back to ready, attempt 1 of 2
	c.Receive(ctx, "t/jobs", 0)       // d2 again: redelivered
	clk.Advance(6 * time.Second)      // lease expires -> exhausted -> dlq
	clk.Advance(30 * time.Second)     // oldest live msg (3rd) now ~36s old
	c.Ack(ctx, "t/jobs", "nope")      // 410; also triggers expiry processing

	out := scrape(t, o)
	for _, want := range []string{
		`qsrv_messages_total{queue="t/jobs",event="sent"} 3`,
		`qsrv_messages_total{queue="t/jobs",event="acked"} 1`,
		`qsrv_messages_total{queue="t/jobs",event="nacked"} 1`,
		`qsrv_messages_total{queue="t/jobs",event="redelivered"} 1`,
		`qsrv_messages_total{queue="t/jobs",event="expired"} 1`,
		`qsrv_messages_total{queue="t/jobs",event="dead_lettered"} 1`,
		`qsrv_messages_total{queue="t/dlq",event="dead_letter_in"} 1`,
		`qsrv_queue_messages{queue="t/jobs",state="ready"} 1`,
		`qsrv_queue_messages{queue="t/dlq",state="ready"} 1`,
		`qsrv_queue_max_messages{queue="t/jobs"} 100`,
		`qsrv_queue_oldest_message_age_seconds{queue="t/jobs"} 36`,
		`qsrv_queue_info{queue="t/jobs",dead_letter_queue="t/dlq",ordering="available",max_attempts="2"} 1`,
		`qsrv_http_requests_total{route="POST /v1/queues/{name}/messages",method="POST",code="201"} 3`,
		`qsrv_http_requests_total{route="POST /v1/queues/{name}/leases/{lease}/ack",method="POST",code="410"} 1`,
		`qsrv_store_commit_duration_seconds_count`,
		`qsrv_disk_free_bytes `,
		`qsrv_db_size_bytes{file="db"}`,
		`qsrv_build_info{version="test"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	if t.Failed() {
		t.Log(out)
	}

	// deleting a queue drops its series (no stale gauges/counters)
	c.CreateQueue(ctx, "t/tmp", api.QueueConfig{})
	c.Send(ctx, "t/tmp", []byte("x"), "")
	if err := c.DeleteQueue(ctx, "t/tmp"); err != nil {
		t.Fatal(err)
	}
	if out := scrape(t, o); strings.Contains(out, `queue="t/tmp"`) {
		t.Fatal("deleted queue still exported")
	}
}

func TestConcurrentScrapes(t *testing.T) {
	o := obs.New()
	dir := t.TempDir()
	b, _ := queue.Open(dir, queue.Options{NoSync: true, Hooks: o.Hooks()})
	defer b.Close()
	o.RegisterBroker(b, dir, "test")
	q, _ := b.Create("q", queue.Config{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(2)
		go func() { defer wg.Done(); scrape(t, o) }()
		go func() { defer wg.Done(); q.Send([]byte("x"), "") }()
	}
	wg.Wait()
	_ = http.StatusOK
}
