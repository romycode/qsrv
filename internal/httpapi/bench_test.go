package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

// End-to-end over HTTP with auth and real fsync: send + receive + ack per op,
// through the Go client. Includes JSON/base64 encoding and loopback TCP.
func BenchmarkHTTPCycle(b *testing.B) {
	auth, err := httpapi.NewAuthenticator([]httpapi.KeyEntry{entry("bench", "b", key)})
	if err != nil {
		b.Fatal(err)
	}
	br, err := queue.Open(b.TempDir(), queue.Options{JanitorEvery: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.New(br, httpapi.Options{Auth: auth}))
	defer func() { srv.Close(); br.Close() }()

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 256
	c := client.New(srv.URL, key, client.WithHTTPClient(&http.Client{Transport: tr}))
	ctx := context.Background()
	if _, err := c.CreateQueue(ctx, "q", api.QueueConfig{}); err != nil {
		b.Fatal(err)
	}
	body := make([]byte, 1<<10)

	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.Send(ctx, "q", body, ""); err != nil {
				b.Error(err)
				return
			}
			d, err := c.Receive(ctx, "q", time.Second)
			if err != nil || d == nil {
				b.Errorf("receive: %v (delivery %v)", err, d)
				return
			}
			if err := c.Ack(ctx, "q", d.LeaseID); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
