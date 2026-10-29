package httpapi_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

var key = strings.Repeat("k", 40)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	auth, err := httpapi.NewAuthenticator([]string{key})
	if err != nil {
		t.Fatal(err)
	}
	b, err := queue.Open(t.TempDir(), queue.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.New(b, httpapi.Options{Auth: auth, MaxMessageBytes: 1024}))
	t.Cleanup(func() { srv.Close(); b.Close() })
	return srv
}

func TestAuth(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	_, err := client.New(srv.URL, "wrong-key-wrong-key-wrong-key-xx").ListQueues(ctx)
	var ae *client.APIError
	if !errorsAs(err, &ae) || ae.Status != http.StatusUnauthorized {
		t.Fatalf("want 401, got %v", err)
	}
	if _, err := client.New(srv.URL, "").ListQueues(ctx); err == nil {
		t.Fatal("missing key accepted")
	}
	resp, _ := http.Get(srv.URL + "/healthz")
	if resp.StatusCode != 200 {
		t.Fatalf("healthz must be public, got %d", resp.StatusCode)
	}
}

func TestShortKeyRejected(t *testing.T) {
	if _, err := httpapi.NewAuthenticator([]string{"short"}); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestRoundTrip(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	c := client.New(srv.URL, key)

	if _, err := c.CreateQueue(ctx, "jobs", api.QueueConfig{
		VisibilityTimeout: api.Duration(10 * time.Second),
		Retry:             &api.RetryPolicy{MaxAttempts: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateQueue(ctx, "jobs", api.QueueConfig{}); !isStatus(err, http.StatusConflict) {
		t.Fatalf("want 409, got %v", err)
	}

	binary := []byte{0xff, 0x00, 0xfe}
	if _, err := c.Send(ctx, "jobs", binary, "user-1"); err != nil {
		t.Fatal(err)
	}
	c.Send(ctx, "jobs", []byte("second"), "user-1")

	d, err := c.Receive(ctx, "jobs", 0)
	if err != nil || d == nil {
		t.Fatalf("receive: %v %v", d, err)
	}
	if !bytes.Equal(d.Message.Bytes(), binary) || d.Message.Body != nil || d.Message.GroupID != "user-1" {
		t.Fatalf("binary body/group mangled: %+v", d.Message)
	}
	if empty, _ := c.Receive(ctx, "jobs", 0); empty != nil {
		t.Fatal("group must block the second message")
	}
	if _, err := c.Extend(ctx, "jobs", d.LeaseID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ctx, "jobs", d.LeaseID); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ctx, "jobs", d.LeaseID); !client.IsLeaseGone(err) {
		t.Fatalf("want lease_gone, got %v", err)
	}
	d2, _ := c.Receive(ctx, "jobs", 0)
	if d2 == nil || *d2.Message.Body != "second" {
		t.Fatalf("got %+v", d2)
	}
	if err := c.Nack(ctx, "jobs", d2.LeaseID); err != nil {
		t.Fatal(err)
	}
	q, _ := c.GetQueue(ctx, "jobs")
	if q.Stats.Messages != 1 {
		t.Fatalf("stats %+v", q.Stats)
	}
}

func TestLimits(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	c := client.New(srv.URL, key)
	c.CreateQueue(ctx, "small", api.QueueConfig{MaxMessages: 1})

	if _, err := c.Send(ctx, "small", bytes.Repeat([]byte("x"), 2000), ""); !isStatus(err, http.StatusRequestEntityTooLarge) {
		t.Fatalf("want 413, got %v", err)
	}
	c.Send(ctx, "small", []byte("a"), "")
	if _, err := c.Send(ctx, "small", []byte("b"), ""); !client.IsQueueFull(err) {
		t.Fatalf("want queue_full, got %v", err)
	}
	if _, err := c.CreateQueue(ctx, "bad", api.QueueConfig{Ordering: "random"}); !isStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("want 422, got %v", err)
	}
	if _, err := c.Send(ctx, "nope", []byte("a"), ""); !client.IsNotFound(err) {
		t.Fatalf("want not_found, got %v", err)
	}
}

func TestReplayHTTP(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	c := client.New(srv.URL, key)
	q, _ := c.CreateQueue(ctx, "src", api.QueueConfig{})
	if q.Retention == nil || q.Retention.D() != httpapi.DefaultRetention {
		t.Fatalf("default retention not applied: %+v", q.Retention)
	}
	forever := api.Duration(0)
	if q, _ := c.CreateQueue(ctx, "dst", api.QueueConfig{Retention: &forever}); q.Retention.D() != 0 {
		t.Fatalf("explicit 0 retention lost: %v", q.Retention.D())
	}
	for i := range 3 {
		c.Send(ctx, "src", []byte{byte('a' + i)}, "g")
		d, _ := c.Receive(ctx, "src", 0)
		c.Ack(ctx, "src", d.LeaseID)
	}
	res, err := c.Replay(ctx, "src", api.ReplayRequest{To: "dst", Limit: 2})
	if err != nil || res.Replayed != 2 || res.NextCursor == 0 {
		t.Fatalf("page 1: %+v %v", res, err)
	}
	res, err = c.Replay(ctx, "src", api.ReplayRequest{To: "dst", Limit: 2, Cursor: res.NextCursor})
	if err != nil || res.Replayed != 1 || res.NextCursor != 0 {
		t.Fatalf("page 2: %+v %v", res, err)
	}
	d, _ := c.Receive(ctx, "dst", 0)
	if d == nil || *d.Message.Body != "a" || d.Message.ReplayOf == "" || d.Message.GroupID != "g" {
		t.Fatalf("got %+v", d)
	}
	if _, err := c.Replay(ctx, "src", api.ReplayRequest{To: "missing"}); !client.IsNotFound(err) {
		t.Fatalf("want not_found, got %v", err)
	}
	if _, err := c.Replay(ctx, "src", api.ReplayRequest{}); !isStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("want 422 without to, got %v", err)
	}
	info, _ := c.GetQueue(ctx, "src")
	if info.Stats.History != 3 {
		t.Fatalf("history %+v", info.Stats)
	}
}

func isStatus(err error, code int) bool {
	var ae *client.APIError
	return errorsAs(err, &ae) && ae.Status == code
}

func TestSendDelay(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	c := client.New(srv.URL, key)
	if _, err := c.CreateQueue(ctx, "q", api.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendWith(ctx, "q", []byte("x"), client.SendOptions{Delay: 200 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if d, err := c.Receive(ctx, "q", 0); err != nil || d != nil {
		t.Fatalf("delayed message delivered early: %v %v", d, err)
	}
	d, err := c.Receive(ctx, "q", 5*time.Second)
	if err != nil || d == nil || string(d.Message.Bytes()) != "x" {
		t.Fatalf("got %v %v", d, err)
	}
	var ae *client.APIError
	_, err = c.SendWith(ctx, "q", []byte("x"), client.SendOptions{Delay: 1000 * time.Hour})
	if !errorsAs(err, &ae) || ae.Status != http.StatusBadRequest {
		t.Fatalf("want 400 for delay over max, got %v", err)
	}
}
