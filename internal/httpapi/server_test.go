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
	auth, err := httpapi.NewAuthenticator([]httpapi.KeyEntry{entry("test", "suite", key)})
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

func entry(tenant, name, secret string) httpapi.KeyEntry {
	e, err := httpapi.ParseKeyEntry(tenant + ":" + name + ":" + secret)
	if err != nil {
		panic(err)
	}
	return e
}

func TestKeyEntries(t *testing.T) {
	for _, bad := range []string{
		strings.Repeat("k", 40),                     // no tenant
		"acme:billing:short",                        // too short
		"Acme:billing:" + strings.Repeat("k", 40),   // bad tenant
		"acme:bad name:" + strings.Repeat("k", 40),  // bad key name
		"acme/x:billing:" + strings.Repeat("k", 40), // slash
	} {
		if _, err := httpapi.ParseKeyEntry(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	e, err := httpapi.ParseKeyEntry("*:ops:" + strings.Repeat("k", 40))
	if err != nil || !e.Admin() || e.Name != "ops" {
		t.Fatalf("admin entry: %+v %v", e, err)
	}
	keys, err := httpapi.ParseKeys(strings.NewReader("# comment\nacme:a:" + strings.Repeat("a", 32) + ", globex:b:" + strings.Repeat("b", 32) + "\n\n"))
	if err != nil || len(keys) != 2 || keys[1].Tenant != "globex" {
		t.Fatalf("parse: %+v %v", keys, err)
	}
	if _, err := httpapi.NewAuthenticator([]httpapi.KeyEntry{keys[0], keys[0]}); err == nil {
		t.Fatal("duplicate key accepted")
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

func TestTopics(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	c := client.New(srv.URL, key)
	for _, q := range []string{"billing", "email"} {
		if _, err := c.CreateQueue(ctx, q, api.QueueConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	top, err := c.CreateTopic(ctx, "orders", "email")
	if err != nil || top.Name != "orders" || len(top.Queues) != 1 {
		t.Fatalf("create: %+v %v", top, err)
	}
	if _, err := c.CreateTopic(ctx, "orders"); !hasStatus(err, http.StatusConflict) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := c.CreateTopic(ctx, "x", "missing"); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("missing queue: %v", err)
	}
	if top, err = c.Subscribe(ctx, "orders", "billing"); err != nil || len(top.Queues) != 2 {
		t.Fatalf("subscribe: %+v %v", top, err)
	}

	pub, err := c.Publish(ctx, "orders", []byte("o1"), client.SendOptions{GroupID: "c42"})
	if err != nil || len(pub) != 2 || pub[0].Queue != "billing" || pub[1].Queue != "email" || pub[0].ID == "" {
		t.Fatalf("publish: %+v %v", pub, err)
	}
	for _, q := range []string{"billing", "email"} {
		d, err := c.Receive(ctx, q, 0)
		if err != nil || d == nil || string(d.Message.Bytes()) != "o1" || d.Message.GroupID != "c42" {
			t.Fatalf("%s: %+v %v", q, d, err)
		}
	}

	if err := c.DeleteQueue(ctx, "email"); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("delete subscribed queue: %v", err)
	}
	if top, err = c.Unsubscribe(ctx, "orders", "email"); err != nil || len(top.Queues) != 1 {
		t.Fatalf("unsubscribe: %+v %v", top, err)
	}
	if list, err := c.ListTopics(ctx); err != nil || len(list) != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	if _, err := c.Publish(ctx, "nope", []byte("x"), client.SendOptions{}); !client.IsNotFound(err) {
		t.Fatalf("publish to missing topic: %v", err)
	}
	if err := c.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetTopic(ctx, "orders"); !client.IsNotFound(err) {
		t.Fatalf("deleted topic: %v", err)
	}
}

func hasStatus(err error, status int) bool {
	var ae *client.APIError
	return errorsAs(err, &ae) && ae.Status == status
}

func TestNackDeadLetter(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()
	c := client.New(srv.URL, key)
	c.CreateQueue(ctx, "dlq", api.QueueConfig{})
	c.CreateQueue(ctx, "q", api.QueueConfig{DeadLetterQueue: "dlq", Retry: &api.RetryPolicy{MaxAttempts: 5}})
	c.Send(ctx, "q", []byte("bad"), "")
	d, _ := c.Receive(ctx, "q", 0)
	if err := c.DeadLetter(ctx, "q", d.LeaseID); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Receive(ctx, "dlq", 0); got == nil || string(got.Message.Bytes()) != "bad" {
		t.Fatalf("dlq: %+v", got)
	}
	if again, _ := c.Receive(ctx, "q", 0); again != nil {
		t.Fatal("dead-lettered message was retried")
	}
	// A plain nack (no body) still applies the retry policy.
	c.Send(ctx, "q", []byte("retry me"), "")
	d, _ = c.Receive(ctx, "q", 0)
	c.Nack(ctx, "q", d.LeaseID)
	if again, _ := c.Receive(ctx, "q", 0); again == nil || again.Message.Attempts != 2 {
		t.Fatalf("plain nack: %+v", again)
	}
}
