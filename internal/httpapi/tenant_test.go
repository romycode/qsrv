package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

var (
	acmeKey   = strings.Repeat("a", 40)
	globexKey = strings.Repeat("g", 40)
	adminKey  = strings.Repeat("x", 40)
)

func multiTenant(t *testing.T) (*httptest.Server, *httpapi.Authenticator) {
	t.Helper()
	auth, err := httpapi.NewAuthenticator([]httpapi.KeyEntry{
		entry("acme", "app", acmeKey), entry("globex", "app", globexKey), entry("*", "ops", adminKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := queue.Open(t.TempDir(), queue.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.New(b, httpapi.Options{Auth: auth}))
	t.Cleanup(func() { srv.Close(); b.Close() })
	return srv, auth
}

func TestTenantIsolation(t *testing.T) {
	srv, _ := multiTenant(t)
	ctx := context.Background()
	acme := client.New(srv.URL, acmeKey)
	globex := client.New(srv.URL, globexKey)

	// Same names in both tenants, no clash.
	for _, c := range []*client.Client{acme, globex} {
		if _, err := c.CreateQueue(ctx, "dlq", api.QueueConfig{}); err != nil {
			t.Fatal(err)
		}
		q, err := c.CreateQueue(ctx, "orders", api.QueueConfig{DeadLetterQueue: "dlq"})
		if err != nil || q.Name != "orders" || q.DeadLetterQueue != "dlq" {
			t.Fatalf("create: %+v %v", q, err)
		}
	}
	acme.Send(ctx, "orders", []byte("acme secret"), "")

	if qs, _ := globex.ListQueues(ctx); !slices.Equal(qs, []string{"dlq", "orders"}) {
		t.Fatalf("globex list: %v", qs)
	}
	if d, _ := globex.Receive(ctx, "orders", 0); d != nil {
		t.Fatalf("globex read acme's message: %+v", d)
	}
	// Other tenants' queues don't exist, whatever the spelling.
	for _, name := range []string{"acme/orders", "../acme/orders"} {
		if _, err := globex.GetQueue(ctx, name); !client.IsNotFound(err) {
			t.Fatalf("globex GET %q: %v", name, err)
		}
		if _, err := globex.Send(ctx, name, []byte("x"), ""); !client.IsNotFound(err) {
			t.Fatalf("globex send %q: %v", name, err)
		}
		if err := globex.DeleteQueue(ctx, name); !client.IsNotFound(err) {
			t.Fatalf("globex delete %q: %v", name, err)
		}
	}
	// No references into another tenant through bodies either.
	if _, err := globex.CreateQueue(ctx, "x", api.QueueConfig{DeadLetterQueue: "acme/dlq"}); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("cross-tenant DLQ: %v", err)
	}
	if _, err := globex.Replay(ctx, "orders", api.ReplayRequest{To: "acme/orders"}); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("cross-tenant replay: %v", err)
	}
	if _, err := globex.CreateTopic(ctx, "t", "acme/orders"); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("cross-tenant topic: %v", err)
	}

	d, err := acme.Receive(ctx, "orders", 0)
	if err != nil || d == nil || string(d.Message.Bytes()) != "acme secret" {
		t.Fatalf("acme: %+v %v", d, err)
	}
	// A lease id alone doesn't give access from another tenant.
	if err := globex.Ack(ctx, "orders", d.LeaseID); !client.IsLeaseGone(err) {
		t.Fatalf("globex acked acme's lease: %v", err)
	}
	if err := acme.Ack(ctx, "orders", d.LeaseID); err != nil {
		t.Fatal(err)
	}
}

func TestTenantTopics(t *testing.T) {
	srv, _ := multiTenant(t)
	ctx := context.Background()
	acme := client.New(srv.URL, acmeKey)
	globex := client.New(srv.URL, globexKey)
	acme.CreateQueue(ctx, "billing", api.QueueConfig{})
	globex.CreateQueue(ctx, "billing", api.QueueConfig{})
	top, err := acme.CreateTopic(ctx, "events", "billing")
	if err != nil || top.Name != "events" || !slices.Equal(top.Queues, []string{"billing"}) {
		t.Fatalf("topic: %+v %v", top, err)
	}
	if _, err := globex.Publish(ctx, "events", []byte("x"), client.SendOptions{}); !client.IsNotFound(err) {
		t.Fatalf("globex published to acme's topic: %v", err)
	}
	pub, err := acme.Publish(ctx, "events", []byte("e1"), client.SendOptions{})
	if err != nil || len(pub) != 1 || pub[0].Queue != "billing" {
		t.Fatalf("publish: %+v %v", pub, err)
	}
	if ts, _ := globex.ListTopics(ctx); len(ts) != 0 {
		t.Fatalf("globex sees topics: %+v", ts)
	}
	if d, _ := globex.Receive(ctx, "billing", 0); d != nil {
		t.Fatal("copy landed in globex's billing")
	}
}

func TestAdminSeesFullNames(t *testing.T) {
	srv, _ := multiTenant(t)
	ctx := context.Background()
	acme := client.New(srv.URL, acmeKey)
	admin := client.New(srv.URL, adminKey)
	acme.CreateQueue(ctx, "dlq", api.QueueConfig{})
	acme.CreateQueue(ctx, "orders", api.QueueConfig{DeadLetterQueue: "dlq"})

	if qs, _ := admin.ListQueues(ctx); !slices.Equal(qs, []string{"acme/dlq", "acme/orders"}) {
		t.Fatalf("admin list: %v", qs)
	}
	q, err := admin.GetQueue(ctx, "acme/orders")
	if err != nil || q.Name != "acme/orders" || q.DeadLetterQueue != "acme/dlq" {
		t.Fatalf("admin get: %+v %v", q, err)
	}
	if _, err := admin.Send(ctx, "acme/orders", []byte("from ops"), ""); err != nil {
		t.Fatal(err)
	}
	if d, _ := acme.Receive(ctx, "orders", 0); d == nil || string(d.Message.Bytes()) != "from ops" {
		t.Fatalf("acme didn't get admin's message: %+v", d)
	}
	// Admin keys must name the tenant: no queue lives outside one.
	if _, err := admin.CreateQueue(ctx, "loose", api.QueueConfig{}); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("admin created a queue without tenant: %v", err)
	}
	if _, err := admin.CreateQueue(ctx, "globex/x", api.QueueConfig{DeadLetterQueue: "acme/dlq"}); !hasStatus(err, http.StatusUnprocessableEntity) {
		t.Fatalf("admin crossed tenants: %v", err)
	}
}

func TestKeyReload(t *testing.T) {
	srv, auth := multiTenant(t)
	ctx := context.Background()
	newKey := strings.Repeat("n", 40)
	if _, err := client.New(srv.URL, newKey).ListQueues(ctx); !hasStatus(err, http.StatusUnauthorized) {
		t.Fatalf("unknown key: %v", err)
	}
	if err := auth.Replace([]httpapi.KeyEntry{entry("acme", "rotated", newKey)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.New(srv.URL, newKey).ListQueues(ctx); err != nil {
		t.Fatalf("new key: %v", err)
	}
	if _, err := client.New(srv.URL, acmeKey).ListQueues(ctx); !hasStatus(err, http.StatusUnauthorized) {
		t.Fatalf("removed key still works: %v", err)
	}
	if err := auth.Replace(nil); err == nil || auth.Len() != 1 {
		t.Fatalf("empty reload must be refused and keep the keys: %v %d", err, auth.Len())
	}
}
