package httpapi_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

func TestPeekAndOverview(t *testing.T) {
	srv, _ := multiTenant(t)
	ctx := context.Background()
	acme := client.New(srv.URL, acmeKey)
	globex := client.New(srv.URL, globexKey)
	admin := client.New(srv.URL, adminKey)

	acme.CreateQueue(ctx, "dlq", api.QueueConfig{})
	acme.CreateQueue(ctx, "orders", api.QueueConfig{DeadLetterQueue: "dlq", Retry: &api.RetryPolicy{MaxAttempts: 1}})
	acme.CreateTopic(ctx, "events", "orders")
	globex.CreateQueue(ctx, "orders", api.QueueConfig{})

	m1, _ := acme.Send(ctx, "orders", []byte(`{"n":1}`), "g")
	m2, _ := acme.Send(ctx, "orders", []byte{0xff, 0x00}, "")
	acme.SendWith(ctx, "orders", []byte("later"), client.SendOptions{Delay: time.Hour})
	d, _ := acme.Receive(ctx, "orders", 0) // m1 in flight

	page, err := acme.PeekMessages(ctx, "orders", client.PeekOptions{})
	if err != nil || len(page.Messages) != 3 {
		t.Fatalf("peek: %+v %v", page, err)
	}
	if m := page.Messages[0]; m.ID != m1.ID || m.State != "in_flight" || m.LeaseExpiresAt == nil || m.Body != nil || m.GroupID != "g" {
		t.Fatalf("first: %+v", m)
	}
	if m := page.Messages[2]; m.State != "delayed" || m.AvailableAt == nil {
		t.Fatalf("delayed: %+v", m)
	}
	p1, _ := acme.PeekMessages(ctx, "orders", client.PeekOptions{Limit: 2})
	p2, _ := acme.PeekMessages(ctx, "orders", client.PeekOptions{Limit: 2, Cursor: p1.NextCursor})
	if len(p1.Messages) != 2 || p1.NextCursor == 0 || len(p2.Messages) != 1 || p2.NextCursor != 0 {
		t.Fatalf("pages: %d next=%d, %d next=%d", len(p1.Messages), p1.NextCursor, len(p2.Messages), p2.NextCursor)
	}

	// One message with its body: text, or base64 when not UTF-8.
	one, err := acme.PeekMessage(ctx, "orders", m1.ID)
	if err != nil || one.Body == nil || *one.Body != `{"n":1}` {
		t.Fatalf("peek one: %+v %v", one, err)
	}
	if bin, _ := acme.PeekMessage(ctx, "orders", m2.ID); bin.Body != nil || !slices.Equal(bin.BodyBase64, []byte{0xff, 0x00}) || bin.Size != 2 {
		t.Fatalf("binary body: %+v", bin)
	}
	if _, err := acme.PeekMessage(ctx, "orders", "nope"); !client.IsNotFound(err) {
		t.Fatalf("missing message: %v", err)
	}

	// History, with the DLQ shown in the tenant's names.
	acme.DeadLetter(ctx, "orders", d.LeaseID)
	h, err := acme.PeekMessages(ctx, "orders", client.PeekOptions{History: true})
	if err != nil || len(h.Messages) != 1 || h.Messages[0].State != "dead_lettered" || h.Messages[0].MovedTo != "dlq" || h.Messages[0].DoneAt == nil {
		t.Fatalf("history: %+v %v", h, err)
	}
	if _, err := acme.PeekMessages(ctx, "orders", client.PeekOptions{States: []string{"acked"}}); err == nil {
		t.Fatal("history state accepted in the live view")
	}

	// Other tenants can't peek: 404, like a queue that doesn't exist.
	if _, err := globex.PeekMessage(ctx, "orders", m1.ID); !client.IsNotFound(err) {
		t.Fatalf("globex peeked acme's message: %v", err)
	}
	if p, _ := globex.PeekMessages(ctx, "orders", client.PeekOptions{}); len(p.Messages) != 0 {
		t.Fatalf("globex sees %d messages in its own empty queue", len(p.Messages))
	}

	// Overview: only the key's tenant, local names, live counts.
	ov, err := acme.Overview(ctx)
	if err != nil || ov.Tenant != "acme" || len(ov.Queues) != 2 || len(ov.Topics) != 1 {
		t.Fatalf("acme overview: %+v %v", ov, err)
	}
	dlq, orders := ov.Queues[0], ov.Queues[1]
	if dlq.Name != "dlq" || dlq.Messages != 1 || !slices.Equal(dlq.IsDeadLetterQueueOf, []string{"orders"}) {
		t.Fatalf("dlq summary: %+v", dlq)
	}
	if orders.Name != "orders" || orders.Messages != 2 || orders.Delayed != 1 || orders.DeadLetterQueue != "dlq" ||
		!slices.Equal(orders.Topics, []string{"events"}) || orders.OldestEnqueuedAt == nil {
		t.Fatalf("orders summary: %+v", orders)
	}
	all, _ := admin.Overview(ctx)
	if all.Tenant != "" || len(all.Queues) != 3 || all.Queues[2].Name != "globex/orders" {
		t.Fatalf("admin overview: %+v", all.Queues)
	}
	if p, err := admin.PeekMessages(ctx, "acme/orders", client.PeekOptions{}); err != nil || len(p.Messages) != 2 {
		t.Fatalf("admin peek with a full name: %+v %v", p, err)
	}
}
