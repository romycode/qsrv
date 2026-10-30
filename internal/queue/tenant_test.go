package queue

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestTenantNames(t *testing.T) {
	for _, ok := range []string{"q", "acme/orders", "a-1_b/x.y-z"} {
		if !validFullName(ok) {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "Acme/orders", "/orders", "acme/", "a/b/c", "acme orders/x", "-a/x"} {
		if validFullName(bad) {
			t.Errorf("%q should be invalid", bad)
		}
	}
	if tn, n := SplitName("acme/orders"); tn != "acme" || n != "orders" || JoinName(tn, n) != "acme/orders" {
		t.Fatalf("split/join: %q %q", tn, n)
	}
}

func TestNoCrossTenantReferences(t *testing.T) {
	b := newTestBroker(t, nil)
	mustCreate(t, b, "acme/dlq", Config{})
	mustCreate(t, b, "globex/q", Config{})
	if _, err := b.Create("globex/orders", Config{DeadLetterQueue: "acme/dlq"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("cross-tenant DLQ: %v", err)
	}
	if _, err := b.CreateTopic("acme/t", []string{"globex/q"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("cross-tenant subscription at create: %v", err)
	}
	b.CreateTopic("acme/t", nil)
	if _, err := b.Subscribe("acme/t", "globex/q"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("cross-tenant subscribe: %v", err)
	}
	if _, err := b.Replay("acme/dlq", ReplayRequest{To: "globex/q"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("cross-tenant replay: %v", err)
	}
	mustCreate(t, b, "acme/orders", Config{DeadLetterQueue: "acme/dlq"}) // same tenant: fine
}

func TestAssignTenant(t *testing.T) {
	dir := t.TempDir()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	b := openB(t, dir, clk)
	b.Create("dlq", Config{})
	q, _ := b.Create("orders", Config{Retry: RetryPolicy{MaxAttempts: 1}, DeadLetterQueue: "dlq"})
	b.Create("billing", Config{})
	b.Create("other/kept", Config{})
	b.CreateTopic("events", []string{"orders", "billing"})
	q.Send([]byte("live"), "")
	q.Send([]byte("dead"), "")
	q.Ack(recv(t, q).LeaseID)     // "live" -> history
	q.Nack(recv(t, q).LeaseID)    // "dead" -> moved to dlq
	if len(b.Untenanted()) != 4 { // dlq, orders, billing, events
		t.Fatalf("untenanted %v", b.Untenanted())
	}
	b.Close()

	nq, nt, err := AssignTenant(dir, "acme")
	if err != nil || nq != 3 || nt != 1 {
		t.Fatalf("assign: %d %d %v", nq, nt, err)
	}

	b = openB(t, dir, clk)
	defer b.Close()
	if u := b.Untenanted(); len(u) != 0 {
		t.Fatalf("still untenanted: %v", u)
	}
	if got := b.List(); !slices.Equal(got, []string{"acme/billing", "acme/dlq", "acme/orders", "other/kept"}) {
		t.Fatalf("queues %v", got)
	}
	o := mustGet(t, b, "acme/orders")
	if o.Config().DeadLetterQueue != "acme/dlq" || o.Stats().History != 2 {
		t.Fatalf("orders: dlq=%q history=%d", o.Config().DeadLetterQueue, o.Stats().History)
	}
	if got := string(recv(t, mustGet(t, b, "acme/dlq")).Message.Body); got != "dead" {
		t.Fatalf("dlq message: %q", got)
	}
	if top, err := b.GetTopic("acme/events"); err != nil || !slices.Equal(top.Queues, []string{"acme/billing", "acme/orders"}) {
		t.Fatalf("topic: %+v %v", top, err)
	}
	if res, err := b.Replay("acme/orders", ReplayRequest{To: "acme/billing"}); err != nil || res.Replayed != 2 {
		t.Fatalf("replay after rename: %+v %v", res, err)
	}
	if _, _, err := AssignTenant(dir, "Bad Tenant"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid tenant: %v", err)
	}
}
