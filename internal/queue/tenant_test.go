package queue

import (
	"errors"
	"testing"
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
