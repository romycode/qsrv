package queue

import (
	"errors"
	"maps"
	"strings"
	"testing"
)

func TestAttributesTravelWithTheMessage(t *testing.T) {
	dir := t.TempDir()
	b := openB(t, dir, nil)
	mustCreate(t, b, "dlq", Config{})
	q := mustCreate(t, b, "q", Config{Retry: RetryPolicy{MaxAttempts: 1}, DeadLetterQueue: "dlq", Persistent: true})
	mustCreate(t, b, "replay", Config{})
	a := Attributes{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "content-type": "application/json"}

	sent, err := q.SendWith([]byte("x"), SendOptions{Attributes: a})
	if err != nil || !maps.Equal(sent.Attributes, a) {
		t.Fatalf("send: %+v %v", sent.Attributes, err)
	}
	b.Close() // survives a restart
	b = openB(t, dir, nil)
	defer b.Close()
	q = mustGet(t, b, "q")
	d := recv(t, q)
	if !maps.Equal(d.Message.Attributes, a) {
		t.Fatalf("delivered %v", d.Message.Attributes)
	}
	info, _, err := q.PeekMessage(d.Message.ID)
	if err != nil || !maps.Equal(info.Attributes, a) {
		t.Fatalf("peek %v %v", info.Attributes, err)
	}

	q.Nack(d.LeaseID) // one attempt: dead-lettered, attributes kept
	if dd := recv(t, mustGet(t, b, "dlq")); !maps.Equal(dd.Message.Attributes, a) {
		t.Fatalf("dead letter %v", dd.Message.Attributes)
	}
	if _, err := b.Replay("q", ReplayRequest{To: "replay"}); err != nil {
		t.Fatal(err)
	}
	if r := recv(t, mustGet(t, b, "replay")); !maps.Equal(r.Message.Attributes, a) || r.Message.ReplayOf != d.Message.ID {
		t.Fatalf("replayed %+v", r.Message)
	}

	plain, _ := q.Send([]byte("no attributes"), "")
	if plain.Attributes != nil || recv(t, q).Message.Attributes != nil {
		t.Fatal("attributes on a message sent without them")
	}
}

func TestPublishCopiesAttributes(t *testing.T) {
	b := newTestBroker(t, nil)
	x, y := mustCreate(t, b, "x", Config{}), mustCreate(t, b, "y", Config{})
	if _, err := b.CreateTopic("t", []string{"x", "y"}); err != nil {
		t.Fatal(err)
	}
	a := Attributes{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	pub, err := b.PublishWith("t", []byte("e"), SendOptions{Attributes: a})
	if err != nil || len(pub) != 2 || !maps.Equal(pub[0].Message.Attributes, a) {
		t.Fatalf("publish %+v %v", pub, err)
	}
	for _, q := range []*Queue{x, y} {
		if got := recv(t, q).Message.Attributes; !maps.Equal(got, a) {
			t.Fatalf("%s: %v", q.Name(), got)
		}
	}
}

func TestAttributeLimits(t *testing.T) {
	many := Attributes{}
	for i := range MaxAttributes + 1 {
		many[strings.Repeat("k", i+1)] = "v"
	}
	for name, a := range map[string]Attributes{
		"too many":     many,
		"bad name":     {"no spaces": "v"},
		"leading dot":  {".x": "v"},
		"long name":    {strings.Repeat("k", 65): "v"},
		"too big":      {"k": strings.Repeat("v", MaxAttributeBytes)},
		"invalid utf8": {"k": "\xff"},
	} {
		if err := ValidateAttributes(a); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := ValidateAttributes(Attributes{"traceparent": "x", "tracestate": "", "a.b_c-1": "y"}); err != nil {
		t.Fatal(err)
	}
	b := newTestBroker(t, nil)
	q := mustCreate(t, b, "q", Config{})
	if _, err := q.SendWith([]byte("x"), SendOptions{Attributes: Attributes{"bad name": "v"}}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("send accepted a bad attribute: %v", err)
	}
	if s := q.Stats(); s.Messages != 0 {
		t.Fatalf("rejected send enqueued: %+v", s)
	}
}
