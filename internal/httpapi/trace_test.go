package httpapi_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

func newTracedServer(t *testing.T) (*httptest.Server, *tracetest.SpanRecorder) {
	t.Helper()
	auth, err := httpapi.NewAuthenticator([]httpapi.KeyEntry{entry("test", "suite", key)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := queue.Open(t.TempDir(), queue.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	srv := httptest.NewServer(httpapi.New(b, httpapi.Options{
		Auth: auth, MaxMessageBytes: 1024,
		TracerProvider: tp, Propagator: propagation.TraceContext{},
	}))
	t.Cleanup(func() { srv.Close(); b.Close(); tp.Shutdown(context.Background()) })
	return srv, sr
}

func spanNamed(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var names []string
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return s
		}
		names = append(names, s.Name())
	}
	t.Fatalf("no span %q; have %v", name, names)
	return nil
}

func attr(s sdktrace.ReadOnlySpan, k string) attribute.Value {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == k {
			return kv.Value
		}
	}
	return attribute.Value{}
}

func linksTo(s sdktrace.ReadOnlySpan, sc trace.SpanContext) bool {
	for _, l := range s.Links() {
		if l.SpanContext.TraceID() == sc.TraceID() && l.SpanContext.SpanID() == sc.SpanID() {
			return true
		}
	}
	return false
}

// A traced producer: the message carries its span as the creation context,
// qsrv's spans join its trace through the request headers, and receive
// links back to it.
func TestTracingWithTracedProducer(t *testing.T) {
	srv, sr := newTracedServer(t)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{}) // what an instrumented app does
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	c := client.New(srv.URL, key)
	bg := context.Background()
	c.CreateQueue(bg, "orders", api.QueueConfig{})

	producer := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(bg, producer)
	m, err := c.Send(ctx, "orders", []byte(`{"id":1}`), "")
	if err != nil {
		t.Fatal(err)
	}
	if got := client.MessageSpanContext(m); got.TraceID() != producer.TraceID() || got.SpanID() != producer.SpanID() {
		t.Fatalf("creation context %v, want the producer's span", m.Attributes)
	}

	send := spanNamed(t, sr, "send test/orders")
	if send.SpanKind() != trace.SpanKindProducer || send.Parent().SpanID() != producer.SpanID() {
		t.Fatalf("send span: kind %v parent %v", send.SpanKind(), send.Parent())
	}
	if !linksTo(send, producer) {
		t.Fatal("send span doesn't link to the producer's creation context")
	}
	if attr(send, "messaging.message.id").AsString() != m.ID || attr(send, "messaging.system").AsString() != "qsrv" ||
		attr(send, "qsrv.tenant").AsString() != "test" {
		t.Fatalf("send attributes %v", send.Attributes())
	}

	d, err := c.Receive(bg, "orders", 0)
	if err != nil || d == nil {
		t.Fatalf("receive %v %v", d, err)
	}
	recv := spanNamed(t, sr, "receive test/orders")
	if !linksTo(recv, producer) || attr(recv, "messaging.operation.type").AsString() != "receive" {
		t.Fatalf("receive span links %v attrs %v", recv.Links(), recv.Attributes())
	}
	if err := c.Ack(bg, "orders", d.LeaseID); err != nil {
		t.Fatal(err)
	}
	if ack := spanNamed(t, sr, "ack test/orders"); attr(ack, "messaging.operation.type").AsString() != "settle" {
		t.Fatalf("ack attributes %v", ack.Attributes())
	}
}

// An untraced producer: qsrv's send span becomes the creation context.
// Empty long-polls record no span.
func TestTracingWithUntracedProducer(t *testing.T) {
	srv, sr := newTracedServer(t)
	c := client.New(srv.URL, key)
	ctx := context.Background()
	c.CreateQueue(ctx, "q", api.QueueConfig{})
	c.CreateQueue(ctx, "b", api.QueueConfig{})
	c.CreateTopic(ctx, "t", "b")

	if d, _ := c.Receive(ctx, "q", 50*time.Millisecond); d != nil {
		t.Fatal("queue not empty")
	}
	m, err := c.Send(ctx, "q", []byte("x"), "")
	if err != nil {
		t.Fatal(err)
	}
	send := spanNamed(t, sr, "send test/q")
	if sc := client.MessageSpanContext(m); sc.SpanID() != send.SpanContext().SpanID() {
		t.Fatalf("creation context %v, want qsrv's send span %v", m.Attributes, send.SpanContext().SpanID())
	}
	if len(send.Links()) != 0 {
		t.Fatalf("send span links %v", send.Links())
	}
	for _, s := range sr.Ended() {
		if s.Name() == "receive test/q" {
			t.Fatal("an empty receive recorded a span")
		}
	}

	pub, err := c.Publish(ctx, "t", []byte("e"), client.SendOptions{})
	if err != nil || len(pub) != 1 {
		t.Fatalf("publish %v %v", pub, err)
	}
	ps := spanNamed(t, sr, "publish test/t")
	if attr(ps, "messaging.batch.message_count").AsInt64() != 1 {
		t.Fatalf("publish attributes %v", ps.Attributes())
	}
	if d, _ := c.Receive(ctx, "b", 0); d == nil || client.MessageSpanContext(d.Message).SpanID() != ps.SpanContext().SpanID() {
		t.Fatalf("published copy doesn't carry the publish span: %+v", d)
	}
}

// Without tracing (the default), messages from an untraced client get no
// trace context.
func TestNoTracingNoTraceContext(t *testing.T) {
	srv := newServer(t)
	c := client.New(srv.URL, key)
	ctx := context.Background()
	c.CreateQueue(ctx, "q", api.QueueConfig{})
	m, err := c.Send(ctx, "q", []byte("x"), "")
	if err != nil || m.Attributes != nil {
		t.Fatalf("send %+v %v", m, err)
	}
}
