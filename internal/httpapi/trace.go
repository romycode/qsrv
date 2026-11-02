package httpapi

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/romycode/qsrv/internal/queue"
)

// Spans follow the OpenTelemetry messaging conventions (still in
// development upstream): one span per message operation, named
// "<operation> <destination>".
//
//   - send, publish: PRODUCER. Its context becomes the message's creation
//     context (attribute traceparent) unless the producer already set one,
//     in which case the span links to it.
//   - receive: SERVER, recorded only when a message is delivered (empty
//     long-polls leave no span), from the start of the request, linked to
//     the message's creation context.
//   - ack, nack, dead_letter (settle), extend, replay: SERVER.
//
// The parent of every span is the trace context of the HTTP request headers.

const tracerName = "github.com/romycode/qsrv/internal/httpapi"

// msgProp reads and writes the creation context in message attributes:
// W3C traceparent and tracestate, no baggage.
var msgProp = propagation.TraceContext{}

const attrTraceparent = "traceparent"

func (s *Server) startSpan(r *http.Request, name string, kind trace.SpanKind, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx := s.prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	return s.tracer.Start(ctx, name, append(opts, trace.WithSpanKind(kind))...)
}

// msgAttrs are the attributes every messaging span has. dest is the full
// name (tenant/name).
func msgAttrs(opName, opType, dest string) []attribute.KeyValue {
	kv := []attribute.KeyValue{
		attribute.String("messaging.system", "qsrv"),
		attribute.String("messaging.operation.name", opName),
		attribute.String("messaging.destination.name", dest),
	}
	if opType != "" {
		kv = append(kv, attribute.String("messaging.operation.type", opType))
	}
	if tenant, _, ok := strings.Cut(dest, "/"); ok {
		kv = append(kv, attribute.String("qsrv.tenant", tenant))
	}
	return kv
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// messageLinks links to the creation context in attrs, if any.
func messageLinks(attrs map[string]string) []trace.Link {
	if attrs[attrTraceparent] == "" {
		return nil
	}
	sc := trace.SpanContextFromContext(msgProp.Extract(context.Background(), propagation.MapCarrier(attrs)))
	if !sc.IsValid() {
		return nil
	}
	return []trace.Link{{SpanContext: sc}}
}

// withCreationContext returns attrs with the span context of ctx as the
// message's creation context. Attributes that already carry one, or have no
// room for it, are returned as they are; so is everything when ctx has no
// valid span context (tracing off and no trace context from the client).
func withCreationContext(ctx context.Context, attrs map[string]string) map[string]string {
	if attrs[attrTraceparent] != "" || len(attrs) > queue.MaxAttributes-2 {
		return attrs
	}
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return attrs
	}
	out := make(map[string]string, len(attrs)+2)
	for k, v := range attrs {
		out[k] = v
	}
	msgProp.Inject(ctx, propagation.MapCarrier(out))
	return out
}
