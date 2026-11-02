// Package tracing sets up OpenTelemetry tracing for qsrv from the standard
// OTEL_* environment variables.
//
// Traces are exported with OTLP over HTTP when an endpoint is configured
// (OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT);
// otherwise qsrv records no spans. The W3C trace context and baggage
// propagators are always installed, so a traceparent sent by a client is
// carried into the messages even when qsrv itself doesn't export.
package tracing

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Enabled reports whether the environment asks for traces to be exported.
func Enabled() bool {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return false
	}
	if strings.EqualFold(os.Getenv("OTEL_TRACES_EXPORTER"), "none") {
		return false
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Setup installs the propagators and, when Enabled, a tracer provider that
// exports over OTLP/HTTP. The sampler comes from OTEL_TRACES_SAMPLER (default
// parentbased_always_on); service.name defaults to "qsrv" and can be
// overridden with OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES. The
// returned function flushes and stops the exporter; call it on shutdown.
func Setup(ctx context.Context, version string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if !Enabled() {
		return func(context.Context) error { return nil }, nil
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { slog.Warn("opentelemetry", "err", err) }))

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", "qsrv"),
			attribute.String("service.version", version),
		),
		resource.WithTelemetrySDK(),
		resource.WithFromEnv(), // last: OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES win
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, err
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	slog.Info("tracing enabled", "exporter", "otlp/http")
	return tp.Shutdown, nil
}
