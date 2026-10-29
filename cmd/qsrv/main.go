// Command qsrv runs the queue server.
package main

import (
	"context"
	cryptotls "crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/obs"
	"github.com/romycode/qsrv/internal/queue"
)

// version is set at build time: -ldflags "-X main.version=1.2.3"
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr        = flag.String("addr", envOr("QSRV_ADDR", ":8080"), "listen address")
		dataDir     = flag.String("data-dir", envOr("QSRV_DATA_DIR", "./data"), "directory for qsrv.db (SQLite)")
		keysFile    = flag.String("api-keys-file", os.Getenv("QSRV_API_KEYS_FILE"), "file with one API key per line")
		insecure    = flag.Bool("insecure-no-auth", false, "disable authentication (local development only)")
		tlsCert     = flag.String("tls-cert", os.Getenv("QSRV_TLS_CERT"), "TLS certificate (PEM)")
		tlsKey      = flag.String("tls-key", os.Getenv("QSRV_TLS_KEY"), "TLS private key (PEM)")
		maxMsg      = flag.Int64("max-message-bytes", 256<<10, "max message size")
		janitor     = flag.Duration("retention-check", time.Minute, "how often expired history is purged")
		simLatency  = flag.Duration("debug-simulated-sync-latency", 0, "BENCHMARK ONLY: extra latency per commit to emulate slow disks")
		metricsAddr = flag.String("metrics-addr", envOr("QSRV_METRICS_ADDR", "127.0.0.1:9464"), "Prometheus /metrics listen address (no auth: keep it private; empty disables)")
		logLevel    = flag.String("log-level", envOr("QSRV_LOG_LEVEL", "info"), "debug|info|warn|error")
		healthcheck = flag.Bool("healthcheck", false, "probe GET /healthz on -addr and exit 0 if healthy (for container health checks)")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}
	if *healthcheck {
		return probe(*addr, *tlsCert != "")
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))

	auth, err := buildAuth(*keysFile, *insecure)
	if err != nil {
		return err
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return errors.New("-tls-cert and -tls-key must be set together")
	}

	lock, err := lockDir(*dataDir)
	if err != nil {
		return err
	}
	defer lock.Close()

	o := obs.New()
	broker, err := queue.Open(*dataDir, queue.Options{JanitorEvery: *janitor, SimulatedSyncLatency: *simLatency, Hooks: o.Hooks()})
	if err != nil {
		return err
	}
	o.RegisterBroker(broker, *dataDir, version)

	// Cancelled on shutdown so long-polls return immediately instead of
	// holding Shutdown for up to MaxWait.
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	if *simLatency > 0 {
		slog.Warn("SIMULATED SYNC LATENCY ENABLED: benchmark mode", "latency", *simLatency)
	}
	srv := &http.Server{
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		Addr:              *addr,
		Handler:           o.HTTPMiddleware(httpapi.New(broker, httpapi.Options{Auth: auth, MaxMessageBytes: *maxMsg})),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      httpapi.MaxWait + 10*time.Second, // must exceed long-poll wait
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 2)
	var msrv *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", o.Handler())
		msrv = &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			slog.Info("metrics listening", "addr", *metricsAddr)
			errc <- msrv.ListenAndServe()
		}()
	}
	go func() {
		slog.Info("listening", "version", version, "driver", queue.DriverInfo, "addr", *addr, "tls", *tlsCert != "", "auth", auth != nil, "data_dir", *dataDir)
		if *tlsCert != "" {
			errc <- srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			errc <- srv.ListenAndServe()
		}
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			broker.Close()
			return err
		}
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	cancelBase()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http shutdown", "err", err)
	}
	if msrv != nil {
		msrv.Shutdown(shutdownCtx)
	}
	return broker.Close() // commits pending writes
}

func buildAuth(keysFile string, insecure bool) (*httpapi.Authenticator, error) {
	if insecure {
		slog.Warn("AUTHENTICATION DISABLED (-insecure-no-auth)")
		return nil, nil
	}
	var keys []string
	if keysFile != "" {
		k, err := httpapi.LoadKeysFile(keysFile)
		if err != nil {
			return nil, fmt.Errorf("read api keys: %w", err)
		}
		keys = append(keys, k...)
	}
	if env := os.Getenv("QSRV_API_KEYS"); env != "" {
		keys = append(keys, strings.Split(env, ",")...)
	}
	if len(keys) == 0 {
		return nil, errors.New("no API keys: set QSRV_API_KEYS or -api-keys-file (or -insecure-no-auth for local dev)")
	}
	return httpapi.NewAuthenticator(keys)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// probe checks /healthz of the server listening on addr (images without a
// shell or curl use it as their health check). With TLS it skips certificate
// verification: it only talks to localhost.
func probe(addr string, tls bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	scheme := "http"
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tls {
		scheme = "https"
		tr.TLSClientConfig = &cryptotls.Config{InsecureSkipVerify: true} //nolint:gosec // localhost probe
	}
	hc := &http.Client{Timeout: 3 * time.Second, Transport: tr}
	resp, err := hc.Get(scheme + "://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	return nil
}
