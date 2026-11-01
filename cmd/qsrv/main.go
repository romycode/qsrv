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
	"strconv"
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
		keysFile    = flag.String("api-keys-file", os.Getenv("QSRV_API_KEYS_FILE"), "file with one tenant:name:key entry per line (SIGHUP reloads it)")
		insecure    = flag.Bool("insecure-no-auth", false, "disable authentication (local development only)")
		tlsCert     = flag.String("tls-cert", os.Getenv("QSRV_TLS_CERT"), "TLS certificate (PEM)")
		tlsKey      = flag.String("tls-key", os.Getenv("QSRV_TLS_KEY"), "TLS private key (PEM)")
		maxMsg      = flag.Int64("max-message-bytes", 256<<10, "max message size")
		backupDir   = flag.String("backup-dir", os.Getenv("QSRV_BACKUP_DIR"), "take periodic snapshots of every tenant's database into this directory (empty disables; use another disk)")
		backupEvery = flag.Duration("backup-every", durationEnv("QSRV_BACKUP_EVERY", time.Hour), "interval between snapshots")
		backupKeep  = flag.Int("backup-keep", intEnv("QSRV_BACKUP_KEEP", 24), "snapshots to keep (oldest are deleted)")
		adminUI     = flag.Bool("admin", envOr("QSRV_ADMIN", "true") != "false", "serve the read-only admin UI at /admin/ (sign in with an API key)")
		minFree     = flag.String("min-free-disk", envOr("QSRV_MIN_FREE_DISK", "1GiB"), "below this free space on the data dir, refuse sends with 503 storage_full (e.g. 500MiB, 2GiB; 0 disables)")
		janitor     = flag.Duration("retention-check", time.Minute, "how often messages past their queue's retention are expired")
		simLatency  = flag.Duration("debug-simulated-sync-latency", 0, "BENCHMARK ONLY: extra latency per commit to emulate slow disks")
		metricsAddr = flag.String("metrics-addr", envOr("QSRV_METRICS_ADDR", "127.0.0.1:9464"), "Prometheus /metrics listen address (no auth: keep it private; empty disables)")
		logLevel    = flag.String("log-level", envOr("QSRV_LOG_LEVEL", "info"), "debug|info|warn|error")
		restoreT    = flag.String("restore-tenant", "", "replace this tenant's queues, topics and messages with their state in the snapshot given by -from, then exit (qsrv stopped)")
		restoreFrom = flag.String("from", "", "snapshot directory for -restore-tenant (a directory inside -backup-dir)")
		healthcheck = flag.Bool("healthcheck", false, "probe GET /healthz on -addr and exit 0 if healthy (for container health checks)")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s (SQLite driver: %s)\n", version, queue.DriverInfo)
		return nil
	}
	if *healthcheck {
		return probe(*addr, *tlsCert != "")
	}

	if *restoreT != "" {
		if *restoreFrom == "" {
			return errors.New("-restore-tenant needs -from SNAPSHOT_DIR")
		}
		lock, err := lockDir(*dataDir)
		if err != nil {
			return fmt.Errorf("%w (stop qsrv before restoring)", err)
		}
		defer lock.Close()
		r, err := queue.RestoreTenant(*dataDir, *restoreFrom, *restoreT)
		if err != nil {
			return err
		}
		fmt.Printf("restored tenant %q from %s: %d queues, %d topics, %d messages (live and history)\n",
			*restoreT, *restoreFrom, r.Queues, r.Topics, r.Messages)
		return nil
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

	minFreeBytes, err := parseBytes(*minFree)
	if err != nil {
		return fmt.Errorf("-min-free-disk: %w", err)
	}

	lock, err := lockDir(*dataDir)
	if err != nil {
		return err
	}
	defer lock.Close()

	o := obs.New()
	broker, err := queue.Open(*dataDir, queue.Options{
		JanitorEvery: *janitor, SimulatedSyncLatency: *simLatency, Hooks: o.Hooks(), MinFreeDisk: minFreeBytes,
		Backup: queue.BackupOptions{Dir: *backupDir, Every: *backupEvery, Keep: *backupKeep},
	})
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
		Handler:           o.HTTPMiddleware(httpapi.New(broker, httpapi.Options{Auth: auth, MaxMessageBytes: *maxMsg, Admin: *adminUI, Version: version})),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      httpapi.MaxWait + 10*time.Second, // must exceed long-poll wait
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reloadKeysOnHUP(ctx, auth, *keysFile)

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
		slog.Info("listening", "version", version, "driver", queue.DriverInfo, "addr", *addr, "tls", *tlsCert != "", "auth", auth != nil, "data_dir", *dataDir, "keys", authLen(auth))
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

// loadKeys reads the key entries from the keys file and QSRV_API_KEYS.
func loadKeys(keysFile string) ([]httpapi.KeyEntry, error) {
	var keys []httpapi.KeyEntry
	if keysFile != "" {
		k, err := httpapi.LoadKeysFile(keysFile)
		if err != nil {
			return nil, fmt.Errorf("api keys file %s: %w", keysFile, err)
		}
		keys = append(keys, k...)
	}
	if env := os.Getenv("QSRV_API_KEYS"); env != "" {
		k, err := httpapi.ParseKeys(strings.NewReader(env))
		if err != nil {
			return nil, fmt.Errorf("QSRV_API_KEYS: %w", err)
		}
		keys = append(keys, k...)
	}
	if len(keys) == 0 {
		return nil, errors.New("no API keys: set QSRV_API_KEYS or -api-keys-file with tenant:name:key entries (or -insecure-no-auth for local dev)")
	}
	return keys, nil
}

func buildAuth(keysFile string, insecure bool) (*httpapi.Authenticator, error) {
	if insecure {
		slog.Warn("AUTHENTICATION DISABLED (-insecure-no-auth): every request is an admin")
		return nil, nil
	}
	keys, err := loadKeys(keysFile)
	if err != nil {
		return nil, err
	}
	return httpapi.NewAuthenticator(keys)
}

// reloadKeysOnHUP re-reads the keys on SIGHUP. A bad file keeps the current keys.
func reloadKeysOnHUP(ctx context.Context, auth *httpapi.Authenticator, keysFile string) {
	if auth == nil {
		return
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		defer signal.Stop(hup)
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				keys, err := loadKeys(keysFile)
				if err == nil {
					err = auth.Replace(keys)
				}
				if err != nil {
					slog.Error("reload api keys failed, keeping the current ones", "err", err)
					continue
				}
				slog.Info("api keys reloaded", "keys", auth.Len(), "tenants", tenantCount(keys))
			}
		}
	}()
}

func tenantCount(keys []httpapi.KeyEntry) int {
	t := map[string]bool{}
	for _, k := range keys {
		t[k.Tenant] = true
	}
	return len(t)
}

// parseBytes parses sizes like "1GiB", "500MiB", "64KiB", "1GB" or plain bytes.
func parseBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   uint64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}}
	mult := uint64(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q (use e.g. 500MiB, 2GiB or bytes)", s)
	}
	return uint64(n * float64(mult)), nil
}

func durationEnv(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		slog.Warn("ignoring invalid duration", "env", k, "value", v)
	}
	return def
}

func intEnv(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
		slog.Warn("ignoring invalid number", "env", k, "value", v)
	}
	return def
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

func authLen(a *httpapi.Authenticator) int {
	if a == nil {
		return 0
	}
	return a.Len()
}
