// Package testutil holds the shared TestMain for packages that open brokers.
package testutil

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// Main runs the tests with:
//   - slog silenced, so test and benchmark output stays parseable
//     (QSRV_TEST_LOG=1 shows the server logs);
//   - a shared cache for SQLite compiled from WASM, unless
//     QSRV_WASM_CACHE_DIR is already set. Without it each test binary
//     recompiles SQLite: ~1.5s, or ~15s under -race.
func Main(m *testing.M) {
	if os.Getenv("QSRV_TEST_LOG") == "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	if os.Getenv("QSRV_WASM_CACHE_DIR") == "" {
		if dir := sharedWasmCache(); dir != "" {
			os.Setenv("QSRV_WASM_CACHE_DIR", dir)
		}
	}
	os.Exit(m.Run())
}

// sharedWasmCache returns a per-user cache dir keyed by go.sum. Test binaries
// carry no dependency versions, so wazero can't tell its own versions apart
// ("dev"); keying on go.sum makes a wazero or SQLite upgrade start a fresh
// cache instead of loading native code built by another version.
func sharedWasmCache() string {
	base, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if sum, err := os.ReadFile(filepath.Join(dir, "go.sum")); err == nil {
			h := sha256.Sum256(sum)
			return filepath.Join(base, "qsrv-test", "wasm-"+hex.EncodeToString(h[:6]))
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
