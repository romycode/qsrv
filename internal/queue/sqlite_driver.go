//go:build !sqlite_cgo

package queue

// Default driver: ncruces/go-sqlite3 — SQLite compiled to WASM, run by the
// wazero runtime (pure Go, no cgo). Cross-compiles anywhere with
// CGO_ENABLED=0. For the cgo driver build with -tags sqlite_cgo.

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/ncruces/go-sqlite3"
	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

var configureOnce sync.Once

// configureDriver persists wazero's native compilation of SQLite under
// dataDir/.wasmcache (or $QSRV_WASM_CACHE_DIR): first start ~1s, later starts
// ~tens of ms. Must run before the first connection is opened; only the
// first call has effect.
func configureDriver(dataDir string) {
	configureOnce.Do(func() {
		cfg := wazero.NewRuntimeConfig().
			WithMemoryLimitPages(4096). // 256MB per connection, ncruces' 64-bit default
			WithCoreFeatures(api.CoreFeaturesV2)
		dir := os.Getenv(WasmCacheEnv)
		if dir == "" {
			dir = filepath.Join(dataDir, ".wasmcache")
		}
		cache, err := wazero.NewCompilationCacheWithDir(dir)
		if err != nil {
			slog.Warn("wasm compilation cache disabled", "err", err)
		} else {
			cfg = cfg.WithCompilationCache(cache)
		}
		sqlite3.RuntimeConfig = cfg
	})
}

const driverName = "sqlite3"

const DriverInfo = "ncruces/go-sqlite3 (wasm, pure Go)"

func dsn(path string, readOnly, noSync bool) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	if readOnly {
		q.Set("mode", "ro")
	} else {
		q.Add("_pragma", "journal_mode(wal)")
		q.Set("_txlock", "immediate")
		if noSync {
			q.Add("_pragma", "synchronous(off)")
		} else {
			q.Add("_pragma", "synchronous(full)") // fsync on every commit; group commit amortizes it
		}
	}
	return fmt.Sprintf("file:%s?%s", path, q.Encode())
}
