package queue

// WasmCacheEnv overrides where the WASM driver caches SQLite compiled to
// native code (default: dataDir/.wasmcache). Ignored by the cgo driver.
//
// Tests point it at a shared dir: each test uses a fresh data dir, and
// compiling SQLite under -race takes ~15s per test binary instead of ~1.5s.
const WasmCacheEnv = "QSRV_WASM_CACHE_DIR"
