package httpapi_test

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// Keep test and benchmark output clean (benchstat parses it). Set
// QSRV_TEST_LOG=1 to see the server logs.
func TestMain(m *testing.M) {
	if os.Getenv("QSRV_TEST_LOG") == "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	os.Exit(m.Run())
}
