//go:build sqlite_cgo

package queue

// cgo driver (mattn/go-sqlite3). Opt-in: go build -tags sqlite_cgo
// Requires CGO_ENABLED=1 and a C toolchain for the target platform.

import (
	"fmt"
	"net/url"

	_ "github.com/mattn/go-sqlite3"
)

const driverName = "sqlite3"

const DriverInfo = "mattn/go-sqlite3 (cgo)"

func dsn(path string, readOnly, noSync bool) string {
	q := url.Values{}
	q.Set("_busy_timeout", "5000")
	if readOnly {
		q.Set("mode", "ro")
	} else {
		q.Set("_journal_mode", "WAL")
		q.Set("_txlock", "immediate")
		if noSync {
			q.Set("_synchronous", "OFF")
		} else {
			q.Set("_synchronous", "FULL") // fsync on every commit; group commit amortizes it
		}
	}
	return fmt.Sprintf("file:%s?%s", path, q.Encode())
}

func configureDriver(string) {}
