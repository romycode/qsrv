package queue

import (
	"path/filepath"
	"testing"
)

func TestPragmasApplied(t *testing.T) {
	s, err := openStore(filepath.Join(t.TempDir(), "x.db"), false, func(err error) { panic(err) })
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var jm string
	var sync, bt int
	s.w.QueryRow(`PRAGMA journal_mode`).Scan(&jm)
	s.w.QueryRow(`PRAGMA synchronous`).Scan(&sync)
	s.w.QueryRow(`PRAGMA busy_timeout`).Scan(&bt)
	if jm != "wal" || sync != 2 || bt != 5000 {
		t.Fatalf("driver %s: journal_mode=%s synchronous=%d (want 2=FULL) busy_timeout=%d", DriverInfo, jm, sync, bt)
	}
	if _, err := s.r.Exec(`CREATE TABLE nope(x)`); err == nil {
		t.Fatal("reader pool is writable")
	}
	t.Logf("%s: journal_mode=%s synchronous=FULL busy_timeout=%d, reader read-only", DriverInfo, jm, bt)
}
