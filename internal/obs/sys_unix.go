//go:build unix

package obs

import (
	"bytes"
	"errors"
	"os"
	"strconv"
	"syscall"
)

func diskUsage(path string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	return st.Bavail * uint64(st.Bsize), st.Blocks * uint64(st.Bsize), nil
}

// residentMemory reads RSS from /proc (Linux); errors elsewhere.
func residentMemory() (uint64, error) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	f := bytes.Fields(b)
	if len(f) < 2 {
		return 0, errors.New("bad statm")
	}
	pages, err := strconv.ParseUint(string(f[1]), 10, 64)
	return pages * uint64(os.Getpagesize()), err
}
