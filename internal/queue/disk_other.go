//go:build !unix

package queue

import "errors"

func freeDisk(string) (uint64, error) { return 0, errors.New("free disk space: unsupported platform") }
