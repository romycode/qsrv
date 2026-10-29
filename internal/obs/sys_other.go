//go:build !unix

package obs

import "errors"

var errUnsupported = errors.New("unsupported platform")

func diskUsage(string) (uint64, uint64, error) { return 0, 0, errUnsupported }
func residentMemory() (uint64, error)          { return 0, errUnsupported }
