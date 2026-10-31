package queue

import (
	"errors"
	"log/slog"
	"time"
)

// Free-space guard. A commit that fails (disk full) makes qsrv exit to keep
// memory and disk consistent; restarting into a full disk would fail again.
// So below MinFreeDisk the broker refuses what adds data (sends, publishes,
// replays, new queues and topics) with ErrStorageFull, and keeps serving
// what drains it: receive, ack, nack, extend, deletes and the retention
// purge. It accepts writes again once the space is back.

// ErrStorageFull is returned for writes that add data while free disk space
// is below Options.MinFreeDisk.
var ErrStorageFull = errors.New("queue: not enough free disk space, retry later")

const diskCheckEvery = 2 * time.Second

func (b *Broker) watchDisk(min uint64, free func(string) (uint64, error)) {
	if min == 0 {
		return
	}
	if free == nil {
		free = freeDisk
	}
	check := func() {
		n, err := free(b.dataDir)
		if err != nil {
			slog.Warn("free disk check failed; the guard is not enforced", "err", err)
			return
		}
		low := n < min
		if was := b.diskLow.Swap(low); was != low {
			if low {
				slog.Error("free disk space below the minimum: refusing sends, publishes and replays",
					"free_bytes", n, "min_free_bytes", min)
			} else {
				slog.Info("free disk space recovered: accepting writes again", "free_bytes", n)
			}
		}
	}
	b.diskCheck = check
	check() // before serving: a restart into a full disk refuses writes at once
	go func() {
		t := time.NewTicker(diskCheckEvery)
		defer t.Stop()
		for {
			select {
			case <-b.stop:
				return
			case <-t.C:
				check()
			}
		}
	}()
}

// StorageLow reports whether writes are refused for lack of disk space.
func (b *Broker) StorageLow() bool { return b.diskLow.Load() }

func (b *Broker) checkDisk() error {
	if b.diskLow.Load() {
		return ErrStorageFull
	}
	return nil
}
