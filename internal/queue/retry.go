package queue

import (
	"math"
	"time"
)

// RetryPolicy decides what happens after a failed delivery (nack or lease expiry).
//
// Attempts counts deliveries, so MaxAttempts=3 means: 1 delivery + 2 retries.
// MaxAttempts=0 means retry forever (dangerous: poison messages loop forever).
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64 // <1 is treated as 1 (constant backoff)
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2}
}

// Exhausted reports whether a message with this many deliveries must be dead-lettered.
func (p RetryPolicy) Exhausted(attempts int) bool {
	return p.MaxAttempts > 0 && attempts >= p.MaxAttempts
}

// Delay is the backoff before the next delivery, after `attempts` deliveries.
func (p RetryPolicy) Delay(attempts int) time.Duration {
	if p.InitialBackoff <= 0 || attempts < 1 {
		return 0
	}
	m := p.Multiplier
	if m < 1 {
		m = 1
	}
	d := float64(p.InitialBackoff) * math.Pow(m, float64(attempts-1))
	if p.MaxBackoff > 0 && d > float64(p.MaxBackoff) {
		return p.MaxBackoff
	}
	if d > float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(d)
}
