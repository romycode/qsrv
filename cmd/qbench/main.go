// Command qbench load-tests a running qsrv over HTTP and reports throughput
// and latency percentiles for send, receive, ack and end-to-end delay.
//
//	qbench -producers 16 -consumers 16 -duration 30s -size 1024
//	qbench -rate 50 -duration 60s        # fixed load: latency under normal traffic
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

type samples struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *samples) add(local []time.Duration) {
	s.mu.Lock()
	s.d = append(s.d, local...)
	s.mu.Unlock()
}

func (s *samples) row(name string, elapsed time.Duration) string {
	if len(s.d) == 0 {
		return fmt.Sprintf("%-9s %10s", name, "-")
	}
	slices.Sort(s.d)
	p := func(q float64) time.Duration { return s.d[min(int(q*float64(len(s.d))), len(s.d)-1)] }
	return fmt.Sprintf("%-9s %10.0f %9s %9s %9s %9s %9s",
		name, float64(len(s.d))/elapsed.Seconds(), r(p(0.50)), r(p(0.90)), r(p(0.99)), r(p(0.999)), r(s.d[len(s.d)-1]))
}

func r(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return strconv.FormatFloat(float64(d)/float64(time.Microsecond), 'f', 0, 64) + "µs"
	case d < time.Second:
		return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 2, 64) + "ms"
	}
	return d.Round(time.Millisecond).String()
}

func main() {
	addr := flag.String("addr", envOr("QSRV_ADDR", "http://localhost:8080"), "server URL")
	key := flag.String("api-key", os.Getenv("QSRV_API_KEY"), "API key")
	producers := flag.Int("producers", 8, "concurrent senders")
	consumers := flag.Int("consumers", 8, "concurrent receive+ack workers")
	duration := flag.Duration("duration", 20*time.Second, "test duration")
	size := flag.Int("size", 1024, "message body bytes (min 8)")
	groups := flag.Int("groups", 0, "spread messages over N ordering groups (0 = no groups)")
	rate := flag.Float64("rate", 0, "total send rate msg/s (0 = as fast as possible)")
	keep := flag.Bool("keep", false, "keep the benchmark queue afterwards")
	tenants := flag.Int("tenants", 0, "spread the load over N tenants, one queue each, workers round-robin (needs an admin key, tenant *)")
	flag.Parse()

	ctx := context.Background()
	c := client.New(*addr, *key)
	base := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	qnames := []string{base}
	if *tenants > 0 {
		qnames = qnames[:0]
		for i := range *tenants {
			qnames = append(qnames, fmt.Sprintf("bench%03d/%s", i, base))
		}
	}
	for _, q := range qnames {
		if _, err := c.CreateQueue(ctx, q, api.QueueConfig{
			VisibilityTimeout: api.Duration(30 * time.Second),
			Retry:             &api.RetryPolicy{MaxAttempts: 5},
			Persistent:        true, // no expiry pass during the run
		}); err != nil {
			fatal(err)
		}
	}
	if !*keep {
		defer func() {
			for _, q := range qnames {
				c.DeleteQueue(context.Background(), q)
			}
		}()
	}

	fmt.Printf("queues=%d (%s) producers=%d consumers=%d size=%dB groups=%d rate=%v duration=%v\n",
		len(qnames), qnames[0], *producers, *consumers, *size, *groups, rateStr(*rate), *duration)

	var sendS, recvS, ackS, e2eS samples
	var sendErr, recvErr, ackErr, empty atomic.Int64
	stop := make(chan struct{})
	start := time.Now()
	var wg sync.WaitGroup

	for p := range *producers {
		qname := qnames[p%len(qnames)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := make([]byte, max(*size, 8))
			var tick <-chan time.Time
			if *rate > 0 {
				t := time.NewTicker(time.Duration(float64(time.Second) * float64(*producers) / *rate))
				defer t.Stop()
				tick = t.C
			}
			var local []time.Duration
			defer func() { sendS.add(local) }()
			for i := 0; ; i++ {
				if tick != nil {
					select {
					case <-stop:
						return
					case <-tick:
					}
				} else {
					select {
					case <-stop:
						return
					default:
					}
				}
				g := ""
				if *groups > 0 {
					g = "g" + strconv.Itoa((p*1_000_003+i)%*groups)
				}
				t0 := time.Now()
				binary.LittleEndian.PutUint64(body, uint64(t0.UnixNano()))
				if _, err := c.Send(ctx, qname, body, g); err != nil {
					sendErr.Add(1)
					continue
				}
				local = append(local, time.Since(t0))
			}
		}()
	}

	for ci := range *consumers {
		qname := qnames[ci%len(qnames)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lr, la, le []time.Duration
			defer func() { recvS.add(lr); ackS.add(la); e2eS.add(le) }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				t0 := time.Now()
				d, err := c.Receive(rctx, qname, time.Second)
				cancel()
				if err != nil {
					if !errors.Is(err, context.DeadlineExceeded) {
						recvErr.Add(1)
					}
					continue
				}
				if d == nil {
					empty.Add(1)
					continue
				}
				lr = append(lr, time.Since(t0))
				t1 := time.Now()
				if err := c.Ack(ctx, qname, d.LeaseID); err != nil {
					ackErr.Add(1)
					continue
				}
				now := time.Now()
				la = append(la, now.Sub(t1))
				if b := d.Message.Bytes(); len(b) >= 8 {
					le = append(le, now.Sub(time.Unix(0, int64(binary.LittleEndian.Uint64(b)))))
				}
			}
		}()
	}

	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	backlog := 0
	for _, q := range qnames {
		info, _ := c.GetQueue(ctx, q)
		backlog += info.Stats.Messages
	}
	fmt.Printf("\n%-9s %10s %9s %9s %9s %9s %9s\n", "op", "ops/s", "p50", "p90", "p99", "p99.9", "max")
	fmt.Println(sendS.row("send", elapsed))
	fmt.Println(recvS.row("receive", elapsed))
	fmt.Println(ackS.row("ack", elapsed))
	fmt.Println(e2eS.row("end2end", elapsed))
	fmt.Printf("\nerrors: send=%d receive=%d ack=%d | empty polls=%d | backlog at end=%d\n",
		sendErr.Load(), recvErr.Load(), ackErr.Load(), empty.Load(), backlog)
	fmt.Println("end2end = send start -> ack done (includes queueing time when consumers lag)")
	if sendErr.Load()+recvErr.Load()+ackErr.Load() > 0 {
		os.Exit(1) // results above are still printed; CI treats errors as failure
	}
}

func rateStr(r float64) string {
	if r == 0 {
		return "max"
	}
	return strconv.FormatFloat(r, 'f', -1, 64) + "/s"
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
