// Command qdevload generates continuous, realistic traffic against a qsrv
// for the development stack (deploy/dev), so every dashboard panel and alert
// has something to show: two tenants, queues with retries and a DLQ,
// ordering groups, delayed sends, a topic fanning out to two queues,
// consumer failures (nack, dead letter, abandoned leases) and a consumer
// that pauses now and then so a backlog builds up and drains.
//
//	qdevload -rate 5        # msg/s per tenant (default), Ctrl-C to stop
//
// Needs an admin key (tenant *): it works across tenants. Not for
// production: it creates and fills queues named in its own tenants.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

func main() {
	addr := flag.String("addr", envOr("QSRV_ADDR", "http://localhost:8080"), "server URL")
	key := flag.String("api-key", os.Getenv("QSRV_API_KEY"), "admin API key (tenant *)")
	tenants := flag.String("tenants", envOr("QDEVLOAD_TENANTS", "acme,globex"), "comma-separated tenants to generate traffic in")
	rate := flag.Float64("rate", floatEnv("QDEVLOAD_RATE", 5), "average messages/s per tenant (varies ±50% over 10 minutes)")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c := client.New(*addr, *key)

	waitHealthy(ctx, c)
	var wg sync.WaitGroup
	for _, t := range strings.Split(*tenants, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if err := setup(ctx, c, t); err != nil {
			slog.Error("setup", "tenant", t, "err", err)
			os.Exit(1)
		}
		g := &gen{c: c, t: t, rate: *rate}
		g.start(ctx, &wg)
		slog.Info("generating traffic", "tenant", t, "rate", *rate)
	}
	<-ctx.Done()
	wg.Wait()
}

// setup creates the tenant's queues and topic; existing ones are kept.
func setup(ctx context.Context, c *client.Client, t string) error {
	queues := []struct {
		name string
		cfg  api.QueueConfig
	}{
		{"orders-dlq", api.QueueConfig{}},
		{"orders", api.QueueConfig{
			VisibilityTimeout: api.Duration(10 * time.Second),
			Retry:             &api.RetryPolicy{MaxAttempts: 3, InitialBackoff: api.Duration(time.Second), MaxBackoff: api.Duration(10 * time.Second), Multiplier: 2},
			DeadLetterQueue:   t + "/orders-dlq",
		}},
		{"emails", api.QueueConfig{MaxMessages: 5000}},
		{"billing", api.QueueConfig{}},
		{"audit", api.QueueConfig{}},
	}
	for _, q := range queues {
		if _, err := c.CreateQueue(ctx, t+"/"+q.name, q.cfg); err != nil && !exists(err) {
			return fmt.Errorf("queue %s: %w", q.name, err)
		}
	}
	if _, err := c.CreateTopic(ctx, t+"/events", t+"/billing", t+"/audit"); err != nil && !exists(err) {
		return fmt.Errorf("topic events: %w", err)
	}
	return nil
}

type gen struct {
	c    *client.Client
	t    string
	rate float64
	seq  uint64
	mu   sync.Mutex
}

func (g *gen) start(ctx context.Context, wg *sync.WaitGroup) {
	run := func(f func(context.Context)) {
		wg.Add(1)
		go func() { defer wg.Done(); f(ctx) }()
	}
	run(g.produce)
	for range 2 {
		run(func(ctx context.Context) { g.consume(ctx, "orders", g.orderOutcome, 0) })
	}
	run(func(ctx context.Context) { g.consume(ctx, "emails", alwaysAck, 0) })
	run(func(ctx context.Context) { g.consume(ctx, "billing", alwaysAck, 0) })
	// audit stops 3 minutes out of every 15: a backlog builds up and drains,
	// and the oldest-message age climbs.
	run(func(ctx context.Context) { g.consume(ctx, "audit", alwaysAck, 3*time.Minute) })
	// orders-dlq has no consumer on purpose: dead letters accumulate.
}

// produce sends at a rate that varies smoothly around g.rate.
func (g *gen) produce(ctx context.Context) {
	start := time.Now()
	for ctx.Err() == nil {
		phase := 2 * math.Pi * time.Since(start).Seconds() / 600 // 10-minute cycle
		r := max(g.rate*(1+0.5*math.Sin(phase)), 0.1)
		if !sleep(ctx, time.Duration(rand.ExpFloat64()/r*float64(time.Second))) {
			return
		}
		g.mu.Lock()
		g.seq++
		n := g.seq
		g.mu.Unlock()

		var err error
		switch p := rand.Float64(); {
		case p < 0.5: // an order, ordered per customer
			body := fmt.Sprintf(`{"order":%d,"customer":"c%d","total":%.2f}`, n, rand.IntN(20), rand.Float64()*200)
			_, err = g.c.Send(ctx, g.q("orders"), []byte(body), fmt.Sprintf("customer-%d", rand.IntN(20)))
		case p < 0.75: // an email, some scheduled for later
			o := client.SendOptions{}
			if rand.Float64() < 0.1 {
				o.Delay = time.Duration(5+rand.IntN(55)) * time.Second
			}
			_, err = g.c.SendWith(ctx, g.q("emails"), []byte(fmt.Sprintf(`{"to":"user%d@example.com","template":"receipt"}`, n%500)), o)
		default: // an event, fanned out to billing and audit
			_, err = g.c.Publish(ctx, g.q("events"), []byte(fmt.Sprintf(`{"event":"order.paid","order":%d}`, n)), client.SendOptions{})
		}
		if err != nil && ctx.Err() == nil {
			slog.Warn("send", "tenant", g.t, "err", err)
			sleep(ctx, time.Second)
		}
	}
}

type outcome int

const (
	ack outcome = iota
	nack
	deadLetter
	abandon // never settled: the lease expires and the message comes back
)

func alwaysAck(*api.Delivery) outcome { return ack }

// orderOutcome: mostly fine, some transient failures, a few poison messages
// and a few consumers that "crash" mid-message.
func (g *gen) orderOutcome(*api.Delivery) outcome {
	switch p := rand.Float64(); {
	case p < 0.90:
		return ack
	case p < 0.97:
		return nack
	case p < 0.985:
		return deadLetter
	default:
		return abandon
	}
}

// consume long-polls queue and settles each message as decide says. With
// pauseEvery > 0 it stops for that long every 5×pauseEvery.
func (g *gen) consume(ctx context.Context, queue string, decide func(*api.Delivery) outcome, pause time.Duration) {
	name := g.q(queue)
	cycle := time.Now()
	for ctx.Err() == nil {
		if pause > 0 && time.Since(cycle) > 4*pause {
			slog.Info("consumer paused", "queue", name, "for", pause)
			if !sleep(ctx, pause) {
				return
			}
			cycle = time.Now()
		}
		d, err := g.c.Receive(ctx, name, 10*time.Second)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("receive", "queue", name, "err", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		if d == nil {
			continue
		}
		sleep(ctx, time.Duration(5+rand.IntN(45))*time.Millisecond) // the "work"
		switch decide(d) {
		case ack:
			err = g.c.Ack(ctx, name, d.LeaseID)
		case nack:
			err = g.c.Nack(ctx, name, d.LeaseID)
		case deadLetter:
			err = g.c.DeadLetter(ctx, name, d.LeaseID)
		case abandon:
		}
		if err != nil && !client.IsLeaseGone(err) && ctx.Err() == nil {
			slog.Warn("settle", "queue", name, "err", err)
		}
	}
}

func (g *gen) q(name string) string { return g.t + "/" + name }

func waitHealthy(ctx context.Context, c *client.Client) {
	for ctx.Err() == nil {
		_, err := c.ListQueues(ctx)
		if err == nil {
			return
		}
		slog.Info("waiting for qsrv", "err", err)
		sleep(ctx, 2*time.Second)
	}
}

func exists(err error) bool {
	var e *client.APIError
	return errors.As(err, &e) && e.Code == api.CodeExists
}

// sleep waits d or until ctx is done; reports whether it slept the whole d.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func floatEnv(k string, def float64) float64 {
	var f float64
	if _, err := fmt.Sscan(os.Getenv(k), &f); err == nil && f > 0 {
		return f
	}
	return def
}
