// Command qctl is the qsrv command-line client.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

const usage = `qctl — qsrv client

Usage:
  qctl [global flags] <command> [command flags] [args]

Global flags (or env):
  -addr     server URL            (QSRV_ADDR, default http://localhost:8080)
  -api-key  API key               (QSRV_API_KEY)

Commands:
  queues                              list queues
  create [flags] NAME                 create a queue (see: qctl create -h)
  info NAME                           config + stats
  delete NAME                         delete a queue and its messages
  send [-group G] [-delay D] QUEUE [BODY|-]  send BODY, or stdin if omitted or "-"
  recv [-wait D] [-ack] [-raw] QUEUE  lease one message (exit 3 if empty)
  ack QUEUE LEASE
  nack [-dead-letter] QUEUE LEASE     failed: retry policy, or straight to the DLQ
  extend [-by D] QUEUE LEASE
  consume [flags] QUEUE -- CMD [ARGS] run CMD per message: body on stdin,
                                      exit 0 = ack, otherwise nack
  replay [flags] QUEUE -to DEST       re-enqueue processed messages into DEST
                                      (see: qctl replay -h)
  topic list|create|info|delete|sub|unsub|publish
                                      fan-out to several queues (see: qctl topic)
`

const exitEmpty = 3

func main() {
	g := flag.NewFlagSet("qctl", flag.ContinueOnError)
	g.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	addr := g.String("addr", envOr("QSRV_ADDR", "http://localhost:8080"), "")
	key := g.String("api-key", os.Getenv("QSRV_API_KEY"), "")
	if err := g.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if g.NArg() == 0 {
		g.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := client.New(*addr, *key)
	cmd, args := g.Arg(0), g.Args()[1:]
	var err error
	switch cmd {
	case "queues":
		err = cmdQueues(ctx, c)
	case "create":
		err = cmdCreate(ctx, c, args)
	case "info":
		err = withName(args, func(n string) error { return cmdInfo(ctx, c, n) })
	case "delete":
		err = withName(args, func(n string) error { return c.DeleteQueue(ctx, n) })
	case "send":
		err = cmdSend(ctx, c, args)
	case "recv":
		err = cmdRecv(ctx, c, args)
	case "ack":
		err = withLease(args, func(q, l string) error { return c.Ack(ctx, q, l) })
	case "nack":
		deadLetter := len(args) > 0 && args[0] == "-dead-letter"
		if deadLetter {
			args = args[1:]
		}
		err = withLease(args, func(q, l string) error {
			if deadLetter {
				return c.DeadLetter(ctx, q, l)
			}
			return c.Nack(ctx, q, l)
		})
	case "extend":
		err = cmdExtend(ctx, c, args)
	case "consume":
		err = cmdConsume(ctx, c, args)
	case "replay":
		err = cmdReplay(ctx, c, args)
	case "topic":
		err = cmdTopic(ctx, c, args)
	case "help", "-h", "--help":
		g.Usage()
	default:
		err = fmt.Errorf("unknown command %q (qctl help)", cmd)
	}

	var ee exitError
	switch {
	case errors.As(err, &ee):
		os.Exit(int(ee))
	case err != nil:
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type exitError int

func (e exitError) Error() string { return "exit " + strconv.Itoa(int(e)) }

func cmdQueues(ctx context.Context, c *client.Client) error {
	qs, err := c.ListQueues(ctx)
	for _, q := range qs {
		fmt.Println(q)
	}
	return err
}

func cmdCreate(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	vt := fs.Duration("visibility", 30*time.Second, "lease duration before redelivery")
	attempts := fs.Int("max-attempts", 5, "deliveries before dead-lettering (0 = infinite)")
	backoff := fs.Duration("backoff", time.Second, "initial retry backoff")
	maxBackoff := fs.Duration("max-backoff", time.Minute, "backoff cap")
	mult := fs.Float64("multiplier", 2, "backoff multiplier")
	dlq := fs.String("dlq", "", "dead letter queue (must exist)")
	ordering := fs.String("ordering", "available", "available | strict")
	maxMsgs := fs.Int("max-messages", 0, "queue depth limit (0 = unlimited)")
	retention := fs.Duration("retention", 0, "expire (delete unprocessed) messages that wait longer than this (default: server default, 14 days)")
	persistent := fs.Bool("persistent", false, "never expire messages and keep processed ones as history for replay")
	fs.Parse(args)
	var ret *api.Duration
	if *retention != 0 {
		r := api.Duration(*retention)
		ret = &r
	}
	if fs.NArg() != 1 {
		return errors.New("usage: qctl create [flags] NAME")
	}
	q, err := c.CreateQueue(ctx, fs.Arg(0), api.QueueConfig{
		VisibilityTimeout: api.Duration(*vt),
		Retry: &api.RetryPolicy{
			MaxAttempts:    *attempts,
			InitialBackoff: api.Duration(*backoff),
			MaxBackoff:     api.Duration(*maxBackoff),
			Multiplier:     *mult,
		},
		DeadLetterQueue: *dlq,
		Ordering:        *ordering,
		MaxMessages:     *maxMsgs,
		Retention:       ret,
		Persistent:      *persistent,
	})
	if err != nil {
		return err
	}
	return printJSON(q)
}

func cmdInfo(ctx context.Context, c *client.Client, name string) error {
	q, err := c.GetQueue(ctx, name)
	if err != nil {
		return err
	}
	return printJSON(q)
}

func cmdSend(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	group := fs.String("group", "", "ordering group id")
	delay := fs.Duration("delay", 0, "deliverable only after this long (max 720h)")
	fs.Parse(args)
	if fs.NArg() < 1 || fs.NArg() > 2 {
		return errors.New("usage: qctl send [-group G] [-delay D] QUEUE [BODY|-]")
	}
	var body []byte
	if fs.NArg() == 2 && fs.Arg(1) != "-" {
		body = []byte(fs.Arg(1))
	} else {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		body = b
	}
	m, err := c.SendWith(ctx, fs.Arg(0), body, client.SendOptions{GroupID: *group, Delay: *delay})
	if err != nil {
		return err
	}
	fmt.Println(m.ID)
	return nil
}

func cmdRecv(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	wait := fs.Duration("wait", 0, "long-poll up to this long (max 20s)")
	ack := fs.Bool("ack", false, "ack immediately after printing")
	raw := fs.Bool("raw", false, "print only the body; lease id goes to stderr")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: qctl recv [-wait D] [-ack] [-raw] QUEUE")
	}
	q := fs.Arg(0)
	d, err := c.Receive(ctx, q, *wait)
	if err != nil {
		return err
	}
	if d == nil {
		return exitError(exitEmpty)
	}
	if *raw {
		os.Stdout.Write(d.Message.Bytes())
		fmt.Fprintln(os.Stderr, d.LeaseID)
	} else if err := printJSON(d); err != nil {
		return err
	}
	if *ack {
		return c.Ack(ctx, q, d.LeaseID)
	}
	return nil
}

// cmdReplay pages through the whole selection unless -limit is set.
func cmdReplay(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	to := fs.String("to", "", "destination queue (required; may be the same queue)")
	from := fs.String("from", "", "only messages enqueued at/after this time (RFC3339 or YYYY-MM-DD)")
	until := fs.String("until", "", "only messages enqueued before this time (RFC3339 or YYYY-MM-DD)")
	group := fs.String("group", "", "only this group_id")
	states := fs.String("states", "", "comma list: acked,dropped,dead_lettered (default all)")
	page := fs.Int("page", 1000, "messages per request (max 10000)")
	limit := fs.Int("limit", 0, "stop after this many messages (0 = all)")
	dry := fs.Bool("dry-run", false, "print the request and exit")
	fs.Parse(reorder(args))
	if fs.NArg() != 1 || *to == "" {
		return errors.New("usage: qctl replay -to DEST [flags] QUEUE")
	}
	req := api.ReplayRequest{To: *to, GroupID: *group, Limit: *page}
	for _, p := range []struct {
		raw string
		dst **time.Time
	}{{*from, &req.From}, {*until, &req.Until}} {
		if p.raw == "" {
			continue
		}
		t, err := parseTime(p.raw)
		if err != nil {
			return err
		}
		*p.dst = &t
	}
	if *states != "" {
		req.States = strings.Split(*states, ",")
	}
	if *dry {
		return printJSON(req)
	}
	total := 0
	for {
		if *limit > 0 {
			req.Limit = min(*page, *limit-total)
		}
		res, err := c.Replay(ctx, fs.Arg(0), req)
		if err != nil {
			return fmt.Errorf("after %d replayed: %w", total, err)
		}
		total += res.Replayed
		fmt.Fprintf(os.Stderr, "replayed %d (total %d)\n", res.Replayed, total)
		if res.NextCursor == 0 || (*limit > 0 && total >= *limit) {
			break
		}
		req.Cursor = res.NextCursor
	}
	fmt.Println(total)
	return nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.ParseInLocation(time.DateOnly, s, time.Local)
}

// reorder lets flags follow the positional QUEUE arg (Go's flag stops at the first one).
func reorder(args []string) []string {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			if !strings.Contains(a, "=") && i+1 < len(args) && a != "-dry-run" && a != "--dry-run" {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		pos = append(pos, a)
	}
	return append(flags, pos...)
}

func cmdExtend(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("extend", flag.ExitOnError)
	by := fs.Duration("by", 0, "new lease duration from now (0 = queue visibility timeout)")
	fs.Parse(args)
	if fs.NArg() != 2 {
		return errors.New("usage: qctl extend [-by D] QUEUE LEASE")
	}
	exp, err := c.Extend(ctx, fs.Arg(0), fs.Arg(1), *by)
	if err != nil {
		return err
	}
	fmt.Println(exp.Format(time.RFC3339Nano))
	return nil
}

// cmdConsume runs a worker pool. Ordering groups are enforced by the server,
// so concurrency > 1 is safe for grouped messages.
//
// SIGINT/SIGTERM stops taking new messages; running commands are allowed to
// finish and are settled. A second signal kills the process (default Go behavior
// after stop()).
func cmdConsume(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	conc := fs.Int("concurrency", 1, "parallel workers")
	wait := fs.Duration("wait", 20*time.Second, "long-poll wait")
	timeout := fs.Duration("timeout", 0, "kill CMD after this long and nack (0 = no limit)")
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) < 3 || rest[1] != "--" {
		return errors.New("usage: qctl consume [flags] QUEUE -- CMD [ARGS...]")
	}
	queue, argv := rest[0], rest[2:]
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("queue", queue)

	var wg sync.WaitGroup
	for i := range max(*conc, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			backoff := time.Second
			for ctx.Err() == nil {
				d, err := c.Receive(ctx, queue, *wait)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Warn("receive failed", "worker", i, "err", err, "retry_in", backoff)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					backoff = min(backoff*2, 30*time.Second)
					continue
				}
				backoff = time.Second
				if d != nil {
					handle(c, log, queue, d, argv, *timeout)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func handle(c *client.Client, log *slog.Logger, queue string, d *api.Delivery, argv []string, timeout time.Duration) {
	log = log.With("id", d.Message.ID, "attempt", d.Message.Attempts)

	// Not tied to the signal context: a running job finishes on shutdown.
	runCtx, cancel := context.Background(), context.CancelFunc(func() {})
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(runCtx, timeout)
	}
	defer cancel()

	hbDone := make(chan struct{})
	defer close(hbDone)
	go heartbeat(c, log, queue, d, hbDone)

	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Stdin = bytes.NewReader(d.Message.Bytes())
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(),
		"QSRV_QUEUE="+queue,
		"QSRV_MESSAGE_ID="+d.Message.ID,
		"QSRV_GROUP_ID="+d.Message.GroupID,
		"QSRV_ATTEMPTS="+strconv.Itoa(d.Message.Attempts),
	)
	start := time.Now()
	runErr := cmd.Run()

	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	var err error
	if runErr == nil {
		err = c.Ack(sctx, queue, d.LeaseID)
		log.Info("acked", "took", time.Since(start).Round(time.Millisecond))
	} else {
		err = c.Nack(sctx, queue, d.LeaseID)
		log.Warn("nacked", "err", runErr, "took", time.Since(start).Round(time.Millisecond))
	}
	if client.IsLeaseGone(err) {
		log.Warn("lease lost before settle; message may be processed twice")
	} else if err != nil {
		log.Error("settle failed", "err", err)
	}
}

// heartbeat extends the lease every third of its duration while CMD runs.
func heartbeat(c *client.Client, log *slog.Logger, queue string, d *api.Delivery, done <-chan struct{}) {
	every := max(time.Until(d.ExpiresAt)/3, time.Second)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), every)
			_, err := c.Extend(ctx, queue, d.LeaseID, 0)
			cancel()
			if client.IsLeaseGone(err) {
				log.Warn("lease lost during processing")
				return
			} else if err != nil {
				log.Warn("extend failed", "err", err)
			}
		}
	}
}

// --- helpers ---

func withName(args []string, fn func(string) error) error {
	if len(args) != 1 {
		return errors.New("expected exactly one queue name")
	}
	return fn(args[0])
}

func withLease(args []string, fn func(q, l string) error) error {
	if len(args) != 2 {
		return errors.New("expected QUEUE LEASE")
	}
	return fn(args[0], args[1])
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
