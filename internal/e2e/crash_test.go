//go:build e2e

// Package e2e runs the real qsrv binary. The crash test kills it with
// SIGKILL over and over under load and checks qsrv's two promises:
//
//   - a send or publish that got 201 is never lost: it is delivered
//     eventually, after any number of crashes;
//   - an ack that got 204 is final: that message is never delivered again.
//
// Duplicates of messages whose ack was not confirmed are allowed
// (at-least-once). Run with:
//
//	go test -tags e2e -run Crash -v ./internal/e2e/            # WASM driver
//	QSRV_E2E_TAGS=sqlite_cgo go test -tags e2e ./internal/e2e/ # cgo driver
package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

const (
	keyA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	keyB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func buildQsrv(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "qsrv")
	args := []string{"build", "-o", bin}
	if tags := os.Getenv("QSRV_E2E_TAGS"); tags != "" {
		args = append(args, "-tags", tags)
	}
	cmd := exec.Command("go", append(args, "../../cmd/qsrv")...)
	if os.Getenv("QSRV_E2E_TAGS") != "" {
		cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build qsrv: %v\n%s", err, out)
	}
	return bin
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type server struct {
	bin, dataDir, backupDir, addr, log string
	cmd                                *exec.Cmd
}

func (s *server) start(t *testing.T) {
	t.Helper()
	logf, err := os.OpenFile(s.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	s.cmd = exec.Command(s.bin, "-data-dir", s.dataDir, "-addr", s.addr, "-metrics-addr", "",
		"-backup-dir", s.backupDir, "-backup-every", "1s", "-backup-keep", "3", "-min-free-disk", "0")
	s.cmd.Env = append(os.Environ(), "QSRV_API_KEYS=a:e2e:"+keyA+",b:e2e:"+keyB, "QSRV_WASM_CACHE_DIR="+filepath.Join(filepath.Dir(s.dataDir), "wasmcache"))
	s.cmd.Stdout, s.cmd.Stderr = logf, logf
	if err := s.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	logf.Close()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get("http://" + s.addr + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	raw, _ := os.ReadFile(s.log)
	t.Fatalf("qsrv did not become healthy:\n%s", raw)
}

func (s *server) kill() {
	s.cmd.Process.Signal(syscall.SIGKILL)
	s.cmd.Wait()
}

// ledger records what clients were told.
type ledger struct {
	mu        sync.Mutex
	attempted map[string]bool            // token
	confirmed map[string]map[string]bool // queue -> token: send/publish answered 2xx
	acked     map[string]map[string]bool // queue -> token: ack answered 204
	delivered map[string]map[string]int  // queue -> token -> deliveries
	ackedThen []string                   // violations: delivered after a confirmed ack
	unknown   []string                   // violations: delivered but never sent
}

func newLedger(queues ...string) *ledger {
	l := &ledger{attempted: map[string]bool{}, confirmed: map[string]map[string]bool{}, acked: map[string]map[string]bool{}, delivered: map[string]map[string]int{}}
	for _, q := range queues {
		l.confirmed[q], l.acked[q], l.delivered[q] = map[string]bool{}, map[string]bool{}, map[string]int{}
	}
	return l
}

func (l *ledger) deliver(q, token string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.attempted[token] {
		l.unknown = append(l.unknown, q+" "+token)
	}
	if l.acked[q][token] {
		l.ackedThen = append(l.ackedThen, q+" "+token)
	}
	l.delivered[q][token]++
}

type target struct {
	c      *client.Client
	topic  string   // publish here, or
	queue  string   // send here
	queues []string // queues (full names) the message lands in
}

func TestCrashSafety(t *testing.T) {
	rounds := envInt("QSRV_E2E_ROUNDS", 15)
	dir := t.TempDir()
	s := &server{bin: buildQsrv(t), dataDir: filepath.Join(dir, "data"), backupDir: filepath.Join(dir, "backup"), addr: freeAddr(t), log: filepath.Join(dir, "qsrv.log")}
	t.Cleanup(func() {
		if t.Failed() {
			raw, _ := os.ReadFile(s.log)
			t.Logf("qsrv log (tail):\n%s", tail(raw, 4000))
		}
	})

	ctx := context.Background()
	ca := client.New("http://"+s.addr, keyA, client.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
	cb := client.New("http://"+s.addr, keyB, client.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
	s.start(t)
	cfg := api.QueueConfig{VisibilityTimeout: api.Duration(2 * time.Second), Retry: &api.RetryPolicy{MaxAttempts: 0}}
	for _, q := range []string{"q1", "q2"} {
		if _, err := ca.CreateQueue(ctx, q, cfg); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cb.CreateQueue(ctx, "q", cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := ca.CreateTopic(ctx, "events", "q1", "q2"); err != nil {
		t.Fatal(err)
	}
	targets := []target{
		{c: ca, topic: "events", queues: []string{"q1", "q2"}}, // atomic fan-out, tenant a
		{c: cb, queue: "q", queues: []string{"q"}},             // tenant b
	}
	consumers := []struct {
		c     *client.Client
		queue string
		key   string // ledger name
	}{{ca, "q1", "a/q1"}, {ca, "q2", "a/q2"}, {cb, "q", "b/q"}}
	led := newLedger("a/q1", "a/q2", "b/q")
	ledgerName := map[string]map[string]string{"events": {"q1": "a/q1", "q2": "a/q2"}, "q": {"q": "b/q"}}

	var seq atomic.Uint64
	runLoad := func(stop <-chan struct{}, produce bool) *sync.WaitGroup {
		var wg sync.WaitGroup
		if produce {
			for _, tg := range targets {
				for p := 0; p < 3; p++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							select {
							case <-stop:
								return
							default:
							}
							token := fmt.Sprintf("m%07d", seq.Add(1))
							led.mu.Lock()
							led.attempted[token] = true
							led.mu.Unlock()
							var err error
							if tg.topic != "" {
								_, err = tg.c.Publish(ctx, tg.topic, []byte(token), client.SendOptions{})
							} else {
								_, err = tg.c.Send(ctx, tg.queue, []byte(token), "")
							}
							if err != nil {
								time.Sleep(10 * time.Millisecond) // server down
								continue
							}
							name := tg.topic
							if name == "" {
								name = tg.queue
							}
							led.mu.Lock()
							for _, q := range tg.queues {
								led.confirmed[ledgerName[name][q]][token] = true
							}
							led.mu.Unlock()
						}
					}()
				}
			}
		}
		for _, cs := range consumers {
			for w := 0; w < 2; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						d, err := cs.c.Receive(ctx, cs.queue, 200*time.Millisecond)
						if err != nil || d == nil {
							if err != nil {
								time.Sleep(10 * time.Millisecond)
							}
							continue
						}
						token := string(d.Message.Bytes())
						led.deliver(cs.key, token)
						if err := cs.c.Ack(ctx, cs.queue, d.LeaseID); err == nil {
							led.mu.Lock()
							led.acked[cs.key][token] = true
							led.mu.Unlock()
						}
					}
				}()
			}
		}
		return &wg
	}

	for r := 0; r < rounds; r++ {
		if r > 0 {
			s.start(t)
		}
		stop := make(chan struct{})
		wg := runLoad(stop, true)
		time.Sleep(time.Duration(300+rand.IntN(1200)) * time.Millisecond)
		s.kill() // mid-flight: requests in progress get errors
		close(stop)
		wg.Wait()
	}

	// Drain: no producers; leases from the last crash expire after 2s.
	s.start(t)
	stop := make(chan struct{})
	wg := runLoad(stop, false)
	deadline := time.Now().Add(60 * time.Second)
	for {
		empty := true
		for _, cs := range consumers {
			info, err := cs.c.GetQueue(ctx, cs.queue)
			if err != nil {
				t.Fatal(err)
			}
			if info.Stats.Messages > 0 {
				empty = false
			}
		}
		if empty || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	s.cmd.Process.Signal(syscall.SIGTERM)
	if err := s.cmd.Wait(); err != nil {
		t.Errorf("qsrv did not exit cleanly on SIGTERM: %v", err)
	}

	led.mu.Lock()
	defer led.mu.Unlock()
	var confirmed, lost, dups int
	for q, toks := range led.confirmed {
		for tok := range toks {
			confirmed++
			if led.delivered[q][tok] == 0 {
				lost++
				if lost <= 10 {
					t.Errorf("LOST: %s %s was confirmed (201) and never delivered", q, tok)
				}
			}
		}
		for _, n := range led.delivered[q] {
			if n > 1 {
				dups += n - 1
			}
		}
	}
	if lost > 0 {
		t.Errorf("%d of %d confirmed messages lost", lost, confirmed)
	}
	if len(led.ackedThen) > 0 {
		t.Errorf("%d messages delivered again after a confirmed ack, e.g. %v", len(led.ackedThen), led.ackedThen[:min(5, len(led.ackedThen))])
	}
	if len(led.unknown) > 0 {
		t.Errorf("%d deliveries of messages never sent, e.g. %v", len(led.unknown), led.unknown[:min(5, len(led.unknown))])
	}
	if confirmed < 100*rounds {
		t.Errorf("only %d messages confirmed in %d rounds: the load did not run", confirmed, rounds)
	}
	snaps, err := os.ReadDir(s.backupDir)
	if err != nil || len(snaps) == 0 {
		t.Errorf("no snapshots taken: %v", err)
	}
	t.Logf("%d rounds with SIGKILL: %d confirmed deliveries expected, 0 lost, %d duplicates of unconfirmed acks (allowed), %d snapshots kept",
		rounds, confirmed, dups, len(snaps))
}

func tail(b []byte, n int) []byte {
	if len(b) > n {
		return b[len(b)-n:]
	}
	return b
}
