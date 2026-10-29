package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
)

const (
	MaxWait          = 20 * time.Second
	DefaultRetention = 90 * 24 * time.Hour
)

type Options struct {
	Auth            *Authenticator // nil = no auth (only for local dev)
	MaxMessageBytes int64          // default 256KiB
}

type Server struct {
	b        *queue.Broker
	maxBody  int64
	maxFrame int64
}

func New(b *queue.Broker, o Options) http.Handler {
	if o.MaxMessageBytes <= 0 {
		o.MaxMessageBytes = 256 << 10
	}
	// JSON base64 inflates ~4/3; allow headroom for the envelope.
	s := &Server{b: b, maxBody: o.MaxMessageBytes, maxFrame: o.MaxMessageBytes*4/3 + 4096}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/queues", s.list)
	mux.HandleFunc("PUT /v1/queues/{name}", s.create)
	mux.HandleFunc("GET /v1/queues/{name}", s.get)
	mux.HandleFunc("DELETE /v1/queues/{name}", s.delete)
	mux.HandleFunc("POST /v1/queues/{name}/messages", s.send)
	mux.HandleFunc("POST /v1/queues/{name}/receive", s.receive)
	mux.HandleFunc("POST /v1/queues/{name}/leases/{lease}/ack", s.ack)
	mux.HandleFunc("POST /v1/queues/{name}/leases/{lease}/nack", s.nack)
	mux.HandleFunc("POST /v1/queues/{name}/leases/{lease}/extend", s.extend)
	mux.HandleFunc("POST /v1/queues/{name}/replay", s.replay)

	var h http.Handler = mux
	if o.Auth != nil {
		h = o.Auth.Middleware(h)
	}

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
	root.Handle("/", h)
	return accessLog(root)
}

// --- mapping ---

func toAPIQueue(q *queue.Queue) api.Queue {
	c := q.Config()
	st := q.Stats()
	ret := api.Duration(c.Retention)
	return api.Queue{
		Name: q.Name(),
		QueueConfig: api.QueueConfig{
			VisibilityTimeout: api.Duration(c.VisibilityTimeout),
			Retry: &api.RetryPolicy{
				MaxAttempts:    c.Retry.MaxAttempts,
				InitialBackoff: api.Duration(c.Retry.InitialBackoff),
				MaxBackoff:     api.Duration(c.Retry.MaxBackoff),
				Multiplier:     c.Retry.Multiplier,
			},
			DeadLetterQueue: c.DeadLetterQueue,
			Ordering:        string(c.Ordering),
			MaxMessages:     c.MaxMessages,
			Retention:       &ret,
		},
		Stats: api.Stats(st),
	}
}

func toAPIMessage(m queue.Message) api.Message {
	text, raw := api.EncodeBody(m.Body)
	return api.Message{ID: m.ID, GroupID: m.GroupID, Body: text, BodyBase64: raw, Attempts: m.Attempts, EnqueuedAt: m.EnqueuedAt, ReplayOf: m.ReplayOf}
}

// --- handlers ---

func (s *Server) list(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, api.QueueList{Queues: s.b.List()})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var in api.QueueConfig
	if !s.decode(w, r, &in, 64<<10) {
		return
	}
	cfg := queue.Config{
		VisibilityTimeout: in.VisibilityTimeout.D(),
		Retry:             queue.DefaultRetryPolicy(),
		DeadLetterQueue:   in.DeadLetterQueue,
		Ordering:          queue.Ordering(in.Ordering),
		MaxMessages:       in.MaxMessages,
		Retention:         DefaultRetention,
	}
	if in.Retention != nil {
		cfg.Retention = in.Retention.D()
	}
	if in.Retry != nil {
		cfg.Retry = queue.RetryPolicy{
			MaxAttempts:    in.Retry.MaxAttempts,
			InitialBackoff: in.Retry.InitialBackoff.D(),
			MaxBackoff:     in.Retry.MaxBackoff.D(),
			Multiplier:     in.Retry.Multiplier,
		}
	}
	q, err := s.b.Create(r.PathValue("name"), cfg)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toAPIQueue(q))
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	if q, ok := s.queue(w, r); ok {
		writeJSON(w, http.StatusOK, toAPIQueue(q))
	}
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	if err := s.b.Delete(r.PathValue("name")); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	var in api.SendRequest
	if !s.decode(w, r, &in, s.maxFrame) {
		return
	}
	var body []byte
	switch {
	case in.Body != nil && in.BodyBase64 != nil:
		writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, "set either body or body_base64, not both")
		return
	case in.Body != nil:
		body = []byte(*in.Body)
	default:
		body = in.BodyBase64
	}
	if int64(len(body)) > s.maxBody {
		writeAPIErr(w, http.StatusRequestEntityTooLarge, api.CodeBadRequest, "message too large")
		return
	}
	if d := in.Delay.D(); d < 0 || d > queue.MaxSendDelay {
		writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, fmt.Sprintf("delay must be between 0 and %s", queue.MaxSendDelay))
		return
	}
	m, err := q.SendDelayed(body, in.GroupID, in.Delay.D())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toAPIMessage(m))
}

func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	var wait time.Duration
	if v := r.URL.Query().Get("wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, "invalid wait")
			return
		}
		wait = min(d, MaxWait)
	}
	d, err := q.Receive(r.Context(), wait)
	switch {
	case errors.Is(err, queue.ErrEmpty):
		w.WriteHeader(http.StatusNoContent)
	case r.Context().Err() != nil:
		// client went away or server shutting down
		writeAPIErr(w, http.StatusServiceUnavailable, api.CodeUnavailable, "server shutting down")
	case err != nil:
		writeErr(w, err)
	default:
		writeJSON(w, http.StatusOK, api.Delivery{Message: toAPIMessage(d.Message), LeaseID: d.LeaseID, ExpiresAt: d.ExpiresAt})
	}
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	s.settle(w, r, (*queue.Queue).Ack)
}

func (s *Server) nack(w http.ResponseWriter, r *http.Request) {
	s.settle(w, r, (*queue.Queue).Nack)
}

func (s *Server) settle(w http.ResponseWriter, r *http.Request, fn func(*queue.Queue, string) error) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	if err := fn(q, r.PathValue("lease")); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) extend(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	var in api.ExtendRequest
	if r.ContentLength != 0 && !s.decode(w, r, &in, 4096) {
		return
	}
	exp, err := q.Extend(r.PathValue("lease"), in.By.D())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.ExtendResponse{ExpiresAt: exp})
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	var in api.ReplayRequest
	if !s.decode(w, r, &in, 16<<10) {
		return
	}
	req := queue.ReplayRequest{To: in.To, States: in.States, GroupID: in.GroupID, After: in.Cursor, Limit: in.Limit}
	if in.From != nil {
		req.From = *in.From
	}
	if in.Until != nil {
		req.Until = *in.Until
	}
	res, err := s.b.Replay(r.PathValue("name"), req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.ReplayResponse{Replayed: res.Replayed, NextCursor: res.Next})
}

// --- helpers ---

func (s *Server) queue(w http.ResponseWriter, r *http.Request) (*queue.Queue, bool) {
	q, err := s.b.Get(r.PathValue("name"))
	if err != nil {
		writeErr(w, err)
		return nil, false
	}
	return q, true
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeAPIErr(w, http.StatusRequestEntityTooLarge, api.CodeBadRequest, "request too large")
			return false
		}
		writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, err.Error())
		return false
	}
	return true
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrQueueNotFound):
		writeAPIErr(w, http.StatusNotFound, api.CodeNotFound, err.Error())
	case errors.Is(err, queue.ErrLeaseNotFound):
		writeAPIErr(w, http.StatusGone, api.CodeLeaseGone, err.Error())
	case errors.Is(err, queue.ErrQueueExists):
		writeAPIErr(w, http.StatusConflict, api.CodeExists, err.Error())
	case errors.Is(err, queue.ErrInvalidConfig), errors.Is(err, queue.ErrInvalidMessage):
		writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, err.Error())
	case errors.Is(err, queue.ErrQueueFull):
		writeAPIErr(w, http.StatusTooManyRequests, api.CodeQueueFull, err.Error())
	case errors.Is(err, queue.ErrQueueClosed):
		writeAPIErr(w, http.StatusServiceUnavailable, api.CodeUnavailable, err.Error())
	default:
		slog.Error("unhandled error", "err", err)
		writeAPIErr(w, http.StatusInternalServerError, api.CodeInternal, "internal error")
	}
}

func writeAPIErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, api.Error{Error: msg, Code: code})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// --- access log ---

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(c int) { s.status = c; s.ResponseWriter.WriteHeader(c) }

func accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		lvl := slog.LevelDebug
		switch {
		case sw.status == http.StatusServiceUnavailable:
			lvl = slog.LevelInfo // expected during shutdown / queue deletion
		case sw.status >= 500:
			lvl = slog.LevelError
		case sw.status == http.StatusUnauthorized:
			lvl = slog.LevelWarn
		}
		slog.Log(r.Context(), lvl, "http", "method", r.Method, "path", r.URL.Path, "status", sw.status,
			"dur", time.Since(start).Round(time.Microsecond), "remote", r.RemoteAddr)
	})
}
