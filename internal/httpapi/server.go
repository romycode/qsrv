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
	DefaultRetention = queue.DefaultRetention
)

type Options struct {
	Auth            *Authenticator // nil = no auth (only for local dev)
	MaxMessageBytes int64          // default 256KiB
	// Admin serves the read-only admin UI under /admin/ (see admin.go).
	Admin   bool
	Version string // shown in the admin UI, and busts its asset cache
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
	mux.HandleFunc("GET /v1/queues/{name}/messages", s.peek)
	mux.HandleFunc("GET /v1/queues/{name}/messages/{id}", s.peekOne)
	mux.HandleFunc("GET /v1/overview", s.overview)
	mux.HandleFunc("POST /v1/queues/{name}/receive", s.receive)
	mux.HandleFunc("POST /v1/queues/{name}/leases/{lease}/ack", s.ack)
	mux.HandleFunc("POST /v1/queues/{name}/leases/{lease}/nack", s.nack)
	mux.HandleFunc("POST /v1/queues/{name}/leases/{lease}/extend", s.extend)
	mux.HandleFunc("POST /v1/queues/{name}/replay", s.replay)
	mux.HandleFunc("GET /v1/topics", s.listTopics)
	mux.HandleFunc("PUT /v1/topics/{topic}", s.createTopic)
	mux.HandleFunc("GET /v1/topics/{topic}", s.getTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", s.deleteTopic)
	mux.HandleFunc("PUT /v1/topics/{topic}/subscriptions/{queue}", s.subscribe)
	mux.HandleFunc("DELETE /v1/topics/{topic}/subscriptions/{queue}", s.unsubscribe)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", s.publish)

	var h http.Handler = mux
	if o.Auth != nil {
		h = o.Auth.Middleware(h)
	}

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
	if o.Admin {
		ah := s.adminHandler(o.Auth, o.Version)
		root.Handle("/admin", ah)
		root.Handle("/admin/", ah)
	}
	root.Handle("/", h)
	return accessLog(root)
}

// --- mapping ---

func toAPIQueue(sc scope, q *queue.Queue) api.Queue {
	c := q.Config()
	st := q.Stats()
	var ret *api.Duration
	if !c.Persistent {
		r := api.Duration(c.Retention)
		ret = &r
	}
	return api.Queue{
		Name: sc.local(q.Name()),
		QueueConfig: api.QueueConfig{
			VisibilityTimeout: api.Duration(c.VisibilityTimeout),
			Retry: &api.RetryPolicy{
				MaxAttempts:    c.Retry.MaxAttempts,
				InitialBackoff: api.Duration(c.Retry.InitialBackoff),
				MaxBackoff:     api.Duration(c.Retry.MaxBackoff),
				Multiplier:     c.Retry.Multiplier,
			},
			DeadLetterQueue: sc.local(c.DeadLetterQueue),
			Ordering:        string(c.Ordering),
			MaxMessages:     c.MaxMessages,
			Retention:       ret,
			Persistent:      c.Persistent,
		},
		Stats: api.Stats(st),
	}
}

func toAPIMessage(m queue.Message) api.Message {
	text, raw := api.EncodeBody(m.Body)
	return api.Message{ID: m.ID, GroupID: m.GroupID, Body: text, BodyBase64: raw, Attempts: m.Attempts, EnqueuedAt: m.EnqueuedAt, ReplayOf: m.ReplayOf}
}

// --- handlers ---

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	out := api.QueueList{Queues: []string{}}
	for _, n := range s.b.List() {
		if sc.owns(n) {
			out.Queues = append(out.Queues, sc.local(n))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	name, ok := sc.full(r.PathValue("name"))
	if !ok {
		writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "invalid queue name ("+sc.nameHelp()+")")
		return
	}
	var in api.QueueConfig
	if !s.decode(w, r, &in, 64<<10) {
		return
	}
	dlq := ""
	if in.DeadLetterQueue != "" {
		if dlq, ok = sc.full(in.DeadLetterQueue); !ok {
			writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "invalid dead_letter_queue name ("+sc.nameHelp()+")")
			return
		}
	}
	cfg := queue.Config{
		VisibilityTimeout: in.VisibilityTimeout.D(),
		Retry:             queue.DefaultRetryPolicy(),
		DeadLetterQueue:   dlq,
		Ordering:          queue.Ordering(in.Ordering),
		MaxMessages:       in.MaxMessages,
		Retention:         DefaultRetention,
		Persistent:        in.Persistent,
	}
	switch {
	case in.Persistent && in.Retention != nil:
		writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "retention and persistent are exclusive: a persistent queue never expires messages")
		return
	case in.Persistent:
		cfg.Retention = 0
	case in.Retention != nil:
		if in.Retention.D() < queue.MinRetention {
			writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "retention must be at least "+queue.MinRetention.String()+" (for no expiry, use persistent: true)")
			return
		}
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
	q, err := s.b.Create(name, cfg)
	if err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	writeJSON(w, http.StatusCreated, toAPIQueue(sc, q))
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	if q, ok := s.queue(w, r); ok {
		writeJSON(w, http.StatusOK, toAPIQueue(scopeOf(r), q))
	}
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	name, ok := sc.full(r.PathValue("name"))
	if !ok {
		writeErr(w, queue.ErrQueueNotFound)
		return
	}
	if err := s.b.Delete(name); err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	in, body, ok := s.readSend(w, r)
	if !ok {
		return
	}
	m, err := q.SendDelayed(body, in.GroupID, in.Delay.D())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toAPIMessage(m))
}

// readSend decodes and validates a send/publish request.
func (s *Server) readSend(w http.ResponseWriter, r *http.Request) (api.SendRequest, []byte, bool) {
	var in api.SendRequest
	if !s.decode(w, r, &in, s.maxFrame) {
		return in, nil, false
	}
	var body []byte
	switch {
	case in.Body != nil && in.BodyBase64 != nil:
		writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, "set either body or body_base64, not both")
		return in, nil, false
	case in.Body != nil:
		body = []byte(*in.Body)
	default:
		body = in.BodyBase64
	}
	if int64(len(body)) > s.maxBody {
		writeAPIErr(w, http.StatusRequestEntityTooLarge, api.CodeBadRequest, "message too large")
		return in, nil, false
	}
	if d := in.Delay.D(); d < 0 || d > queue.MaxSendDelay {
		writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, fmt.Sprintf("delay must be between 0 and %s", queue.MaxSendDelay))
		return in, nil, false
	}
	return in, body, true
}

// --- topics ---

func toAPITopic(sc scope, t queue.Topic) api.Topic {
	return api.Topic{Name: sc.local(t.Name), Queues: sc.locals(t.Queues)}
}

// topicName resolves the {topic} path value; a name outside the scope is
// reported as not found.
func (s *Server) topicName(w http.ResponseWriter, r *http.Request, sc scope) (string, bool) {
	name, ok := sc.full(r.PathValue("topic"))
	if !ok {
		writeErr(w, queue.ErrTopicNotFound)
	}
	return name, ok
}

func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	out := api.TopicList{Topics: []api.Topic{}}
	for _, t := range s.b.Topics() {
		if sc.owns(t.Name) {
			out.Topics = append(out.Topics, toAPITopic(sc, t))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) createTopic(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	name, ok := sc.full(r.PathValue("topic"))
	if !ok {
		writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "invalid topic name ("+sc.nameHelp()+")")
		return
	}
	var in api.TopicRequest
	if !s.decode(w, r, &in, 64<<10) {
		return
	}
	queues := make([]string, len(in.Queues))
	for i, q := range in.Queues {
		if queues[i], ok = sc.full(q); !ok {
			writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "invalid queue name "+fmt.Sprintf("%q", q)+" ("+sc.nameHelp()+")")
			return
		}
	}
	t, err := s.b.CreateTopic(name, queues)
	if err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	writeJSON(w, http.StatusCreated, toAPITopic(sc, t))
}

func (s *Server) getTopic(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	name, ok := s.topicName(w, r, sc)
	if !ok {
		return
	}
	t, err := s.b.GetTopic(name)
	if err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	writeJSON(w, http.StatusOK, toAPITopic(sc, t))
}

func (s *Server) deleteTopic(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	name, ok := s.topicName(w, r, sc)
	if !ok {
		return
	}
	if err := s.b.DeleteTopic(name); err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	s.subscription(w, r, s.b.Subscribe)
}

func (s *Server) unsubscribe(w http.ResponseWriter, r *http.Request) {
	s.subscription(w, r, s.b.Unsubscribe)
}

func (s *Server) subscription(w http.ResponseWriter, r *http.Request, fn func(topic, queue string) (queue.Topic, error)) {
	sc := scopeOf(r)
	topic, ok := s.topicName(w, r, sc)
	if !ok {
		return
	}
	q, ok := sc.full(r.PathValue("queue"))
	if !ok {
		writeErr(w, queue.ErrQueueNotFound)
		return
	}
	t, err := fn(topic, q)
	if err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	writeJSON(w, http.StatusOK, toAPITopic(sc, t))
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	topic, ok := s.topicName(w, r, sc)
	if !ok {
		return
	}
	in, body, ok := s.readSend(w, r)
	if !ok {
		return
	}
	pub, err := s.b.Publish(topic, body, in.GroupID, in.Delay.D())
	if err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	out := api.PublishResponse{Messages: make([]api.PublishedMessage, len(pub))}
	for i, p := range pub {
		out.Messages[i] = api.PublishedMessage{Queue: sc.local(p.Queue), Message: toAPIMessage(p.Message)}
	}
	writeJSON(w, http.StatusCreated, out)
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
	var in api.NackRequest
	if r.ContentLength != 0 && !s.decode(w, r, &in, 4096) {
		return
	}
	if in.DeadLetter {
		s.settle(w, r, (*queue.Queue).DeadLetter)
		return
	}
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
	sc := scopeOf(r)
	src, ok := sc.full(r.PathValue("name"))
	if !ok {
		writeErr(w, queue.ErrQueueNotFound)
		return
	}
	to := ""
	if in.To != "" {
		if to, ok = sc.full(in.To); !ok {
			writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, "invalid destination queue name ("+sc.nameHelp()+")")
			return
		}
	}
	req := queue.ReplayRequest{To: to, States: in.States, GroupID: in.GroupID, After: in.Cursor, Limit: in.Limit}
	if in.From != nil {
		req.From = *in.From
	}
	if in.Until != nil {
		req.Until = *in.Until
	}
	res, err := s.b.Replay(src, req)
	if err != nil {
		writeScopedErr(w, sc, err)
		return
	}
	writeJSON(w, http.StatusOK, api.ReplayResponse{Replayed: res.Replayed, NextCursor: res.Next})
}

// --- helpers ---

// queue resolves the {name} path value in the request's scope. A name
// outside the scope is reported as not found.
func (s *Server) queue(w http.ResponseWriter, r *http.Request) (*queue.Queue, bool) {
	name, ok := scopeOf(r).full(r.PathValue("name"))
	if !ok {
		writeErr(w, queue.ErrQueueNotFound)
		return nil, false
	}
	q, err := s.b.Get(name)
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
	case errors.Is(err, queue.ErrQueueNotFound), errors.Is(err, queue.ErrTopicNotFound), errors.Is(err, queue.ErrMessageNotFound):
		writeAPIErr(w, http.StatusNotFound, api.CodeNotFound, err.Error())
	case errors.Is(err, queue.ErrLeaseNotFound):
		writeAPIErr(w, http.StatusGone, api.CodeLeaseGone, err.Error())
	case errors.Is(err, queue.ErrQueueExists), errors.Is(err, queue.ErrTopicExists):
		writeAPIErr(w, http.StatusConflict, api.CodeExists, err.Error())
	case errors.Is(err, queue.ErrInvalidConfig), errors.Is(err, queue.ErrInvalidMessage):
		writeAPIErr(w, http.StatusUnprocessableEntity, api.CodeInvalidConfig, err.Error())
	case errors.Is(err, queue.ErrQueueFull):
		writeAPIErr(w, http.StatusTooManyRequests, api.CodeQueueFull, err.Error())
	case errors.Is(err, queue.ErrQueueClosed):
		writeAPIErr(w, http.StatusServiceUnavailable, api.CodeUnavailable, err.Error())
	case errors.Is(err, queue.ErrStorageFull):
		w.Header().Set("Retry-After", "30")
		writeAPIErr(w, http.StatusServiceUnavailable, api.CodeStorageFull, err.Error())
	default:
		slog.Error("unhandled error", "err", err)
		writeAPIErr(w, http.StatusInternalServerError, api.CodeInternal, "internal error")
	}
}

// writeScopedErr is writeErr with the tenant prefix removed from names in the message.
func writeScopedErr(w http.ResponseWriter, sc scope, err error) {
	writeErr(&scopedErrWriter{ResponseWriter: w, sc: sc}, err)
}

type scopedErrWriter struct {
	http.ResponseWriter
	sc scope
}

func (s *scopedErrWriter) Write(b []byte) (int, error) {
	if _, err := s.ResponseWriter.Write([]byte(s.sc.message(string(b)))); err != nil {
		return 0, err
	}
	return len(b), nil
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
		inner, id := withIdentitySlot(r)
		next.ServeHTTP(sw, inner)
		r.Pattern = inner.Pattern // the mux sets it on the request it got; metrics read it here
		tenant := id.Tenant
		if tenant == "" {
			tenant = AdminTenant
		}
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
			"dur", time.Since(start).Round(time.Microsecond), "remote", r.RemoteAddr, "tenant", tenant, "key", id.Name)
	})
}
