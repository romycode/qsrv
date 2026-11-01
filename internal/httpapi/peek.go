package httpapi

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
)

// Read-only inspection: peek at messages without receiving them, and an
// overview of every queue and topic of the key. Used by the admin UI.

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func toAPIInfo(sc scope, m queue.MessageInfo) api.MessageInfo {
	return api.MessageInfo{
		ID: m.ID, GroupID: m.GroupID, Seq: m.Seq, State: m.State, Attempts: m.Attempts, Size: m.Size,
		EnqueuedAt: m.EnqueuedAt, AvailableAt: timePtr(m.AvailableAt), LeaseExpiresAt: timePtr(m.LeaseExpiresAt),
		DoneAt: timePtr(m.DoneAt), MovedTo: sc.local(m.MovedTo), ReplayOf: m.ReplayOf, Attributes: m.Attributes,
	}
}

// GET /v1/queues/{name}/messages?view=live|history&state=a,b&cursor=N&limit=N
func (s *Server) peek(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	qs := r.URL.Query()
	req := queue.PeekRequest{}
	switch qs.Get("view") {
	case "", "live":
	case "history":
		req.History = true
	default:
		writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, "view must be live or history")
		return
	}
	if st := qs.Get("state"); st != "" {
		req.States = strings.Split(st, ",")
	}
	for _, p := range []struct {
		key string
		dst any
	}{{"cursor", &req.Cursor}, {"limit", &req.Limit}} {
		v := qs.Get(p.key)
		if v == "" {
			continue
		}
		n, err := strconv.ParseUint(v, 10, 63)
		if err != nil {
			writeAPIErr(w, http.StatusBadRequest, api.CodeBadRequest, p.key+" must be a non-negative integer")
			return
		}
		switch d := p.dst.(type) {
		case *uint64:
			*d = n
		case *int:
			*d = int(n)
		}
	}
	page, err := q.Peek(req)
	if err != nil {
		writeErr(w, err)
		return
	}
	sc := scopeOf(r)
	out := api.MessagePage{Messages: make([]api.MessageInfo, len(page.Messages)), NextCursor: page.Next}
	for i, m := range page.Messages {
		out.Messages[i] = toAPIInfo(sc, m)
	}
	writeJSON(w, http.StatusOK, out)
}

// GET /v1/queues/{name}/messages/{id}
func (s *Server) peekOne(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queue(w, r)
	if !ok {
		return
	}
	m, body, err := q.PeekMessage(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	out := toAPIInfo(scopeOf(r), m)
	out.Body, out.BodyBase64 = api.EncodeBody(body)
	writeJSON(w, http.StatusOK, out)
}

// GET /v1/overview
func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	sc := scopeOf(r)
	out := api.Overview{Tenant: sc.tenant, Now: s.b.Now(), Queues: []api.QueueSummary{}, Topics: []api.Topic{}}

	topicsOf := map[string][]string{}
	for _, t := range s.b.Topics() {
		if !sc.owns(t.Name) {
			continue
		}
		out.Topics = append(out.Topics, toAPITopic(sc, t))
		for _, q := range t.Queues {
			topicsOf[q] = append(topicsOf[q], sc.local(t.Name))
		}
	}
	dlqOf := map[string][]string{}
	queues := s.b.Queues()
	for _, q := range queues {
		if dlq := q.Config().DeadLetterQueue; dlq != "" && sc.owns(q.Name()) {
			dlqOf[dlq] = append(dlqOf[dlq], sc.local(q.Name()))
		}
	}
	for _, q := range queues {
		if !sc.owns(q.Name()) {
			continue
		}
		l, c := q.Live(), q.Config()
		of := dlqOf[q.Name()]
		slices.Sort(of)
		out.Queues = append(out.Queues, api.QueueSummary{
			Name: sc.local(q.Name()), Messages: l.Messages, Ready: l.Ready, Delayed: l.Delayed,
			InFlight: l.InFlight, Groups: l.Groups, OldestEnqueuedAt: timePtr(l.OldestEnqueuedAt),
			MaxMessages: c.MaxMessages, DeadLetterQueue: sc.local(c.DeadLetterQueue),
			IsDeadLetterQueueOf: of, Topics: topicsOf[q.Name()],
		})
	}
	slices.SortFunc(out.Queues, func(a, b api.QueueSummary) int { return strings.Compare(a.Name, b.Name) })
	writeJSON(w, http.StatusOK, out)
}
