package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/romycode/qsrv/pkg/api"
)

// The admin UI: server-rendered pages (html/template) made interactive with
// Alpine.js, both embedded in the binary. It is read-only: it shows queues,
// topics and messages and never changes them.
//
// Sign-in is with an API key, like the API: an admin key sees every tenant,
// a tenant key only its own. The key is exchanged for a session cookie
// (HttpOnly, SameSite=Strict, Secure over HTTPS); the server keeps only the
// key's SHA-256 and re-checks it on every request, so removing a key and
// reloading (SIGHUP) ends its sessions at once. The pages read data from
// /admin/api/, which serves the API's GET endpoints for the session.

//go:embed admin/templates/*.html
var adminTemplates embed.FS

//go:embed admin/static
var adminStatic embed.FS

const (
	sessionCookie = "qsrv_session"
	sessionTTL    = 12 * time.Hour
	maxSessions   = 10000
)

type session struct {
	digest  [32]byte
	expires time.Time
}

type sessions struct {
	mu sync.Mutex
	m  map[string]session
}

func (ss *sessions) create(digest [32]byte, now time.Time) string {
	b := make([]byte, 32)
	rand.Read(b)
	id := hex.EncodeToString(b)
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.m) >= maxSessions {
		for k, s := range ss.m {
			if now.After(s.expires) {
				delete(ss.m, k)
			}
		}
		if len(ss.m) >= maxSessions { // still full: drop an arbitrary one
			for k := range ss.m {
				delete(ss.m, k)
				break
			}
		}
	}
	ss.m[id] = session{digest: digest, expires: now.Add(sessionTTL)}
	return id
}

func (ss *sessions) get(id string, now time.Time) (session, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.m[id]
	if ok && now.After(s.expires) {
		delete(ss.m, id)
		return session{}, false
	}
	return s, ok
}

func (ss *sessions) delete(id string) {
	ss.mu.Lock()
	delete(ss.m, id)
	ss.mu.Unlock()
}

type admin struct {
	s        *Server
	auth     *Authenticator // nil: no auth, everyone is admin
	sessions *sessions
	pages    map[string]*template.Template
	api      http.Handler
	version  string
}

type pageData struct {
	Title    string
	Identity Identity
	NoAuth   bool
	Version  string
	Queue    string // queue page: the queue's name as this identity sees it
	Error    string
}

func (s *Server) adminHandler(auth *Authenticator, version string) http.Handler {
	a := &admin{s: s, auth: auth, sessions: &sessions{m: map[string]session{}}, pages: map[string]*template.Template{}, version: version}
	funcs := template.FuncMap{"asset": func(p string) string { return "/admin/static/" + p + "?v=" + url.QueryEscape(version) }}
	for _, p := range []string{"login", "dashboard", "queue"} {
		a.pages[p] = template.Must(template.New("").Funcs(funcs).ParseFS(adminTemplates, "admin/templates/layout.html", "admin/templates/"+p+".html"))
	}

	// Read-only API for the pages: the API's own GET handlers.
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /v1/overview", s.overview)
	apiMux.HandleFunc("GET /v1/queues/{name}", s.get)
	apiMux.HandleFunc("GET /v1/queues/{name}/messages", s.peek)
	apiMux.HandleFunc("GET /v1/queues/{name}/messages/{id}", s.peekOne)
	a.api = apiMux

	static, _ := fs.Sub(adminStatic, "admin/static")
	files := http.FileServerFS(static)

	mux := http.NewServeMux()
	mux.Handle("GET /admin/static/", http.StripPrefix("/admin/static/", cacheFor(files, 24*time.Hour)))
	mux.HandleFunc("GET /admin/login", a.loginPage)
	mux.HandleFunc("POST /admin/login", a.login)
	mux.HandleFunc("POST /admin/logout", a.logout)
	mux.HandleFunc("GET /admin/{$}", a.page("dashboard", "Queues"))
	mux.HandleFunc("GET /admin/queue", a.queuePage)
	mux.Handle("/admin/api/", http.StripPrefix("/admin/api", a.requireSession(a.api, true)))
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusFound)
	})
	return securityHeaders(mux)
}

func cacheFor(h http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(int(d.Seconds())))
		h.ServeHTTP(w, r)
	})
}

// securityHeaders: no framing, no third-party anything. Alpine.js evaluates
// its directives with Function(), hence 'unsafe-eval'; the pages never put
// data into Alpine attributes (data comes as JSON and is shown with x-text).
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self' 'unsafe-eval'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("X-Frame-Options", "DENY")
		if !strings.HasPrefix(r.URL.Path, "/admin/static/") {
			h.Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

// identity resolves the session of the request, re-checking its key.
func (a *admin) identity(r *http.Request) (Identity, bool) {
	if a.auth == nil {
		return Identity{Name: "anonymous"}, true
	}
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return Identity{}, false
	}
	s, ok := a.sessions.get(c.Value, time.Now())
	if !ok {
		return Identity{}, false
	}
	id, ok := a.auth.lookupDigest(s.digest)
	if !ok { // key removed since: end the session
		a.sessions.delete(c.Value)
	}
	return id, ok
}

// requireSession puts the session's identity in the request, like the API's
// Bearer middleware. Without one: 401 for data, the sign-in page for pages.
func (a *admin) requireSession(next http.Handler, isAPI bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := a.identity(r)
		if !ok {
			if isAPI {
				writeAPIErr(w, http.StatusUnauthorized, api.CodeUnauthorized, "session expired: sign in again")
			} else {
				http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
			}
			return
		}
		inner, slot := withIdentitySlot(r)
		*slot = id
		next.ServeHTTP(w, inner)
		r.Pattern = inner.Pattern
	})
}

func (a *admin) render(w http.ResponseWriter, page string, status int, d pageData) {
	d.Version, d.NoAuth = a.version, a.auth == nil
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := a.pages[page].ExecuteTemplate(w, "layout", d); err != nil {
		slog.Error("admin template", "page", page, "err", err)
	}
}

func (a *admin) page(name, title string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.requireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			a.render(w, name, http.StatusOK, pageData{Title: title, Identity: IdentityFrom(r.Context())})
		}), false).ServeHTTP(w, r)
	}
}

func (a *admin) queuePage(w http.ResponseWriter, r *http.Request) {
	a.requireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := IdentityFrom(r.Context())
		name := r.URL.Query().Get("name")
		full, ok := scope{tenant: id.Tenant}.full(name)
		if ok {
			_, err := a.s.b.Get(full)
			ok = err == nil
		}
		if !ok {
			a.render(w, "dashboard", http.StatusNotFound, pageData{Title: "Queues", Identity: id, Error: "There is no queue named " + name + "."})
			return
		}
		a.render(w, "queue", http.StatusOK, pageData{Title: name, Identity: id, Queue: name})
	}), false).ServeHTTP(w, r)
}

func (a *admin) loginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.identity(r); ok {
		http.Redirect(w, r, "/admin/", http.StatusSeeOther)
		return
	}
	a.render(w, "login", http.StatusOK, pageData{Title: "Sign in"})
}

func (a *admin) login(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return
	}
	if a.auth == nil {
		http.Redirect(w, r, "/admin/", http.StatusSeeOther)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	key := strings.TrimSpace(r.PostFormValue("key"))
	id, ok := a.auth.Lookup(key)
	if !ok {
		time.Sleep(300 * time.Millisecond) // keys are long random strings; this only slows down scripts
		slog.Warn("admin sign-in failed", "remote", r.RemoteAddr)
		a.render(w, "login", http.StatusUnauthorized, pageData{Title: "Sign in", Error: "That API key is not valid. Use the key you give to qsrv clients."})
		return
	}
	sid := a.sessions.create(sha256.Sum256([]byte(key)), time.Now())
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: sid, Path: "/admin", MaxAge: int(sessionTTL.Seconds()),
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: isHTTPS(r),
	})
	slog.Info("admin sign-in", "tenant", id.Tenant, "key", id.Name, "remote", r.RemoteAddr)
	http.Redirect(w, r, "/admin/", http.StatusSeeOther)
}

func (a *admin) logout(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return
	}
	if c, err := r.Cookie(sessionCookie); err == nil {
		a.sessions.delete(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/admin", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: isHTTPS(r)})
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}

func isHTTPS(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// sameOrigin refuses form posts from other sites (CSRF), on top of the
// SameSite=Strict cookie: browsers send Origin on POST, and Sec-Fetch-Site.
func sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" || origin == "null" {
		return origin == "" // no Origin: not a browser form post from elsewhere
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}
