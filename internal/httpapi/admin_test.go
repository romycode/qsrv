package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/romycode/qsrv/internal/httpapi"
	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
	"github.com/romycode/qsrv/pkg/client"
)

func adminServer(t *testing.T, auth *httpapi.Authenticator, admin bool) *httptest.Server {
	t.Helper()
	b, err := queue.Open(t.TempDir(), queue.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.New(b, httpapi.Options{Auth: auth, Admin: admin, Version: "test"}))
	t.Cleanup(func() { srv.Close(); b.Close() })
	return srv
}

// browser is an HTTP client that keeps cookies and doesn't follow redirects.
func browser(t *testing.T) *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func signIn(t *testing.T, c *http.Client, base, key string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/admin/login", strings.NewReader(url.Values{"key": {key}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", base)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func get(t *testing.T, c *http.Client, u string) (*http.Response, string) {
	t.Helper()
	resp, err := c.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestAdminSignInAndScope(t *testing.T) {
	auth, _ := httpapi.NewAuthenticator([]httpapi.KeyEntry{
		entry("acme", "app", acmeKey), entry("globex", "app", globexKey), entry("*", "ops", adminKey),
	})
	srv := adminServer(t, auth, true)
	ctx := context.Background()
	client.New(srv.URL, acmeKey).CreateQueue(ctx, "orders", api.QueueConfig{})
	client.New(srv.URL, acmeKey).Send(ctx, "orders", []byte("hello"), "")
	client.New(srv.URL, globexKey).CreateQueue(ctx, "secret", api.QueueConfig{})

	c := browser(t)
	// Not signed in: pages go to the sign-in page, data answers 401.
	if resp, _ := get(t, c, srv.URL+"/admin/"); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/admin/login" {
		t.Fatalf("anonymous page: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	if resp, _ := get(t, c, srv.URL+"/admin/api/v1/overview"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous api: %d", resp.StatusCode)
	}
	resp, body := get(t, c, srv.URL+"/admin/login")
	if resp.StatusCode != 200 || !strings.Contains(body, `name="key"`) {
		t.Fatalf("login page: %d", resp.StatusCode)
	}
	csp := resp.Header.Get("Content-Security-Policy")
	if !strings.Contains(csp, "frame-ancestors 'none'") || !strings.Contains(csp, "script-src 'self'") || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("security headers: %q", csp)
	}

	// A wrong key: 401 and no cookie.
	if r := signIn(t, c, srv.URL, "wrong-key-wrong-key-wrong-key-wrong"); r.StatusCode != http.StatusUnauthorized || len(r.Cookies()) != 0 {
		t.Fatalf("wrong key: %d %v", r.StatusCode, r.Cookies())
	}

	// A tenant key: session cookie, and only its tenant, in its own names.
	r := signIn(t, c, srv.URL, acmeKey)
	if r.StatusCode != http.StatusSeeOther || r.Header.Get("Location") != "/admin/" {
		t.Fatalf("sign in: %d %s", r.StatusCode, r.Header.Get("Location"))
	}
	ck := r.Cookies()[0]
	if !ck.HttpOnly || ck.SameSite != http.SameSiteStrictMode || ck.Path != "/admin" || ck.Value == acmeKey || len(ck.Value) < 32 {
		t.Fatalf("cookie %+v", ck)
	}
	resp, body = get(t, c, srv.URL+"/admin/api/v1/overview")
	var ov api.Overview
	json.Unmarshal([]byte(body), &ov)
	if resp.StatusCode != 200 || ov.Tenant != "acme" || len(ov.Queues) != 1 || ov.Queues[0].Name != "orders" {
		t.Fatalf("tenant overview: %d %s", resp.StatusCode, body)
	}
	if resp, _ := get(t, c, srv.URL+"/admin/api/v1/queues/secret/messages"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("tenant peeking another tenant's queue: %d", resp.StatusCode)
	}
	if resp, body := get(t, c, srv.URL+"/admin/queue?name="+url.QueryEscape("globex/secret")); resp.StatusCode != http.StatusNotFound || !strings.Contains(body, "There is no queue named") {
		t.Fatalf("tenant opening another tenant's page: %d", resp.StatusCode)
	}
	if resp, body := get(t, c, srv.URL+"/admin/queue?name=orders"); resp.StatusCode != 200 || !strings.Contains(body, `data-queue="orders"`) {
		t.Fatalf("queue page: %d", resp.StatusCode)
	}
	if resp, body := get(t, c, srv.URL+"/admin/"); resp.StatusCode != 200 || !strings.Contains(body, "Tenant acme") {
		t.Fatalf("dashboard: %d", resp.StatusCode)
	}

	// The session can only read: no API write goes through /admin/api.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/admin/api/v1/queues/orders/messages", strings.NewReader(`{"body":"x"}`))
	if resp, _ := c.Do(req); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("write through the admin session: %d", resp.StatusCode)
	}
	// ...and the cookie is not an API credential.
	if resp, _ := get(t, c, srv.URL+"/v1/queues"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("cookie used on /v1: %d", resp.StatusCode)
	}

	// Removing the key (SIGHUP reload) ends its sessions at once.
	auth.Replace([]httpapi.KeyEntry{entry("globex", "app", globexKey), entry("*", "ops", adminKey)})
	if resp, _ := get(t, c, srv.URL+"/admin/api/v1/overview"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("session after its key was removed: %d", resp.StatusCode)
	}

	// An admin key sees every tenant with full names.
	admin := browser(t)
	signIn(t, admin, srv.URL, adminKey)
	_, body = get(t, admin, srv.URL+"/admin/api/v1/overview")
	var all api.Overview
	json.Unmarshal([]byte(body), &all)
	if all.Tenant != "" || len(all.Queues) != 2 || all.Queues[0].Name != "acme/orders" {
		t.Fatalf("admin overview: %s", body)
	}

	// Sign out ends the session.
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/admin/logout", nil)
	req.Header.Set("Origin", srv.URL)
	if resp, _ := admin.Do(req); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("sign out: %d", resp.StatusCode)
	}
	if resp, _ := get(t, admin, srv.URL+"/admin/api/v1/overview"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("after sign out: %d", resp.StatusCode)
	}
}

func TestAdminRefusesCrossSiteForms(t *testing.T) {
	auth, _ := httpapi.NewAuthenticator([]httpapi.KeyEntry{entry("*", "ops", adminKey)})
	srv := adminServer(t, auth, true)
	for _, h := range []map[string]string{{"Origin": "https://evil.example"}, {"Sec-Fetch-Site": "cross-site"}} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/admin/login", strings.NewReader("key="+adminKey))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		for k, v := range h {
			req.Header.Set(k, v)
		}
		resp, err := browser(t).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusForbidden || len(resp.Cookies()) != 0 {
			t.Fatalf("cross-site sign in with %v: %d", h, resp.StatusCode)
		}
	}
}

func TestAdminAssetsAndSwitch(t *testing.T) {
	srv := adminServer(t, nil, true) // no auth: everyone is an admin, no sign-in
	c := browser(t)
	if resp, body := get(t, c, srv.URL+"/admin/"); resp.StatusCode != 200 || !strings.Contains(body, "Authentication is off") {
		t.Fatalf("no-auth dashboard: %d", resp.StatusCode)
	}
	for _, f := range []string{"app.js", "app.css", "theme.js", "alpine-3.17.4.min.js", "icon.svg"} {
		resp, body := get(t, c, srv.URL+"/admin/static/"+f+"?v=test")
		if resp.StatusCode != 200 || len(body) == 0 || !strings.Contains(resp.Header.Get("Cache-Control"), "max-age") {
			t.Fatalf("%s: %d %q", f, resp.StatusCode, resp.Header.Get("Cache-Control"))
		}
	}
	if resp, _ := get(t, c, srv.URL+"/admin"); resp.StatusCode != http.StatusFound {
		t.Fatalf("/admin redirect: %d", resp.StatusCode)
	}

	off := adminServer(t, nil, false)
	if resp, _ := get(t, browser(t), off.URL+"/admin/"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("admin disabled: %d", resp.StatusCode)
	}
}
