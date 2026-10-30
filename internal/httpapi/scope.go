package httpapi

import (
	"net/http"
	"strings"

	"github.com/romycode/qsrv/internal/queue"
)

// scope maps names between the API and the broker for one request.
//
// A tenant key works inside its tenant: it sends and sees "orders", the
// broker stores "acme/orders". It can't name anything outside its tenant, so
// another tenant's queue looks exactly like one that doesn't exist (404).
// An admin key sees every tenant and uses full names ("acme/orders").
type scope struct {
	tenant string // "" = admin
}

func scopeOf(r *http.Request) scope { return scope{tenant: IdentityFrom(r.Context()).Tenant} }

// full resolves an API name into the broker's full name.
func (sc scope) full(name string) (string, bool) {
	if sc.tenant != "" {
		if !queue.ValidLocalName(name) {
			return "", false
		}
		return queue.JoinName(sc.tenant, name), true
	}
	t, n := queue.SplitName(name)
	if !queue.ValidTenant(t) || !queue.ValidLocalName(n) {
		return "", false
	}
	return name, true
}

// local is how a broker name is shown to this scope.
func (sc scope) local(full string) string {
	if sc.tenant == "" || full == "" {
		return full
	}
	_, n := queue.SplitName(full)
	return n
}

func (sc scope) locals(full []string) []string {
	out := make([]string, len(full))
	for i, n := range full {
		out[i] = sc.local(n)
	}
	return out
}

// owns reports whether this scope can see the broker name.
func (sc scope) owns(full string) bool {
	if sc.tenant == "" {
		return true
	}
	t, _ := queue.SplitName(full)
	return t == sc.tenant
}

// message removes this tenant's prefix from broker error text.
func (sc scope) message(msg string) string {
	if sc.tenant == "" {
		return msg
	}
	return strings.ReplaceAll(msg, sc.tenant+queue.TenantSep, "")
}

// nameHelp explains the name format for this scope in errors.
func (sc scope) nameHelp() string {
	if sc.tenant == "" {
		return "admin keys use full names: tenant/name"
	}
	return "names: letters, digits, '.', '-' and '_', up to 80"
}
