package httpapi

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/romycode/qsrv/internal/queue"
	"github.com/romycode/qsrv/pkg/api"
)

const minKeyLen = 32

// AdminTenant in a key entry gives the key access to every tenant.
const AdminTenant = "*"

var validKeyName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,40}$`)

// Identity is who made a request: the tenant and name of its API key.
type Identity struct {
	Tenant string // "" for an admin key
	Name   string // key name, for logs
}

// Admin keys see every tenant and use full "tenant/name" names.
func (id Identity) Admin() bool { return id.Tenant == "" }

// KeyEntry is one configured API key: "tenant:name:secret".
type KeyEntry struct {
	Identity
	Secret string
}

// ParseKeyEntry parses "tenant:name:secret". tenant "*" makes an admin key.
func ParseKeyEntry(s string) (KeyEntry, error) {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return KeyEntry{}, errors.New("api key entry must be tenant:name:key (tenant * = admin), e.g. acme:billing:$(openssl rand -hex 32)")
	}
	tenant, name, secret := parts[0], parts[1], parts[2]
	switch {
	case tenant != AdminTenant && !queue.ValidTenant(tenant):
		return KeyEntry{}, fmt.Errorf("invalid tenant %q (lowercase letters, digits, - and _, up to 40)", tenant)
	case !validKeyName.MatchString(name):
		return KeyEntry{}, fmt.Errorf("invalid key name %q (letters, digits, ., - and _, up to 40)", name)
	case len(secret) < minKeyLen:
		return KeyEntry{}, fmt.Errorf("key %s:%s too short (%d chars, min %d); generate one with: openssl rand -hex 32", tenant, name, len(secret), minKeyLen)
	}
	if tenant == AdminTenant {
		tenant = ""
	}
	return KeyEntry{Identity: Identity{Tenant: tenant, Name: name}, Secret: secret}, nil
}

// Authenticator checks "Authorization: Bearer <key>" against the configured
// keys and resolves the key's identity. Keys are compared as SHA-256
// digests in constant time, against every configured key, so timing leaks
// neither content nor length. The key set can be swapped at runtime
// (Replace) without dropping requests.
type Authenticator struct {
	keys atomic.Pointer[[]digestEntry]
}

type digestEntry struct {
	digest [sha256.Size]byte
	id     Identity
}

func NewAuthenticator(entries []KeyEntry) (*Authenticator, error) {
	a := &Authenticator{}
	if err := a.Replace(entries); err != nil {
		return nil, err
	}
	return a, nil
}

// Replace swaps the key set atomically. On error the current set is kept.
func (a *Authenticator) Replace(entries []KeyEntry) error {
	if len(entries) == 0 {
		return errors.New("no api keys configured")
	}
	seen := map[[sha256.Size]byte]bool{}
	keys := make([]digestEntry, 0, len(entries))
	for _, e := range entries {
		d := sha256.Sum256([]byte(e.Secret))
		if seen[d] {
			return fmt.Errorf("api key %s:%s is configured twice", e.Tenant, e.Name)
		}
		seen[d] = true
		keys = append(keys, digestEntry{digest: d, id: e.Identity})
	}
	a.keys.Store(&keys)
	return nil
}

// Lookup returns the identity of key.
func (a *Authenticator) Lookup(key string) (Identity, bool) {
	d := sha256.Sum256([]byte(key))
	var id Identity
	found := 0
	for _, e := range *a.keys.Load() {
		match := subtle.ConstantTimeCompare(d[:], e.digest[:])
		if match == 1 {
			id = e.id
		}
		found |= match
	}
	return id, found == 1
}

// Len is the number of configured keys.
func (a *Authenticator) Len() int { return len(*a.keys.Load()) }

// ParseKeys reads key entries, one per line (or comma separated); blank
// lines and # comments are ignored.
func ParseKeys(r io.Reader) ([]KeyEntry, error) {
	var out []KeyEntry
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		for _, field := range strings.Split(sc.Text(), ",") {
			field = strings.TrimSpace(field)
			if field == "" || strings.HasPrefix(field, "#") {
				break
			}
			e, err := ParseKeyEntry(field)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", n, err)
			}
			out = append(out, e)
		}
	}
	return out, sc.Err()
}

// LoadKeysFile reads key entries from path (see ParseKeys).
func LoadKeysFile(path string) ([]KeyEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseKeys(f)
}

type identityKey struct{}

// IdentityFrom returns the identity of the request's API key. Without auth
// (-insecure-no-auth) every request is an admin.
func IdentityFrom(ctx context.Context) Identity {
	if h, ok := ctx.Value(identityKey{}).(*Identity); ok {
		return *h
	}
	return Identity{Name: "anonymous"}
}

// withIdentitySlot puts a mutable identity in the context, so the access log
// (outside auth) can log who made the request.
func withIdentitySlot(r *http.Request) (*http.Request, *Identity) {
	if h, ok := r.Context().Value(identityKey{}).(*Identity); ok {
		return r, h
	}
	h := &Identity{Name: "anonymous"}
	return r.WithContext(context.WithValue(r.Context(), identityKey{}, h)), h
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		id, ok := a.Lookup(key)
		if !found || !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="qsrv"`)
			writeAPIErr(w, http.StatusUnauthorized, api.CodeUnauthorized, "missing or invalid api key")
			return
		}
		inner, slot := withIdentitySlot(r)
		*slot = id
		next.ServeHTTP(w, inner)
		r.Pattern = inner.Pattern
	})
}
