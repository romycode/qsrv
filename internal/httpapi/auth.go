package httpapi

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/romycode/qsrv/pkg/api"
)

const minKeyLen = 32

// Authenticator checks "Authorization: Bearer <key>" against a set of keys.
// Keys are compared as SHA-256 digests with constant-time comparison, and
// every configured key is checked, so timing leaks neither content nor length.
type Authenticator struct {
	digests [][sha256.Size]byte
}

func NewAuthenticator(keys []string) (*Authenticator, error) {
	a := &Authenticator{}
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		if len(k) < minKeyLen {
			return nil, fmt.Errorf("api key too short (%d chars, min %d); generate one with: openssl rand -hex 32", len(k), minKeyLen)
		}
		a.digests = append(a.digests, sha256.Sum256([]byte(k)))
	}
	if len(a.digests) == 0 {
		return nil, errors.New("no api keys configured")
	}
	return a, nil
}

// LoadKeysFile reads one key per line; blank lines and # comments are ignored.
func LoadKeysFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var keys []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		keys = append(keys, line)
	}
	return keys, sc.Err()
}

func (a *Authenticator) Valid(key string) bool {
	d := sha256.Sum256([]byte(key))
	ok := 0
	for i := range a.digests {
		ok |= subtle.ConstantTimeCompare(d[:], a.digests[i][:])
	}
	return ok == 1
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !found || !a.Valid(key) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="qsrv"`)
			writeAPIErr(w, http.StatusUnauthorized, api.CodeUnauthorized, "missing or invalid api key")
			return
		}
		next.ServeHTTP(w, r)
	})
}
