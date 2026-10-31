package queue

import (
	"fmt"
	"regexp"
	"strings"
)

// Multitenancy. Queue and topic names are "tenant/name". The HTTP API scopes
// every request to the tenant of its API key, so tenants never see or reach
// each other's queues; the broker additionally refuses any reference across
// tenants (a dead letter queue, a topic subscription, a replay destination).
//
// The broker itself also accepts names without a tenant (tests, embedders);
// the HTTP API never creates them.

// TenantSep separates the tenant from the queue or topic name.
const TenantSep = "/"

var (
	validTenant = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,39}$`)
	validLocal  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,80}$`)
)

// ValidTenant reports whether t can be used as a tenant name: lowercase
// letters, digits, '-' and '_', starting with a letter or digit, up to 40.
func ValidTenant(t string) bool { return validTenant.MatchString(t) }

// ValidLocalName reports whether n is a valid queue or topic name inside a
// tenant: letters, digits, '.', '-' and '_', up to 80.
func ValidLocalName(n string) bool { return validLocal.MatchString(n) }

// SplitName splits "tenant/name" into its parts; tenant is "" without one.
func SplitName(full string) (tenant, name string) {
	if t, n, ok := strings.Cut(full, TenantSep); ok {
		return t, n
	}
	return "", full
}

// JoinName builds the full name of name in tenant.
func JoinName(tenant, name string) string {
	if tenant == "" {
		return name
	}
	return tenant + TenantSep + name
}

func validFullName(full string) bool {
	t, n := SplitName(full)
	return (t == "" && !strings.Contains(full, TenantSep) || ValidTenant(t)) && ValidLocalName(n)
}

func sameTenant(a, b string) bool {
	ta, _ := SplitName(a)
	tb, _ := SplitName(b)
	return ta == tb
}

func crossTenant(what, from, to string) error {
	return fmt.Errorf("%w: %s %q is in another tenant than %q", ErrInvalidConfig, what, to, from)
}
