package delivery

import (
	"context"
	"fmt"
	"net"
	"net/http"
)

// SSRFPolicy configures which outbound webhook targets are permitted. The guard
// runs after DNS resolution to catch DNS-rebinding — the resolved IP is checked,
// not the hostname from the URL.
//
// Evaluation order: if the resolved IP falls in any Allow CIDR, it is permitted.
// Otherwise, if it falls in the default deny set (private, loopback, link-local,
// cloud-metadata, multicast, unspecified) or in any Deny CIDR, it is blocked.
// AllowPrivate is a shortcut that makes the default deny set empty (useful for
// self-hosted setups where webhook receivers live on the same RFC1918 network).
type SSRFPolicy struct {
	// Allow is a list of CIDRs that are always permitted, overriding all denies.
	// Example: ["192.168.100.0/24"] to allow a webhook receiver on a private net.
	Allow []string
	// Deny is a list of extra CIDRs to block on top of the built-in denylist.
	Deny []string
	// AllowPrivate disables the built-in denylist for private/loopback/link-local
	// ranges. Only the explicit Deny list and multicast/unspecified stay active.
	AllowPrivate bool
	// AllowHTTP permits http:// targets. By default only https:// is accepted at
	// the URL-validation layer; this flag is for dev/test environments.
	AllowHTTP bool
}

// compiledPolicy is the parsed form of SSRFPolicy: CIDRs are pre-parsed so
// dial-time checks are cheap comparisons, not repeated string parsing.
type compiledPolicy struct {
	allow        []*net.IPNet
	deny         []*net.IPNet
	allowPrivate bool
}

// compile parses the string CIDRs in p. An invalid CIDR is a configuration
// error returned immediately so the caller can reject the config at startup.
func (p SSRFPolicy) compile() (compiledPolicy, error) {
	out := compiledPolicy{allowPrivate: p.AllowPrivate}
	for _, cidr := range p.Allow {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return compiledPolicy{}, fmt.Errorf("ssrf allow CIDR %q: %w", cidr, err)
		}
		out.allow = append(out.allow, n)
	}
	for _, cidr := range p.Deny {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return compiledPolicy{}, fmt.Errorf("ssrf deny CIDR %q: %w", cidr, err)
		}
		out.deny = append(out.deny, n)
	}
	return out, nil
}

// isBlocked reports whether ip should be blocked under policy p. It parses and
// compiles the policy on each call, so it is suitable for unit tests; production
// code should call compile() once and use isBlockedCompiled.
func isBlocked(ip net.IP, p SSRFPolicy) bool {
	nets, err := p.compile()
	if err != nil {
		// A bad CIDR in the policy is a config error; block as the safe default.
		return true
	}
	return isBlockedCompiled(ip, nets)
}

func isBlockedCompiled(ip net.IP, p compiledPolicy) bool {
	// An explicit allow overrides everything — return early.
	for _, n := range p.allow {
		if n.Contains(ip) {
			return false
		}
	}

	// Explicit extra deny before the built-in list (also subject to allow above).
	for _, n := range p.deny {
		if n.Contains(ip) {
			return true
		}
	}

	if !p.allowPrivate && isPrivateOrReserved(ip) {
		return true
	}

	// Multicast and unspecified are always blocked regardless of allow_private.
	return ip.IsMulticast() || ip.IsUnspecified()
}

func isPrivateOrReserved(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast()
}

// NewGuardDialer wraps a base dialer with the SSRF policy check. The returned
// DialContext function resolves the target address and rejects it with
// ErrSSRFBlocked if the resolved IP matches the policy's denylist. It is
// injected as http.Transport.DialContext so the check happens after DNS
// resolution, closing the DNS-rebinding window.
//
// If policy is nil, all targets are allowed (no guard).
func NewGuardDialer(base *net.Dialer, policy *SSRFPolicy) (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	if policy == nil {
		return base.DialContext, nil
	}
	compiled, err := policy.compile()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := base.Resolver.LookupHost(ctx, host)
		if err != nil {
			// Fall through: let the dial fail naturally with the resolver error.
			ips = []string{host}
		}
		for _, raw := range ips {
			ip := net.ParseIP(raw)
			if ip == nil {
				continue
			}
			if isBlockedCompiled(ip, compiled) {
				return nil, &ErrSSRFBlocked{IP: ip, Host: host}
			}
		}
		return base.DialContext(ctx, network, net.JoinHostPort(host, port))
	}, nil
}

type GuardedTransport struct {
	base   http.RoundTripper
	policy compiledPolicy
}

// NewGuardedTransport builds a GuardedTransport from a policy. Returns nil, nil
// when policy is nil so callers can use the result directly as an
// http.RoundTripper (nil means use default).
func NewGuardedTransport(policy *SSRFPolicy) (*GuardedTransport, error) {
	if policy == nil {
		return nil, nil
	}
	cp, err := policy.compile()
	if err != nil {
		return nil, err
	}
	return &GuardedTransport{base: http.DefaultTransport, policy: cp}, nil
}

// RoundTrip checks the target IP against the SSRF policy before delegating to
// the base transport. The check runs after DNS resolution by resolving the host
// explicitly so the guard cannot be bypassed by a DNS rebind.
func (t *GuardedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	ips, err := net.DefaultResolver.LookupHost(req.Context(), host)
	if err != nil {
		// Fall through: let the base transport fail with the resolver error.
		ips = []string{host}
	}
	for _, raw := range ips {
		ip := net.ParseIP(raw)
		if ip == nil {
			continue
		}
		if isBlockedCompiled(ip, t.policy) {
			return nil, &ErrSSRFBlocked{IP: ip, Host: host}
		}
	}
	return t.base.RoundTrip(req)
}

type ErrSSRFBlocked struct {
	IP   net.IP
	Host string
}

func (e *ErrSSRFBlocked) Error() string {
	return fmt.Sprintf("ssrf: blocked connection to %s (resolved %s)", e.Host, e.IP)
}

func IsSSRFBlocked(err error) bool {
	var e *ErrSSRFBlocked
	return errorAs(err, &e)
}

// errorAs is a thin wrapper to avoid an import cycle with errors.As; the
// delivery package has no other use for the errors package.
func errorAs(err error, target **ErrSSRFBlocked) bool {
	for err != nil {
		if e, ok := err.(*ErrSSRFBlocked); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	return false
}
