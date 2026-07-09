package delivery

import (
	"net"
	"testing"
)

func TestIsBlocked(t *testing.T) {
	defaultPolicy := SSRFPolicy{}

	tests := []struct {
		name    string
		ip      string
		policy  SSRFPolicy
		blocked bool
	}{
		// Default denylist — blocked
		{"loopback v4", "127.0.0.1", defaultPolicy, true},
		{"loopback range", "127.0.0.2", defaultPolicy, true},
		{"loopback v6", "::1", defaultPolicy, true},
		{"private 10/8", "10.0.0.1", defaultPolicy, true},
		{"private 10/8 edge", "10.255.255.255", defaultPolicy, true},
		{"private 172.16/12", "172.16.0.1", defaultPolicy, true},
		{"private 172.31.x.x", "172.31.255.255", defaultPolicy, true},
		{"private 192.168/16", "192.168.1.1", defaultPolicy, true},
		{"link-local v4", "169.254.0.1", defaultPolicy, true},
		{"cloud metadata", "169.254.169.254", defaultPolicy, true},
		{"link-local v6", "fe80::1", defaultPolicy, true},
		{"unique local v6", "fc00::1", defaultPolicy, true},
		{"unique local v6 fd", "fd00::1", defaultPolicy, true},
		{"unspecified v4", "0.0.0.0", defaultPolicy, true},
		{"unspecified v6", "::", defaultPolicy, true},

		// Public IPs — allowed by default
		{"public v4", "8.8.8.8", defaultPolicy, false},
		{"public v4 other", "1.2.3.4", defaultPolicy, false},
		{"public v6", "2001:4860:4860::8888", defaultPolicy, false},

		// allow_private overrides the RFC1918/loopback deny
		{"private allow_private", "192.168.1.1", SSRFPolicy{AllowPrivate: true}, false},
		{"loopback allow_private", "127.0.0.1", SSRFPolicy{AllowPrivate: true}, false},

		// deny extra CIDRs
		{"extra deny CIDR", "203.0.113.5", SSRFPolicy{Deny: []string{"203.0.113.0/24"}}, true},
		{"extra deny not in range", "203.0.114.1", SSRFPolicy{Deny: []string{"203.0.113.0/24"}}, false},

		// allow overrides default deny (even loopback)
		{"allow overrides deny loopback", "127.0.0.1", SSRFPolicy{Allow: []string{"127.0.0.1/32"}}, false},
		{"allow overrides deny private range", "192.168.5.5", SSRFPolicy{Allow: []string{"192.168.5.0/24"}}, false},
		{"allow overrides extra deny", "203.0.113.5", SSRFPolicy{Allow: []string{"203.0.113.0/24"}, Deny: []string{"203.0.113.0/24"}}, false},

		// multicast
		{"multicast v4", "224.0.0.1", defaultPolicy, true},
		{"multicast v6", "ff02::1", defaultPolicy, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			if ip == nil {
				t.Fatalf("invalid IP: %s", tt.ip)
			}
			got := isBlocked(ip, tt.policy)
			if got != tt.blocked {
				t.Errorf("isBlocked(%s, %+v) = %v, want %v", tt.ip, tt.policy, got, tt.blocked)
			}
		})
	}
}

func TestSSRFPolicyParseCIDRs(t *testing.T) {
	p := SSRFPolicy{
		Allow: []string{"10.1.0.0/16"},
		Deny:  []string{"10.1.1.0/24"},
	}
	nets, err := p.compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// 10.1.2.5 is in allow (10.1.0.0/16 covers it) but not in deny → not blocked
	ip := net.ParseIP("10.1.2.5")
	if isBlockedCompiled(ip, nets) {
		t.Error("10.1.2.5 should be allowed by allow CIDR override")
	}
	// 10.1.1.5 is in allow but also in extra deny; deny wins when not in allow list explicitly?
	// No: allow is checked first and overrides deny.
	ip2 := net.ParseIP("10.1.1.5")
	if isBlockedCompiled(ip2, nets) {
		t.Error("10.1.1.5 should be allowed because allow CIDR covers it (allow wins over deny)")
	}
}
