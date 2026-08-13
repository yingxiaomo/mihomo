//go:build with_naive_cronet

package outbound

import (
	"context"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/component/resolver"

	mDNS "github.com/miekg/dns"
)

// fakeResolver is a resolver.Resolver whose only meaningful method is
// ExchangeContext; the rest satisfy the interface.
type fakeResolver struct {
	exchange func(ctx context.Context, m *mDNS.Msg) (*mDNS.Msg, error)
}

func (f *fakeResolver) ExchangeContext(ctx context.Context, m *mDNS.Msg) (*mDNS.Msg, error) {
	return f.exchange(ctx, m)
}
func (*fakeResolver) LookupIP(context.Context, string) ([]netip.Addr, error)   { return nil, nil }
func (*fakeResolver) LookupIPv4(context.Context, string) ([]netip.Addr, error) { return nil, nil }
func (*fakeResolver) LookupIPv6(context.Context, string) ([]netip.Addr, error) { return nil, nil }
func (*fakeResolver) ResolveECH(context.Context, string) ([]byte, error)       { return nil, nil }
func (*fakeResolver) Invalid() bool                                            { return false }
func (*fakeResolver) ClearCache()                                              {}
func (*fakeResolver) ResetConnection()                                         {}

func aQuery(name string) *mDNS.Msg {
	m := &mDNS.Msg{}
	m.SetQuestion(mDNS.Fqdn(name), mDNS.TypeA)
	return m
}

// withResolvers swaps the resolver globals for the duration of a test and
// restores them afterwards (tests in a package run sequentially).
func withResolvers(t *testing.T, proxyHost, def, system resolver.Resolver) {
	t.Helper()
	sp, sd, ss := resolver.ProxyServerHostResolver, resolver.DefaultResolver, resolver.SystemResolver
	t.Cleanup(func() {
		resolver.ProxyServerHostResolver = sp
		resolver.DefaultResolver = sd
		resolver.SystemResolver = ss
	})
	resolver.ProxyServerHostResolver = proxyHost
	resolver.DefaultResolver = def
	resolver.SystemResolver = system
}

// TestNaiveDNSResolverPrefersProxyServerHostResolver checks the happy path: the
// query is answered through mihomo's resolver and its response is returned.
func TestNaiveDNSResolverPrefersProxyServerHostResolver(t *testing.T) {
	want := aQuery("example.com")
	want.Response = true
	called := false
	fake := &fakeResolver{exchange: func(context.Context, *mDNS.Msg) (*mDNS.Msg, error) {
		called = true
		return want, nil
	}}
	// DefaultResolver would answer differently; it must not be consulted.
	other := &fakeResolver{exchange: func(context.Context, *mDNS.Msg) (*mDNS.Msg, error) {
		t.Error("DefaultResolver should not be used when ProxyServerHostResolver succeeds")
		return nil, nil
	}}
	withResolvers(t, fake, other, nil)

	got := naiveDNSResolver(context.Background(), aQuery("example.com"))
	if !called {
		t.Fatal("ProxyServerHostResolver was not used")
	}
	if got != want {
		t.Fatal("expected the resolver's response to be returned verbatim")
	}
}

// TestNaiveDNSResolverFallsThroughOnError checks that a transport error on the
// first resolver falls through to the next one.
func TestNaiveDNSResolverFallsThroughOnError(t *testing.T) {
	want := aQuery("example.com")
	want.Response = true
	failing := &fakeResolver{exchange: func(context.Context, *mDNS.Msg) (*mDNS.Msg, error) {
		return nil, context.DeadlineExceeded
	}}
	answering := &fakeResolver{exchange: func(context.Context, *mDNS.Msg) (*mDNS.Msg, error) {
		return want, nil
	}}
	withResolvers(t, failing, answering, nil)

	got := naiveDNSResolver(context.Background(), aQuery("example.com"))
	if got != want {
		t.Fatal("expected fall-through to DefaultResolver on transport error")
	}
}

// TestNaiveDNSResolverServfailsWithoutResolvers checks that the function never
// returns nil (cronet's DNS server would mishandle a nil response) — it returns
// a SERVFAIL when no resolver is available.
func TestNaiveDNSResolverServfailsWithoutResolvers(t *testing.T) {
	withResolvers(t, nil, nil, nil)

	got := naiveDNSResolver(context.Background(), aQuery("example.com"))
	if got == nil {
		t.Fatal("resolver must never return nil")
	}
	if got.Rcode != mDNS.RcodeServerFailure {
		t.Fatalf("expected SERVFAIL, got rcode %d", got.Rcode)
	}
}
