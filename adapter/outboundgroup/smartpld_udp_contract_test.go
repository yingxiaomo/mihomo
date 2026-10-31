package outboundgroup

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/proxydialer"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	RC "github.com/metacubex/mihomo/rules/common"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type udpContractAdapter struct {
	*outbound.Base
	calls   atomic.Int32
	nilConn bool
}

func newUDPContractProxy(name string, udp bool) (*udpContractAdapter, C.Proxy) {
	p := &udpContractAdapter{Base: outbound.NewBase(outbound.BaseOption{Name: name, Type: C.Socks5, UDP: udp})}
	return p, adapter.NewProxy(p)
}

func (p *udpContractAdapter) ListenPacketContext(ctx context.Context, md *C.Metadata) (C.PacketConn, error) {
	p.calls.Add(1)
	if p.nilConn {
		return nil, nil
	}
	if !p.SupportUDP() {
		return nil, C.ErrNotSupport
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return outbound.NewPacketConn(pc, p), nil
}

func assertUDPContractUnavailable(t *testing.T, s *SmartPLD, md *C.Metadata, wantNoCandidates bool) {
	t.Helper()
	p := s.Unwrap(md, false)
	if p == nil || p.SupportUDP() || p.IsL3Protocol(md) || p.Unwrap(md, false) != nil {
		t.Fatalf("unavailable unwrap is not a safe terminal adapter: %v", p)
	}
	if p.Type() == C.Pass || p.Type() == C.PassRule || p.Type() == C.Rematch || p.Type() == C.Direct {
		t.Fatalf("unavailable adapter changes the rule: %s", p.Type())
	}
	if s.IsL3Protocol(md) {
		t.Fatal("unavailable group reported L3")
	}
	pc, err := p.ListenPacketContext(context.Background(), md)
	if pc != nil || !errors.Is(err, C.ErrNotSupport) || (wantNoCandidates && !errors.Is(err, ErrSmartPLDNoCandidates)) {
		t.Fatalf("unwrap packet=%v error=%v", pc, err)
	}
	conn, err := p.DialContext(context.Background(), md)
	if conn != nil || err == nil {
		t.Fatalf("unavailable dial=%v error=%v", conn, err)
	}
	pc, err = adapter.NewProxy(s).ListenPacketContext(context.Background(), md)
	if pc != nil || !errors.Is(err, C.ErrNotSupport) || (wantNoCandidates && !errors.Is(err, ErrSmartPLDNoCandidates)) {
		t.Fatalf("group packet=%v error=%v", pc, err)
	}
}

func TestPLDUDPContractAllTCPOnly(t *testing.T) {
	for _, fixture := range []string{"outcome", "live-cache"} {
		t.Run(fixture, func(t *testing.T) {
			a, pa := newUDPContractProxy("tcp-A", false)
			b, pb := newUDPContractProxy("tcp-B", false)
			var s *SmartPLD
			var md *C.Metadata
			if fixture == "outcome" {
				s, _, md = newOutcomeTestGroup(t)
				s.GroupBase = NewGroupBase(GroupBaseOption{Name: s.Name(), Type: C.SmartPLD, Providers: []P.ProxyProvider{&cacheMutableProvider{proxies: []C.Proxy{pa, pb}, version: 1}}})
			} else {
				s, _ = liveCacheFixture(t, liveCacheStore(t), "udp-cfg", "udp-grp", []C.Proxy{pa, pb})
				md = &C.Metadata{Host: "verify.example", SmartTarget: "verify.example", DstIPASN: "unknown"}
			}
			md.NetWork = C.UDP
			md.DstIP = netip.MustParseAddr("127.0.0.1")
			md.DstPort = 53
			// Include a stale TCP pin: a cache hit may never bypass UDP eligibility.
			s.store.ForceStoreUnwrapResult(s.Name(), s.configName, md.SmartTarget, []C.Proxy{pa})
			assertUDPContractUnavailable(t, s, md, true)
			if a.calls.Load() != 0 || b.calls.Load() != 0 {
				t.Fatal("UDP attempted a TCP-only node")
			}
			// A regular TCP unwrap remains the existing selected/pinned node.
			md.NetWork = C.TCP
			if got := s.Unwrap(md, false); got != pa {
				t.Fatalf("TCP routing changed: %v", got)
			}
		})
	}
}

func TestPLDUDPContractNilMetadataAndEmptyProvider(t *testing.T) {
	for _, provider := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-provider", true: "empty-provider"}[provider], func(t *testing.T) {
			s, _, md := newOutcomeTestGroup(t)
			if provider {
				s.GroupBase = NewGroupBase(GroupBaseOption{Name: s.Name(), Type: C.SmartPLD, Providers: []P.ProxyProvider{&cacheMutableProvider{version: 1}}})
			}
			md.NetWork = C.UDP
			assertUDPContractUnavailable(t, s, md, true)
			if p := s.Unwrap(nil, false); p == nil || p.Unwrap(nil, false) != nil || s.IsL3Protocol(nil) {
				t.Fatal("empty nil-metadata unwrap is unsafe")
			}
			if pc, err := s.ListenPacketContext(context.Background(), nil); pc != nil || !errors.Is(err, C.ErrNotSupport) || !strings.Contains(err.Error(), "metadata is nil") {
				t.Fatalf("nil metadata packet=%v error=%v", pc, err)
			}
		})
	}
	s, p, _ := newOutcomeTestGroup(t)
	s.GroupBase = NewGroupBase(GroupBaseOption{Name: s.Name(), Type: C.SmartPLD, Providers: []P.ProxyProvider{&cacheMutableProvider{proxies: []C.Proxy{p}, version: 1}}})
	if s.Unwrap(nil, false) != p {
		t.Fatal("nonempty nil-metadata inspection changed")
	}
}

func udpContractRoundTrip(t *testing.T, s *SmartPLD, md *C.Metadata, node string) {
	t.Helper()
	s.sampleRate = 1
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	md.DstIP = netip.MustParseAddr("127.0.0.1")
	md.DstPort = uint16(server.LocalAddr().(*net.UDPAddr).Port)
	pc, err := adapter.NewProxy(s).ListenPacketContext(context.Background(), md)
	if err != nil {
		t.Fatal(err)
	}
	pc = statistic.NewUDPTracker(pc, statistic.DefaultManager, md, nil, 0, 0, true)
	defer pc.Close()
	deadline := time.Now().Add(3 * time.Second)
	_ = server.SetDeadline(deadline)
	_ = pc.SetDeadline(deadline)
	if _, err = pc.WriteTo([]byte("request"), md.UDPAddr()); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	_, peer, err := server.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.WriteTo([]byte("reply"), peer); err != nil {
		t.Fatal(err)
	}
	n, _, err := pc.ReadFrom(buffer)
	if err != nil || string(buffer[:n]) != "reply" {
		t.Fatalf("UDP reply=%q error=%v", buffer[:n], err)
	}
	if pc.Chains().Last() != node || pc.Chains()[len(pc.Chains())-1] != s.Name() {
		t.Fatalf("route chains=%v", pc.Chains())
	}
	if err = pc.Close(); err != nil {
		t.Fatal(err)
	}
	waitTransportStats(t, s, md, node)
}

func TestPLDUDPContractMixedAndManualSelection(t *testing.T) {
	for _, manual := range []string{"", "tcp-A", "udp-B"} {
		t.Run("manual="+manual, func(t *testing.T) {
			a, pa := newUDPContractProxy("tcp-A", false)
			b, pb := newUDPContractProxy("udp-B", true)
			s, _ := liveCacheFixture(t, liveCacheStore(t), "udp-cfg", "udp-mixed", []C.Proxy{pa, pb})
			s.selected = manual
			md := &C.Metadata{NetWork: C.UDP, Host: "verify.example", SmartTarget: "verify.example", DstIPASN: "unknown"}
			if manual == "tcp-A" {
				assertUDPContractUnavailable(t, s, md, false)
				if s.selected != manual || a.calls.Load() != 0 || b.calls.Load() != 0 {
					t.Fatal("unsupported manual choice was replaced or dialed")
				}
				md.NetWork = C.TCP
				if s.Unwrap(md, false) != pa {
					t.Fatal("manual TCP selection changed")
				}
				return
			}
			if s.Unwrap(md, false) != pb {
				t.Fatal("UDP unwrap chose TCP-only node")
			}
			udpContractRoundTrip(t, s, md, pb.Name())
			if a.calls.Load() != 0 || b.calls.Load() != 1 {
				t.Fatalf("attempts TCP=%d UDP=%d", a.calls.Load(), b.calls.Load())
			}
		})
	}
}

func TestPLDUDPContractFilteredTCPOnlyAndDisabled(t *testing.T) {
	for _, state := range []string{"blocked", "dead", "cooldown", "disabled"} {
		t.Run(state, func(t *testing.T) {
			a, pa := newUDPContractProxy("tcp-A", false)
			s, _ := liveCacheFixture(t, liveCacheStore(t), "cfg", "filtered", []C.Proxy{pa})
			md := &C.Metadata{NetWork: C.UDP, Host: "verify.example", SmartTarget: "verify.example", DstIPASN: "unknown"}
			switch state {
			case "blocked":
				s.store.UpdateBlockedNodesCache(s.Name(), s.configName, map[string]*smart.NodeState{pa.Name(): {BlockedUntil: time.Now().Add(time.Hour).Unix()}})
			case "dead":
				s.GroupBase = NewGroupBase(GroupBaseOption{Name: s.Name(), Type: C.SmartPLD, Providers: []P.ProxyProvider{&cacheMutableProvider{proxies: []C.Proxy{&unwrapBarrierProxy{Proxy: pa, name: pa.Name(), udp: false}}, version: 1}}})
			case "cooldown":
				s.failureCooldown = time.Minute
				s.getPLDRecord(md.SmartTarget, pa.Name()).FillFailure(60)
			case "disabled":
				s.disableUDP = true
			}
			assertUDPContractUnavailable(t, s, md, true)
			if a.calls.Load() != 0 {
				t.Fatal("filtered TCP-only node dialed")
			}
		})
	}
}

// Exercise both real resolveMetadata unwrap loops: CheckPassRule and match's
// nested group walk. The final DNS dialer calls the matched group's packet API.
type udpContractRule struct {
	C.Rule
	checked  bool
	metadata *C.Metadata
}

func (r *udpContractRule) Match(md *C.Metadata, helper C.RuleMatchHelper) (bool, string) {
	r.checked = true
	r.metadata = md
	if helper.CheckPassRule(r.Adapter()) {
		return false, ""
	}
	return r.Rule.Match(md, helper)
}

func TestPLDUDPContractTunnelRuleChainNoDirectFallback(t *testing.T) {
	a, pa := newUDPContractProxy("tcp-only", false)
	s, _ := liveCacheFixture(t, liveCacheStore(t), "udp-cfg", "udp-rule-group", []C.Proxy{pa})
	group := adapter.NewProxy(s)
	selector, err := NewSelector(GroupCommonOption{Name: "outer"}, SelectorOption{}, group, []P.ProxyProvider{&cacheMutableProvider{proxies: []C.Proxy{group}, version: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rawRule, err := RC.NewIPCIDR("127.0.0.0/8", selector.Name(), RC.WithIPCIDRNoResolve(true))
	if err != nil {
		t.Fatal(err)
	}
	rule := &udpContractRule{Rule: rawRule}
	direct, directProxy := newUDPContractProxy("DIRECT", true)
	oldRules, oldProxies, oldProviders, oldRP, oldMode := tunnel.Rules(), tunnel.Proxies(), tunnel.Providers(), tunnel.RuleProviders(), tunnel.Mode()
	t.Cleanup(func() {
		tunnel.UpdateRules(oldRules, nil, oldRP)
		tunnel.UpdateProxies(oldProxies, oldProviders)
		tunnel.SetMode(oldMode)
	})
	tunnel.UpdateProxies(map[string]C.Proxy{selector.Name(): adapter.NewProxy(selector), s.Name(): group, "DIRECT": directProxy}, nil)
	tunnel.UpdateRules([]C.Rule{rule, RC.NewMatch("DIRECT")}, nil, nil)
	tunnel.SetMode(tunnel.Rule)
	d := tunnel.NewDNSDialer(nil, nil, tunnel.DnsRespectRules)
	pc, err := d.ListenPacket(context.Background(), "udp", "127.0.0.1:53")
	if pc != nil || !errors.Is(err, C.ErrNotSupport) || !errors.Is(err, ErrSmartPLDNoCandidates) {
		t.Fatalf("tunnel UDP conn=%v error=%v", pc, err)
	}
	if !rule.checked || rule.metadata.SmartTarget != rawRule.RuleType().String()+" ["+rawRule.Payload()+"]" || rule.metadata.SmartBlock != "normal" {
		t.Fatalf("rule chain not preserved: checked=%v metadata=%+v", rule.checked, rule.metadata)
	}
	if a.calls.Load() != 0 || direct.calls.Load() != 0 {
		t.Fatal("tunnel dialed TCP-only or leaked to DIRECT")
	}
	pd := proxydialer.New(group, true)
	if pc, err := pd.ListenPacket(context.Background(), "udp", "", netip.MustParseAddrPort("127.0.0.1:53")); pc != nil || !errors.Is(err, ErrSmartPLDNoCandidates) {
		t.Fatalf("proxy dialer conn=%v error=%v", pc, err)
	}
}

func TestPLDUDPContractRejectNilConnection(t *testing.T) {
	p, proxy := newUDPContractProxy("broken-udp", true)
	p.nilConn = true
	s, _ := liveCacheFixture(t, liveCacheStore(t), "cfg", "nil-conn", []C.Proxy{proxy})
	md := &C.Metadata{NetWork: C.UDP, Host: "verify.example", DstIPASN: "unknown"}
	if pc, err := s.ListenPacketContext(context.Background(), md); pc != nil || err == nil || !strings.Contains(err.Error(), "nil UDP connection") {
		t.Fatalf("broken adapter conn=%v error=%v", pc, err)
	}
}
