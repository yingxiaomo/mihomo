package outboundgroup

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

func TestPLDUDPContractTCPRouteUnchanged(t *testing.T) {
	server, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	direct := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "tcp-A"}))
	p := &unwrapBarrierProxy{Proxy: direct, name: direct.Name(), alive: true, udp: false}
	s, _ := liveCacheFixture(t, liveCacheStore(t), "udp-cfg", "tcp-route", []C.Proxy{p})
	s.selected = p.Name()
	s.sampleRate = 1
	md := &C.Metadata{NetWork: C.TCP, SmartTarget: "verify.example", DstIPASN: "unknown", DstIP: netip.MustParseAddr("127.0.0.1"), DstPort: uint16(server.Addr().(*net.TCPAddr).Port)}
	conn, err := adapter.NewProxy(s).DialContext(context.Background(), md)
	if err != nil {
		t.Fatal(err)
	}
	conn = statistic.NewTCPTracker(conn, statistic.DefaultManager, md, nil, 0, 0, true)
	defer conn.Close()
	peer, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	deadline := time.Now().Add(3 * time.Second)
	_ = conn.SetDeadline(deadline)
	_ = peer.SetDeadline(deadline)
	if _, err = peer.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	n, err := conn.Read(buffer)
	if err != nil || string(buffer[:n]) != "reply" {
		t.Fatalf("TCP reply=%q error=%v", buffer[:n], err)
	}
	if conn.Chains().Last() != p.Name() || conn.Chains()[len(conn.Chains())-1] != s.Name() {
		t.Fatalf("TCP chains=%v", conn.Chains())
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitTransportStats(t, s, md, p.Name())
}

func TestPLDUDPContractEmptyConfiguredFallback(t *testing.T) {
	for _, udp := range []bool{false, true} {
		t.Run(map[bool]string{false: "TCP-only", true: "UDP-supported"}[udp], func(t *testing.T) {
			p, proxy := newUDPContractProxy("configured-fallback", udp)
			s, _ := liveCacheFixture(t, liveCacheStore(t), "udp-cfg", "empty-fallback", nil)
			s.emptyFallback = proxy
			md := &C.Metadata{NetWork: C.UDP, Host: "verify.example", SmartTarget: "verify.example", DstIPASN: "unknown"}
			if !udp {
				assertUDPContractUnavailable(t, s, md, true)
				if p.calls.Load() != 0 {
					t.Fatal("empty provider attempted TCP-only fallback")
				}
				return
			}
			if s.Unwrap(md, false) != proxy {
				t.Fatal("configured eligible empty fallback changed")
			}
			udpContractRoundTrip(t, s, md, proxy.Name())
		})
	}
}

// The existing selector intentionally permits an unhealthy but UDP-capable
// last resort. Keep that policy distinct from a UDP-incapable raw fallback.
func TestPLDUDPContractFilteredCapableFallbackDoesNotPanic(t *testing.T) {
	for _, state := range []string{"blocked", "dead", "cooldown"} {
		t.Run(state, func(t *testing.T) {
			base := adapter.NewProxy(outbound.NewBase(outbound.BaseOption{Name: "udp-last-resort", UDP: true, Type: C.Socks5}))
			p := &unwrapBarrierProxy{Proxy: base, name: base.Name(), alive: state != "dead", udp: true}
			s, _ := liveCacheFixture(t, liveCacheStore(t), "udp-cfg", "udp-last-resort", []C.Proxy{p})
			md := &C.Metadata{NetWork: C.UDP, Host: "verify.example", SmartTarget: "verify.example", DstIPASN: "unknown"}
			switch state {
			case "blocked":
				s.store.UpdateBlockedNodesCache(s.Name(), s.configName, map[string]*smart.NodeState{p.Name(): {BlockedUntil: time.Now().Add(time.Hour).Unix()}})
			case "cooldown":
				s.failureCooldown = time.Minute
				s.getPLDRecord(md.SmartTarget, p.Name()).FillFailure(60)
			}
			if s.Unwrap(md, false) != p || s.IsL3Protocol(md) {
				t.Fatal("eligible legacy last resort changed")
			}
			// Base's packet API provides an explicit unsupported error, never a
			// successful nil connection; call through the real adapter wrapper.
			if pc, err := adapter.NewProxy(s.Unwrap(md, false)).ListenPacketContext(context.Background(), md); pc != nil || !errors.Is(err, C.ErrNotSupport) {
				t.Fatalf("last resort conn=%v error=%v", pc, err)
			}
		})
	}
}
