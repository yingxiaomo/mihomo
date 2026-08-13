//go:build with_naive_cronet

package outbound

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/sagernet/cronet-go"
	// Links the prebuilt libcronet for the target platform: the glibc/musl static
	// archive under CGO, or the purego stub when the library is dlopen'd at runtime.
	_ "github.com/sagernet/cronet-go/all"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	mDNS "github.com/miekg/dns"
)

// NaiveOption defines the configuration for a NaïveProxy outbound.
//
// NaïveProxy uses Chromium's network stack to mimic browser TLS/HTTP2 fingerprints,
// embedded via the sagernet/cronet-go library. No external naiveproxy binary needed.
type NaiveOption struct {
	BasicOption
	Name     string `proxy:"name"`
	Server   string `proxy:"server"`
	Port     int    `proxy:"port"`
	UserName string `proxy:"username,omitempty"`
	Password string `proxy:"password,omitempty"`

	SNI            string `proxy:"sni,omitempty"`
	SkipCertVerify bool   `proxy:"skip-cert-verify,omitempty"`

	// QUIC transport (NaïveProxy over QUIC, anti-QoS on UDP-throttled lines).
	// Requires cronet's built-in UDP support; incompatible with
	// insecure-concurrency > 1 (cronet rejects that combination).
	QUIC                  bool              `proxy:"quic,omitempty"`
	QUICCongestionControl string            `proxy:"quic-congestion-control,omitempty"` // bbr / bbr2 / cubic / reno
	InsecureConcurrency   int               `proxy:"insecure-concurrency,omitempty"`
	ExtraHeaders          map[string]string `proxy:"extra-headers,omitempty"`
	Certificate           []string          `proxy:"certificate,omitempty"`      // PEM lines: trusted root for the naive server
	CertificatePath       string            `proxy:"certificate-path,omitempty"` // file containing the trusted root PEM
}

// cronetDialer adapts mihomo's C.Dialer to sing's N.Dialer interface
// so that the cronet engine can use mihomo's dialer chain (interface,
// routing mark, TFO, etc.) for its underlying TCP transport.
type cronetDialer struct {
	inner C.Dialer
}

func (d *cronetDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.inner.DialContext(ctx, network, destination.TCPAddr().String())
}

func (d *cronetDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return d.inner.ListenPacket(ctx, "udp", destination.UDPAddr().String(), destination.AddrPort())
}

var _ N.Dialer = (*cronetDialer)(nil)

// parseQuicCongestionControl maps the config string to cronet's QUIC
// congestion-control enum. Empty means the library default; supported values:
// bbr, bbr2, cubic, reno.
func parseQuicCongestionControl(s string) (cronet.QUICCongestionControl, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return cronet.QUICCongestionControlDefault, nil
	case "bbr":
		return cronet.QUICCongestionControlBBR, nil
	case "bbr2":
		return cronet.QUICCongestionControlBBRv2, nil
	case "cubic":
		return cronet.QUICCongestionControlCubic, nil
	case "reno":
		return cronet.QUICCongestionControlReno, nil
	default:
		return cronet.QUICCongestionControlDefault, fmt.Errorf("unknown quic-congestion-control %q (supported: bbr/bbr2/cubic/reno)", s)
	}
}

type Naive struct {
	*Base
	option *NaiveOption
	client *cronet.NaiveClient
}

func NewNaive(option NaiveOption) (*Naive, error) {
	if option.QUIC && option.InsecureConcurrency > 1 {
		return nil, fmt.Errorf("naive %s: insecure-concurrency is not supported with quic", option.Name)
	}
	quicCC, err := parseQuicCongestionControl(option.QUICCongestionControl)
	if err != nil {
		return nil, fmt.Errorf("naive %s: %w", option.Name, err)
	}

	addr := net.JoinHostPort(option.Server, strconv.Itoa(option.Port))
	serverAddr := M.ParseSocksaddr(addr)

	serverName := option.SNI
	if serverName == "" {
		serverName = option.Server
	}

	base := NewBase(BaseOption{
		Name:         option.Name,
		Addr:         addr,
		Type:         C.Naive,
		ProviderName: option.ProviderName,
		UDP:          false,
		TFO:          option.TFO,
		MPTCP:        option.MPTCP,
		Interface:    option.Interface,
		RoutingMark:  option.RoutingMark,
		Prefer:       option.IPVersion,
	})

	// Build a system dialer that respects mihomo's base options (interface,
	// routing mark, TFO, IP version preference) but does NOT apply dialer-proxy
	// — the cronet engine's transport must connect directly to the Naive server.
	mihomoDialer := dialer.NewDialer(base.DialOptions()...)

	// Custom trusted root for the naive server's TLS certificate (mihomo uses
	// TrustedRootCertificates since cronet has no skip-cert-verify option).
	var trustedRootCertificates string
	if len(option.Certificate) > 0 {
		trustedRootCertificates = strings.Join(option.Certificate, "\n")
	} else if option.CertificatePath != "" {
		content, err := os.ReadFile(option.CertificatePath)
		if err != nil {
			return nil, fmt.Errorf("naive %s: read certificate: %w", option.Name, err)
		}
		trustedRootCertificates = string(content)
	}

	client, err := cronet.NewNaiveClient(cronet.NaiveClientOptions{
		ServerAddress: serverAddr,
		ServerName:    serverName,
		Username:      option.UserName,
		Password:      option.Password,
		Dialer:        &cronetDialer{inner: mihomoDialer},
		// cronet runs its own in-process DNS server to resolve the Naive server
		// name; this field is REQUIRED (NewNaiveClient errors out otherwise).
		// Answer through mihomo's resolver so hosts/fake-ip/DNS config apply.
		DNSResolver:             naiveDNSResolver,
		InsecureConcurrency:     option.InsecureConcurrency,
		ExtraHeaders:            option.ExtraHeaders,
		TrustedRootCertificates: trustedRootCertificates,
		QUIC:                    option.QUIC,
		QUICCongestionControl:   quicCC,
	})
	if err != nil {
		return nil, fmt.Errorf("create naive client: %w", err)
	}

	if err := client.Start(); err != nil {
		return nil, fmt.Errorf("start naive client: %w", err)
	}

	n := &Naive{
		Base:   base,
		option: &option,
		client: client,
	}

	log.Infoln("[Naive] %s: cronet ready for %s", option.Name, addr)
	return n, nil
}

// DialContext creates a NaïveProxy CONNECT tunnel to the target via cronet's
// Chromium network stack. The underlying transport to the Naive server uses
// mihomo's dialer chain.
func (n *Naive) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	targetAddr := M.ParseSocksaddr(metadata.RemoteAddress())
	conn, err := n.client.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		return nil, fmt.Errorf("naive dial: %w", err)
	}
	return NewConn(conn, n), nil
}

// ListenPacketContext is not supported — NaïveProxy only supports TCP streams.
func (n *Naive) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	return nil, C.ErrNotSupport
}

// SupportUOT returns false — UDP over TCP is not currently supported.
func (n *Naive) SupportUOT() bool { return false }

// Close shuts down the cronet engine and releases all connections.
func (n *Naive) Close() error {
	if n.client != nil {
		return n.client.Close()
	}
	return nil
}

// naiveDNSResolver answers cronet's internal DNS queries (used to resolve the
// Naive server name and ECH records) through mihomo's own resolver.
//
// The globals are read at call time, not at construction, because they are
// populated after the DNS module starts. Resolvers are tried in priority order:
// ProxyServerHostResolver (avoids routing loops when resolving a proxy server
// host), then DefaultResolver, then SystemResolver as a last resort — so the
// outbound still works on a minimal config without a user-configured DNS. A
// valid negative answer (e.g. NXDOMAIN, err==nil) is returned as-is; only a
// transport error or nil resolver falls through to the next.
func naiveDNSResolver(ctx context.Context, request *mDNS.Msg) *mDNS.Msg {
	for _, r := range []resolver.Resolver{
		resolver.ProxyServerHostResolver,
		resolver.DefaultResolver,
		resolver.SystemResolver,
	} {
		if r == nil {
			continue
		}
		if response, err := r.ExchangeContext(ctx, request); err == nil && response != nil {
			return response
		}
	}
	response := &mDNS.Msg{}
	response.SetRcode(request, mDNS.RcodeServerFailure)
	return response
}
