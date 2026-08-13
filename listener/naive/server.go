package naive

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/common/sockopt"
	"github.com/metacubex/mihomo/component/auth"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/ech"
	C "github.com/metacubex/mihomo/constant"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/listener/inner"
	"github.com/metacubex/mihomo/listener/reality"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/ntp"
	"github.com/metacubex/mihomo/transport/socks5"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httputil"
	"github.com/metacubex/quic-go"
	"github.com/metacubex/quic-go/http3"
	"github.com/metacubex/tls"
)

type Listener struct {
	closed       bool
	addr         string
	tcpListener  net.Listener
	httpServer   *http.Server
	udpListeners []net.PacketConn
	h3Servers    []*http3.Server
}

// RawAddress implements C.Listener
func (l *Listener) RawAddress() string { return l.addr }

// Address implements C.Listener
func (l *Listener) Address() string {
	if l.tcpListener != nil {
		return l.tcpListener.Addr().String()
	}
	return l.addr
}

// Close implements C.Listener
func (l *Listener) Close() error {
	l.closed = true
	var errs []error
	// Close the UDP sockets before the HTTP/3 servers: the sockets were passed
	// to http3.Server.Serve (so the server does not own them), and
	// http3.Server.Close blocks until every active QUIC connection has ended —
	// closing the socket first makes those connections error out promptly.
	for _, ul := range l.udpListeners {
		if err := ul.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, h3 := range l.h3Servers {
		if err := h3.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if l.tcpListener != nil {
		if err := l.tcpListener.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// New creates a NaïveProxy inbound. It terminates TLS, speaks HTTP/2 (and,
// optionally, HTTP/3 over QUIC) and tunnels CONNECT requests (with naive
// padding) into mihomo's routing engine. Any request that is not a valid,
// authenticated naive CONNECT is served by the masquerade handler instead
// (probe resistance), so the endpoint looks like an ordinary web server to
// anyone without valid credentials.
func New(config LC.AuthServer, masquerade string, enableH3 bool, lc C.InboundListenConfig, tunnel C.Tunnel, additions ...inbound.Addition) (*Listener, error) {
	if len(additions) == 0 {
		additions = []inbound.Addition{
			inbound.WithInName("DEFAULT-NAIVE"),
			inbound.WithSpecialRules(""),
		}
	}

	masqueradeHandler, err := buildMasqueradeHandler(masquerade, tunnel)
	if err != nil {
		return nil, err
	}

	baseTLSConfig := &tls.Config{Time: ntp.Now}
	var realityBuilder *reality.Builder
	hasCertificate := false

	if config.Certificate != "" && config.PrivateKey != "" {
		certLoader, err := ca.NewTLSKeyPairLoader(config.Certificate, config.PrivateKey)
		if err != nil {
			return nil, err
		}
		baseTLSConfig.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return certLoader()
		}
		hasCertificate = true
		if config.EchKey != "" {
			if err = ech.LoadECHKey(config.EchKey, baseTLSConfig); err != nil {
				return nil, err
			}
		}
	}
	baseTLSConfig.ClientAuth = ca.ClientAuthTypeFromString(config.ClientAuthType)
	if len(config.ClientAuthCert) > 0 {
		if baseTLSConfig.ClientAuth == tls.NoClientCert {
			baseTLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	if baseTLSConfig.ClientAuth == tls.VerifyClientCertIfGiven || baseTLSConfig.ClientAuth == tls.RequireAndVerifyClientCert {
		pool, err := ca.LoadCertificates(config.ClientAuthCert)
		if err != nil {
			return nil, err
		}
		baseTLSConfig.ClientCAs = pool
	}
	if config.RealityConfig.PrivateKey != "" {
		if hasCertificate {
			return nil, errors.New("certificate is unavailable in reality")
		}
		if baseTLSConfig.ClientAuth != tls.NoClientCert {
			return nil, errors.New("client-auth is unavailable in reality")
		}
		realityBuilder, err = config.RealityConfig.Build(tunnel)
		if err != nil {
			return nil, err
		}
	}

	if realityBuilder == nil && !hasCertificate {
		return nil, errors.New("naive server requires a TLS certificate/private-key pair or reality-config")
	}
	if enableH3 && !hasCertificate {
		return nil, errors.New("naive quic (h3) requires a TLS certificate/private-key pair")
	}

	handler := &handler{
		store:      config.AuthStore,
		masquerade: masqueradeHandler,
		tunnel:     tunnel,
		additions:  additions,
	}

	sl := &Listener{addr: config.Listen}

	// TCP: HTTP/2 (naive) + HTTP/1.1 (masquerade). The metacubex/http server
	// auto-negotiates h2 over the TLS listener (its own TLSConfig is nil), so a
	// single server handles both ALPN protocols and dispatches to handler.
	tcpListener, err := lc.Listen(context.Background(), "tcp", config.Listen)
	if err != nil {
		return nil, err
	}
	if realityBuilder != nil {
		tcpListener = realityBuilder.NewListener(tcpListener)
	} else {
		h2TLSConfig := baseTLSConfig.Clone()
		h2TLSConfig.NextProtos = []string{"h2", "http/1.1"}
		tcpListener = tls.NewListener(tcpListener, h2TLSConfig)
	}
	sl.tcpListener = tcpListener
	// The local (server) address is the same for every request on this listener;
	// capture it once for tunnel metadata before serving begins.
	handler.localAddr = tcpListener.Addr()
	sl.httpServer = &http.Server{Handler: handler}
	go func() {
		if err := sl.httpServer.Serve(tcpListener); err != nil && !sl.closed {
			log.Warnln("[Naive] http server on %s exited: %v", config.Listen, err)
		}
	}()

	// UDP: HTTP/3 (naive) over QUIC, when enabled and a real certificate exists.
	if enableH3 {
		h3TLSConfig := baseTLSConfig.Clone()
		h3TLSConfig.NextProtos = []string{"h3"}
		for _, addr := range strings.Split(sl.RawAddress(), ",") {
			udpListener, err := lc.ListenPacket(context.Background(), "udp", addr)
			if err != nil {
				_ = sl.Close()
				return nil, err
			}
			if err := sockopt.UDPReuseaddr(udpListener); err != nil {
				log.Warnln("[Naive] failed to set UDP_REUSEADDR on %s: %v", addr, err)
			}
			h3Server := &http3.Server{
				Handler:   handler,
				TLSConfig: h3TLSConfig,
				QUICConfig: &quic.Config{
					MaxIdleTimeout: 30 * time.Second,
				},
			}
			sl.udpListeners = append(sl.udpListeners, udpListener)
			sl.h3Servers = append(sl.h3Servers, h3Server)
			go func(server *http3.Server, conn net.PacketConn) {
				if err := server.Serve(conn); err != nil && !sl.closed {
					log.Warnln("[Naive] h3 server on %s exited: %v", conn.LocalAddr(), err)
				}
			}(h3Server, udpListener)
		}
	}

	return sl, nil
}

type handler struct {
	store      auth.AuthStore
	masquerade http.Handler
	tunnel     C.Tunnel
	additions  []inbound.Addition
	localAddr  net.Addr
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, ok := h.validateNaive(r)
	if !ok {
		// Probe resistance: anything that isn't a valid, authenticated naive
		// CONNECT is served like a normal web request — never a 407/400 that
		// would reveal a proxy.
		h.serveFallback(w, r)
		return
	}

	host := r.URL.Host
	if host == "" {
		host = r.Host
	}
	target := socks5.ParseAddr(host)
	if target == nil {
		h.serveFallback(w, r)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Padding", generatePaddingHeader())
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	conn := &naiveH2Conn{
		reader:     r.Body,
		writer:     w,
		flusher:    flusher,
		localAddr:  h.localAddr,
		remoteAddr: remoteAddrOf(r),
	}

	additions := h.additions
	if user != "" {
		additions = append(append([]inbound.Addition{}, h.additions...), inbound.WithInUser(user))
	}

	// HandleTCPConn blocks until the tunneled connection finishes, which keeps
	// the HTTP/2 (or HTTP/3) stream (r.Body / w) alive for the whole session.
	h.tunnel.HandleTCPConn(inbound.NewSocket(target, conn, C.NAIVE, additions...))
}

// validateNaive reports whether r is a valid naive CONNECT request and, if
// authentication is enabled, carries valid credentials. The returned user is
// the authenticated username (empty when auth is disabled).
func (h *handler) validateNaive(r *http.Request) (user string, ok bool) {
	if r.Method != http.MethodConnect {
		return "", false
	}
	// Genuine naive clients always send a random "Padding" request header.
	if r.Header.Get("Padding") == "" {
		return "", false
	}
	if h.store == nil {
		return "", true
	}
	authenticator := h.store.Authenticator()
	if authenticator == nil {
		return "", true
	}
	user, pass, parsed := parseBasicAuth(r.Header.Get("Proxy-Authorization"))
	if !parsed || !authenticator.Verify(user, pass) {
		log.Infoln("[Naive] auth failed from %s", r.RemoteAddr)
		return "", false
	}
	return user, true
}

// serveFallback answers non-naive requests. With a masquerade handler it acts
// like the configured decoy site; otherwise it returns a bare 404. CONNECT
// requests (which a normal web server does not proxy) always get a 404.
func (h *handler) serveFallback(w http.ResponseWriter, r *http.Request) {
	if h.masquerade != nil && r.Method != http.MethodConnect {
		h.masquerade.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

func buildMasqueradeHandler(masquerade string, tunnel C.Tunnel) (http.Handler, error) {
	if masquerade == "" {
		return nil, nil
	}
	masqueradeURL, err := url.Parse(masquerade)
	if err != nil {
		return nil, fmt.Errorf("parse masquerade URL: %w", err)
	}
	switch masqueradeURL.Scheme {
	case "file":
		return http.FileServer(http.Dir(masqueradeURL.Path)), nil
	case "http", "https":
		return &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(masqueradeURL)
				r.Out.Host = r.In.Host
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				w.WriteHeader(http.StatusBadGateway)
			},
			Transport: &http.Transport{
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				// Route the masquerade upstream through mihomo itself so the
				// decoy fetch obeys the same rules as any other connection.
				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					return inner.HandleTcp(tunnel, address, "")
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown masquerade URL scheme: %s", masqueradeURL.Scheme)
	}
}

// remoteAddrOf derives the client address for tunnel metadata. r.RemoteAddr is
// set by both the h2 and h3 servers.
func remoteAddrOf(r *http.Request) net.Addr {
	if ap, err := netip.ParseAddrPort(r.RemoteAddr); err == nil {
		return net.TCPAddrFromAddrPort(ap)
	}
	return &net.TCPAddr{}
}

func parseBasicAuth(header string) (username, password string, ok bool) {
	const prefix = "Basic "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return
	}
	decoded, err := base64.StdEncoding.DecodeString(header[len(prefix):])
	if err != nil {
		return
	}
	credentials := string(decoded)
	idx := strings.IndexByte(credentials, ':')
	if idx < 0 {
		return
	}
	return credentials[:idx], credentials[idx+1:], true
}
