package inbound

import (
	"context"
	"errors"
	"net"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/transport/socks5"
	"github.com/metacubex/mihomo/transport/xhttp"
	"github.com/metacubex/tls"
)

type XhttpOption struct {
	BaseOption
	Host                 string            `inbound:"host,omitempty"`
	Path                 string            `inbound:"path,omitempty"`
	Mode                 string            `inbound:"mode,omitempty"`
	HTTPVersion          string            `inbound:"http-version,omitempty"`
	H3WeakNetwork        bool              `inbound:"h3-weak-network,omitempty"`
	Headers              map[string]string `inbound:"headers,omitempty"`
	NoGRPCHeader         bool              `inbound:"no-grpc-header,omitempty"`
	NoSSEHeader          bool              `inbound:"no-sse-header,omitempty"`
	XPaddingBytes        string            `inbound:"x-padding-bytes,omitempty"`
	ScStreamUpServerSecs string            `inbound:"sc-stream-up-server-secs,omitempty"`
	Certificate          string            `inbound:"certificate,omitempty"`
	PrivateKey           string            `inbound:"private-key,omitempty"`
}

func (o XhttpOption) Equal(config C.InboundConfig) bool {
	return optionToString(o) == optionToString(config)
}

type Xhttp struct {
	*Base
	config     *XhttpOption
	listener   net.Listener
	packetConn net.PacketConn
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewXhttp(options *XhttpOption) (*Xhttp, error) {
	base, err := NewBase(&options.BaseOption)
	if err != nil {
		return nil, err
	}
	return &Xhttp{
		Base:   base,
		config: options,
	}, nil
}

func (x *Xhttp) Config() C.InboundConfig {
	return x.config
}

func (x *Xhttp) Address() string {
	if x.listener != nil {
		return x.listener.Addr().String()
	}
	if x.packetConn != nil {
		return x.packetConn.LocalAddr().String()
	}
	return x.RawAddress()
}

// loadTLSConfig loads the X509 key pair. For HTTP/3 the listener must serve a
// *tls.Config with the "h3" ALPN (QUIC mandates TLS 1.3); for HTTP/2 we prefer
// the h2 ALPN, otherwise plain http/1.1.
func loadTLSConfig(certFile, keyFile, httpVersion string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	switch httpVersion {
	case "3", "h3":
		cfg.MinVersion = tls.VersionTLS13
		cfg.NextProtos = []string{"h3"}
	default:
		cfg.MinVersion = tls.VersionTLS12
		if httpVersion == "2" || httpVersion == "h2" {
			cfg.NextProtos = []string{"h2", "http/1.1"}
		} else {
			cfg.NextProtos = []string{"http/1.1"}
		}
	}
	return cfg, nil
}

func (x *Xhttp) Listen(tunnel C.Tunnel) error {
	var err error
	cfg := &xhttp.Config{
		Host:                 x.config.Host,
		Path:                 x.config.Path,
		Mode:                 x.config.Mode,
		Headers:              x.config.Headers,
		NoGRPCHeader:         x.config.NoGRPCHeader,
		NoSSEHeader:          x.config.NoSSEHeader,
		XPaddingBytes:        x.config.XPaddingBytes,
		ScStreamUpServerSecs: x.config.ScStreamUpServerSecs,
		HTTPVersion:          x.config.HTTPVersion,
		H3WeakNetwork:        x.config.H3WeakNetwork,
	}

	addr := x.RawAddress()

	var tlsCfg *tls.Config
	if x.config.Certificate != "" && x.config.PrivateKey != "" {
		tlsCfg, err = loadTLSConfig(x.config.Certificate, x.config.PrivateKey, cfg.HTTPVersion)
		if err != nil {
			return err
		}
		log.Infoln("XHTTP[%s] TLS loaded (http-version=%s)", x.Name(), cfg.HTTPVersion)
	}

	httpVersion := cfg.HTTPVersionToUse(tlsCfg != nil)

	var listener net.Listener
	var packetConn net.PacketConn
	if httpVersion == "3" {
		// HTTP/3 runs over QUIC, which requires a UDP datagram socket.
		packetConn, err = net.ListenPacket("udp", addr)
		if err != nil {
			return err
		}
		x.packetConn = packetConn
	} else {
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		x.listener = listener
	}

	x.ctx, x.cancel = context.WithCancel(context.Background())

	additions := x.Additions()
	connHandler := func(conn net.Conn) {
		tunnel.HandleTCPConn(inbound.NewSocket(socks5.ParseAddr("0.0.0.0:0"), conn, C.HTTPS, additions...))
	}

	switch httpVersion {
	case "3":
		h3srv, err := xhttp.NewHTTP3Server(*cfg, connHandler, tlsCfg)
		if err != nil {
			packetConn.Close()
			return err
		}
		go func() {
			if err := h3srv.Serve(packetConn); err != nil && err != http.ErrServerClosed {
				log.Errorln("XHTTP[%s] HTTP/3 server error: %v", x.Name(), err)
			}
		}()
		go func() {
			<-x.ctx.Done()
			_ = h3srv.Close()
		}()
	default:
		handler, err := xhttp.NewServerHandler(xhttp.ServerOption{
			Config:      *cfg,
			ConnHandler: connHandler,
		})
		if err != nil {
			listener.Close()
			return err
		}
		if tlsCfg != nil {
			srv := &http.Server{Handler: handler, TLSConfig: tlsCfg}
			go func() {
				if err := srv.ServeTLS(listener, "", ""); err != nil && err != http.ErrServerClosed {
					log.Errorln("XHTTP[%s] TLS server error: %v", x.Name(), err)
				}
			}()
		} else {
			srv := &http.Server{Handler: handler}
			go func() {
				if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
					log.Errorln("XHTTP[%s] server error: %v", x.Name(), err)
				}
			}()
		}
		go func() {
			<-x.ctx.Done()
			_ = listener.Close()
		}()
	}

	log.Infoln("XHTTP[%s] listening at: %s (mode: %s, http: %s)", x.Name(), x.Address(), cfg.Mode, httpVersion)
	return nil
}

func (x *Xhttp) Close() error {
	if x.cancel != nil {
		x.cancel()
	}
	if x.packetConn != nil {
		err := x.packetConn.Close()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
	if x.listener != nil {
		err := x.listener.Close()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
	return nil
}

var _ C.InboundListener = (*Xhttp)(nil)
