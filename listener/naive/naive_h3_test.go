package naive

import (
	"context"
	"encoding/base64"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/component/auth"
	authStore "github.com/metacubex/mihomo/listener/auth"
	LC "github.com/metacubex/mihomo/listener/config"

	"github.com/metacubex/http"
	"github.com/metacubex/quic-go"
	"github.com/metacubex/quic-go/http3"
	"github.com/metacubex/tls"
)

func startNaiveH3(t *testing.T) *Listener {
	t.Helper()
	certPEM, keyPEM := selfSignedPEM(t)
	store := authStore.NewAuthStore(auth.NewAuthenticator([]auth.AuthUser{{User: "u", Pass: "p"}}))
	listener, err := New(
		LC.AuthServer{
			Listen:      "127.0.0.1:0",
			AuthStore:   store,
			Certificate: certPEM,
			PrivateKey:  keyPEM,
		},
		"",   // no masquerade
		true, // enable h3
		inbound.NewListenConfig(),
		relayTunnel{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// TestNaiveTunnelH3 verifies a valid naive CONNECT tunnels end to end over
// HTTP/3 (QUIC), including the naive padding on the first frames.
func TestNaiveTunnelH3(t *testing.T) {
	echoAddr := startEcho(t)
	listener := startNaiveH3(t)
	if len(listener.udpListeners) == 0 {
		t.Fatal("naive server did not open a UDP (h3) listener")
	}
	h3Addr := listener.udpListeners[0].LocalAddr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	quicConn, err := quic.DialAddr(ctx, h3Addr, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
		ServerName:         "example.com",
	}, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer quicConn.CloseWithError(0, "")

	cc := (&http3.Transport{}).NewClientConn(quicConn)

	pr, pw := io.Pipe()
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: echoAddr},
		Host:   echoAddr,
		Header: make(http.Header),
		Body:   io.NopCloser(pr),
	}
	req.Header.Set("Padding", "paddingpaddingpaddingpadding~~~~")
	req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("u:p")))

	resp, err := cc.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("Padding") == "" {
		t.Fatal("server did not send a Padding response header")
	}

	assertEcho(t, pw, resp.Body, []byte("hello-naive-quic-h3-0123456789"))
}
