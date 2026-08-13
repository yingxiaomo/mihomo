package naive

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/component/auth"
	C "github.com/metacubex/mihomo/constant"
	authStore "github.com/metacubex/mihomo/listener/auth"
	LC "github.com/metacubex/mihomo/listener/config"

	"golang.org/x/net/http2"
)

// relayTunnel is a minimal C.Tunnel that dials each CONNECT target directly and
// relays bytes, so the naive inbound can be exercised end to end.
type relayTunnel struct{}

func (relayTunnel) HandleTCPConn(conn net.Conn, metadata *C.Metadata) {
	remote, err := net.DialTimeout("tcp", metadata.RemoteAddress(), 5*time.Second)
	if err != nil {
		_ = conn.Close()
		return
	}
	// Full-duplex relay: as soon as either direction ends, close both sides so
	// the other io.Copy unblocks and HandleTCPConn returns (mirrors mihomo's
	// real tunnel; without this an h3 connection would never be released).
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go cp(remote, conn)
	go cp(conn, remote)
	<-done
	_ = conn.Close()
	_ = remote.Close()
	<-done
}

func (relayTunnel) HandleUDPPacket(packet C.UDPPacket, metadata *C.Metadata) {}
func (relayTunnel) NatTable() C.NatTable                                     { return nil }

func selfSignedPEM(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"example.com"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return
}

// startEcho starts a TCP echo server and returns its address.
func startEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// startNaive starts a naive inbound with a single user u:p and returns its addr.
func startNaive(t *testing.T) string {
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
		"",    // no masquerade
		false, // h2 only
		inbound.NewListenConfig(),
		relayTunnel{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Address()
}

// dialH2 opens an HTTP/2 client connection to a naive server.
func dialH2(t *testing.T, addr string) *http2.ClientConn {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tlsConn := tls.Client(raw, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
		ServerName:         "example.com",
	})
	_ = tlsConn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := tlsConn.HandshakeContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := tlsConn.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Fatalf("ALPN = %q, want h2", got)
	}
	_ = tlsConn.SetDeadline(time.Time{})
	cc, err := (&http2.Transport{}).NewClientConn(tlsConn)
	if err != nil {
		t.Fatal(err)
	}
	return cc
}

func connectRequest(target, user, pass string, withPadding bool, body io.Reader) *http.Request {
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: target},
		Host:   target,
		Header: make(http.Header),
	}
	if body != nil {
		req.Body = io.NopCloser(body)
	}
	if withPadding {
		req.Header.Set("Padding", "paddingpaddingpaddingpadding~~~~")
	}
	if user != "" {
		req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}
	return req
}

// TestNaiveTunnel verifies a valid, authenticated naive CONNECT tunnels data
// end to end through the naive padding on the first frames.
func TestNaiveTunnel(t *testing.T) {
	echoAddr := startEcho(t)
	naiveAddr := startNaive(t)

	cc := dialH2(t, naiveAddr)
	pr, pw := io.Pipe()
	resp, err := cc.RoundTrip(connectRequest(echoAddr, "u", "p", true, pr))
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

	assertEcho(t, pw, resp.Body, []byte("hello-naive-padding-0123456789"))
}

// assertEcho writes msg (with naive padding) into the tunnel, reads it back from
// the echo target, verifies it, and only then closes the write side. Reading
// before closing matters: closing the request body tears the tunnel down, so a
// premature close would race the echo reply.
func assertEcho(t *testing.T, pw *io.PipeWriter, respBody io.Reader, msg []byte) {
	t.Helper()
	var client paddingConn

	writeErr := make(chan error, 1)
	go func() {
		_, err := client.writeWithPadding(pw, msg)
		writeErr <- err
	}()

	got := make([]byte, 0, len(msg))
	buf := make([]byte, 512)
	for len(got) < len(msg) {
		n, rerr := client.readWithPadding(respBody, buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			t.Fatalf("read: %v", rerr)
		}
	}
	if werr := <-writeErr; werr != nil {
		t.Fatalf("write: %v", werr)
	}
	if string(got) != string(msg) {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
	_ = pw.Close()
}

// TestNaiveProbeResistance verifies that requests which are not valid,
// authenticated naive CONNECTs get an ordinary 404 (never a 200 tunnel or a
// 407 that would reveal a proxy).
func TestNaiveProbeResistance(t *testing.T) {
	echoAddr := startEcho(t)
	naiveAddr := startNaive(t)

	cases := []struct {
		name        string
		user, pass  string
		withPadding bool
	}{
		{"wrong password", "u", "wrong", true},
		{"no credentials", "", "", true},
		{"missing padding", "u", "p", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cc := dialH2(t, naiveAddr)
			// Empty body: a probe just wants the server's response, and a
			// non-2xx CONNECT reply is only returned once the request body ends.
			resp, err := cc.RoundTrip(connectRequest(echoAddr, tc.user, tc.pass, tc.withPadding, nil))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 404 (probe resistance)", resp.StatusCode)
			}
		})
	}
}
