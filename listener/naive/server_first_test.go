package naive

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/auth"
	"github.com/metacubex/mihomo/component/process"
	C "github.com/metacubex/mihomo/constant"
	authStore "github.com/metacubex/mihomo/listener/auth"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/http"
	"github.com/metacubex/quic-go"
	"github.com/metacubex/quic-go/http3"
	"github.com/metacubex/tls"
)

const directTestChild = "MIHOMO_NAIVE_DIRECT_TEST_CHILD"

// The second real CONNECT also expires a future read deadline on its actual
// H2/H3 request body before entering the real routing/relay implementation.
// This decorator never substitutes its own dial or relay for tunnel.Tunnel.
type deadlineProbeTunnel struct {
	C.Tunnel
	calls   atomic.Int32
	results chan error
}

func (p *deadlineProbeTunnel) HandleTCPConn(conn net.Conn, metadata *C.Metadata) {
	if p.calls.Add(1)%2 == 0 {
		err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
		if err == nil {
			n, readErr := conn.Read(make([]byte, 1))
			if n != 0 || !errors.Is(readErr, os.ErrDeadlineExceeded) {
				err = fmt.Errorf("real HTTP body expiry=(%d,%v), want deadline", n, readErr)
			}
		}
		if err == nil {
			err = conn.SetReadDeadline(time.Time{})
		}
		p.results <- err
		if err != nil {
			_ = conn.Close()
			return
		}
	}
	p.Tunnel.HandleTCPConn(conn, metadata)
}

// Run the actual tunnel in an isolated test process: mode, status, proxies and
// process lookup are global. Never alter another test's routing configuration.
func TestNaiveServerFirstDIRECT(t *testing.T) {
	if os.Getenv(directTestChild) != "1" {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, exe, "-test.run=^TestNaiveServerFirstDIRECT$", "-test.v", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), directTestChild+"=1")
		output, err := cmd.CombinedOutput()
		t.Logf("isolated real tunnel:\n%s", output)
		if err != nil {
			t.Fatalf("real tunnel subprocess: %v", err)
		}
		return
	}
	tunnel.UpdateProxies(map[string]C.Proxy{"DIRECT": adapter.NewProxy(outbound.NewDirect())}, nil)
	tunnel.SetMode(tunnel.Direct)
	tunnel.SetFindProcessMode(process.FindProcessOff)
	tunnel.OnRunning()
	for _, proto := range []string{"h2", "h3"} {
		t.Run(proto, func(t *testing.T) { exerciseServerFirstDIRECT(t, proto) })
	}
}

func exerciseServerFirstDIRECT(t *testing.T, proto string) {
	t.Helper()
	cert, key := selfSignedPEM(t)
	store := authStore.NewAuthStore(auth.NewAuthenticator([]auth.AuthUser{{User: "u", Pass: "p"}}))
	probe := &deadlineProbeTunnel{Tunnel: tunnel.Tunnel, results: make(chan error, 1)}
	listener, err := New(LC.AuthServer{Listen: "127.0.0.1:0", AuthStore: store, Certificate: cert, PrivateKey: key}, "", proto == "h3", inbound.NewListenConfig(), probe)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var open func(string, *io.PipeReader) (io.ReadCloser, int, string, error)
	if proto == "h2" {
		cc := dialH2(t, listener.Address())
		defer cc.Close()
		open = func(target string, pr *io.PipeReader) (io.ReadCloser, int, string, error) {
			resp, err := cc.RoundTrip(connectRequest(target, "u", "p", true, pr).WithContext(ctx))
			if err != nil {
				return nil, 0, "", err
			}
			return resp.Body, resp.StatusCode, resp.Header.Get("Padding"), nil
		}
	} else {
		q, err := quic.DialAddr(ctx, listener.udpListeners[0].LocalAddr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}, ServerName: "example.com"}, &quic.Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer q.CloseWithError(0, "")
		cc := (&http3.Transport{}).NewClientConn(q)
		open = func(target string, pr *io.PipeReader) (io.ReadCloser, int, string, error) {
			req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Host: target}, Host: target, Header: make(http.Header), Body: io.NopCloser(pr)}
			req.Header.Set("Padding", "paddingpaddingpaddingpadding~~~~")
			req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("u:p")))
			resp, err := cc.RoundTrip(req.WithContext(ctx))
			if err != nil {
				return nil, 0, "", err
			}
			return resp.Body, resp.StatusCode, resp.Header.Get("Padding"), nil
		}
	}
	// Two CONNECT streams on the same H2/QUIC connection also prove expiry
	// and closure of one stream do not close the multiplexed listener.
	for i := 0; i < 2; i++ {
		t.Run(fmt.Sprintf("stream-%d", i), func(t *testing.T) {
			target, accepted, serverDone := startSSHBanner(t)
			pr, pw := io.Pipe()
			defer pr.Close()
			defer pw.Close()
			body, status, padding, err := open(target, pr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = pw.Close(); _ = pr.Close(); _ = body.Close() }()
			if status != stdhttp.StatusOK || padding == "" {
				t.Fatalf("CONNECT status=%d Padding=%q", status, padding)
			}
			if i == 1 {
				if err := awaitServerResult(t, probe.results); err != nil {
					t.Fatal(err)
				}
				t.Logf("real %s request-body future deadline expired and was cleared before DIRECT", proto)
			}
			var connected time.Time
			select {
			case connected = <-accepted:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			client := &naiveH2Conn{reader: body, writer: pw}
			defer func() {
				// http2.Response.Body.Close waits for the request writer to
				// exit. connectRequest uses NopCloser, so close both pipe ends
				// ourselves before closing the response / opening a new stream.
				_ = pw.Close()
				_ = pr.Close()
				_ = client.Close()
			}()
			_ = client.SetReadDeadline(connected.Add(350 * time.Millisecond))
			banner := make([]byte, len("SSH-2.0-naive-test\r\n"))
			if _, err := io.ReadFull(client, banner); err != nil {
				t.Fatalf("server-first banner within 350ms: %v", err)
			}
			if string(banner) != "SSH-2.0-naive-test\r\n" {
				t.Fatalf("banner=%q", banner)
			}
			t.Logf("real %s DIRECT banner latency=%v (no client bytes sent)", proto, time.Since(connected))
			// Send client data only after receipt of the unsolicited banner.
			// The sniff timeout must not have destroyed the inbound body.
			time.Sleep(100 * time.Millisecond)
			_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := client.Write([]byte("SSH-2.0-naive-client\r\n")); err != nil {
				t.Fatal(err)
			}
			ack := make([]byte, 3)
			if _, err := io.ReadFull(client, ack); err != nil {
				t.Fatalf("reply after banner: %v", err)
			}
			if string(ack) != "ACK" {
				t.Fatalf("reply=%q", ack)
			}
			if err := awaitServerResult(t, serverDone); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func awaitServerResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("real server operation did not finish")
		return nil
	}
}

func startSSHBanner(t *testing.T) (string, <-chan time.Time, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan time.Time, 1)
	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		accepted <- time.Now()
		if _, err = io.WriteString(conn, "SSH-2.0-naive-test\r\n"); err == nil {
			b := make([]byte, len("SSH-2.0-naive-client\r\n"))
			_, err = io.ReadFull(conn, b)
			if err == nil && !strings.EqualFold(string(b), "SSH-2.0-naive-client\r\n") {
				err = fmt.Errorf("client banner=%q", b)
			}
			if err == nil {
				_, err = io.WriteString(conn, "ACK")
			}
		}
		done <- err
	}()
	return ln.Addr().String(), accepted, done
}
