package net_test

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
)

// The close callback is part of the actual Smart/PLD wrapper chain. Testing
// observer-only endpoints misses a CloseWrite that accidentally performs Close.
func TestProductionClosureWrapperHalfClose(t *testing.T) {
	for _, chain := range []string{"observer-only", "smart", "smartpld"} {
		t.Run(chain, func(t *testing.T) {
			client, clientPeer := copyTCPPair(t)
			upstream, upstreamPeer := copyTCPPair(t)
			left, _, _ := copyObserved(copyConn(client))
			right, _, _ := copyObserved(copyConn(upstream))
			var finalized atomic.Int64
			if chain == "smart" {
				right = callback.NewFirstReadCallBackConn(right, func(error) {})
				right = callback.NewCloseCallbackConn(right, func() { finalized.Add(1) })
			} else if chain == "smartpld" {
				first := callback.NewFirstReadCallBackConn(copyConn(upstream), func(error) {})
				right, _, _ = copyObserved(first)
				right = callback.NewPacketPairCallBackConn(right)
				right = callback.NewCloseCallbackConn(right, func() { finalized.Add(1) })
			}
			defer right.Close()
			done := make(chan struct{})
			go func() { N.Relay(left, right); close(done) }()
			request, response := []byte("request"), []byte("response-after-FIN")
			if _, err := clientPeer.Write(request); err != nil {
				t.Fatal(err)
			}
			if err := clientPeer.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(upstreamPeer)
			if err != nil || !bytes.Equal(got, request) {
				t.Fatalf("request=%q err=%v", got, err)
			}
			if n := finalized.Load(); n != 0 {
				t.Fatalf("half-close finalized before response: %d", n)
			}
			if _, err = upstreamPeer.Write(response); err != nil {
				t.Fatal(err)
			}
			if err = upstreamPeer.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			got, err = io.ReadAll(clientPeer)
			if err != nil || !bytes.Equal(got, response) {
				t.Fatalf("response=%q want=%q err=%v", got, response, err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("relay blocked after both half-closes")
			}
			// Relay performs full Close in its defer after both copy directions
			// complete, so finalization is now required (but never before response).
			want := int64(1)
			if chain == "observer-only" {
				want = 0
			}
			if n := finalized.Load(); n != want {
				t.Fatalf("relay full Close finalizations=%d want=%d", n, want)
			}
			_ = right.Close()
			_ = right.Close()
			if n := finalized.Load(); n != want {
				t.Fatalf("full Close finalizations=%d want=%d", n, want)
			}
		})
	}
}
