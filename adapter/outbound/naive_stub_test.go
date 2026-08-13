//go:build !with_naive_cronet

package outbound

import (
	"strings"
	"testing"
)

// TestNaiveStubReturnsError verifies that a build without the with_naive_cronet
// tag still compiles and parses naive config, but NewNaive fails with a clear,
// actionable error instead of silently misbehaving.
func TestNaiveStubReturnsError(t *testing.T) {
	proxy, err := NewNaive(NaiveOption{Name: "naive", Server: "example.com", Port: 443})
	if err == nil {
		t.Fatal("expected an error from the naive stub build, got nil")
	}
	if proxy != nil {
		t.Fatalf("expected a nil proxy, got %v", proxy)
	}
	if !strings.Contains(err.Error(), "with_naive_cronet") {
		t.Fatalf("error should point at the build tag, got: %v", err)
	}
}
