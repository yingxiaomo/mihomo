//go:build !with_naive_cronet

package outbound

import (
	"fmt"

	C "github.com/metacubex/mihomo/constant"
)

type Naive struct {
	*Base
}

// NaiveOption mirrors the real one so config parsing still succeeds; the outbound
// itself is unavailable unless built with the "with_naive_cronet" tag.
type NaiveOption struct {
	BasicOption
	Name     string `proxy:"name"`
	Server   string `proxy:"server"`
	Port     int    `proxy:"port"`
	UserName string `proxy:"username,omitempty"`
	Password string `proxy:"password,omitempty"`

	SNI            string `proxy:"sni,omitempty"`
	SkipCertVerify bool   `proxy:"skip-cert-verify,omitempty"`

	QUIC                  bool              `proxy:"quic,omitempty"`
	QUICCongestionControl string            `proxy:"quic-congestion-control,omitempty"`
	InsecureConcurrency   int               `proxy:"insecure-concurrency,omitempty"`
	ExtraHeaders          map[string]string `proxy:"extra-headers,omitempty"`
	Certificate           []string          `proxy:"certificate,omitempty"`
	CertificatePath       string            `proxy:"certificate-path,omitempty"`
}

func NewNaive(option NaiveOption) (*Naive, error) {
	return nil, fmt.Errorf("%w: naive requires the \"with_naive_cronet\" build tag (plus \"with_purego\" or \"with_musl\")", C.ErrProxyUnsupported)
}
