package inbound

import (
	"errors"
	"fmt"
	"strings"

	C "github.com/metacubex/mihomo/constant"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/listener/naive"
	"github.com/metacubex/mihomo/log"
)

type NaiveOption struct {
	BaseOption
	Users          AuthUsers     `inbound:"users,omitempty"`
	Certificate    string        `inbound:"certificate,omitempty"`
	PrivateKey     string        `inbound:"private-key,omitempty"`
	ClientAuthType string        `inbound:"client-auth-type,omitempty"`
	ClientAuthCert string        `inbound:"client-auth-cert,omitempty"`
	EchKey         string        `inbound:"ech-key,omitempty"`
	RealityConfig  RealityConfig `inbound:"reality-config,omitempty"`
	Masquerade     string        `inbound:"masquerade,omitempty"`
	QUIC           bool          `inbound:"quic,omitempty"`
}

func (o NaiveOption) Equal(config C.InboundConfig) bool {
	return optionToString(o) == optionToString(config)
}

type Naive struct {
	*Base
	config *NaiveOption
	l      []*naive.Listener
}

func NewNaive(options *NaiveOption) (*Naive, error) {
	base, err := NewBase(&options.BaseOption)
	if err != nil {
		return nil, err
	}
	return &Naive{
		Base:   base,
		config: options,
	}, nil
}

// Config implements constant.InboundListener
func (n *Naive) Config() C.InboundConfig {
	return n.config
}

// Address implements constant.InboundListener
func (n *Naive) Address() string {
	var addrList []string
	for _, l := range n.l {
		addrList = append(addrList, l.Address())
	}
	return strings.Join(addrList, ",")
}

// Listen implements constant.InboundListener
func (n *Naive) Listen(tunnel C.Tunnel) error {
	lc := n.ListenConfig()
	for _, addr := range strings.Split(n.RawAddress(), ",") {
		l, err := naive.New(
			LC.AuthServer{
				Enable:         true,
				Listen:         addr,
				AuthStore:      n.config.Users.GetAuthStore(),
				Certificate:    n.config.Certificate,
				PrivateKey:     n.config.PrivateKey,
				ClientAuthType: n.config.ClientAuthType,
				ClientAuthCert: n.config.ClientAuthCert,
				EchKey:         n.config.EchKey,
				RealityConfig:  n.config.RealityConfig.Build(),
			},
			n.config.Masquerade,
			n.config.QUIC,
			lc,
			tunnel,
			n.Additions()...,
		)
		if err != nil {
			return err
		}
		n.l = append(n.l, l)
	}
	log.Infoln("Naive[%s] proxy listening at: %s", n.Name(), n.Address())
	return nil
}

// Close implements constant.InboundListener
func (n *Naive) Close() error {
	var errs []error
	for _, l := range n.l {
		if err := l.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close tcp listener %s err: %w", l.Address(), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

var _ C.InboundListener = (*Naive)(nil)
