package outboundgroup

import (
	"context"
	"errors"
	"fmt"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// ErrSmartPLDNoCandidates identifies an unavailable SmartPLD route, not a
// transport failure on a node. No raw-pool or DIRECT fallback is implied.
var ErrSmartPLDNoCandidates = errors.New("smart-pld: no eligible candidates")

func (s *SmartPLD) noCandidateError(udp bool) error {
	if udp {
		return fmt.Errorf("smart-pld [%s]: no eligible UDP candidates: %w: %w", s.Name(), ErrSmartPLDNoCandidates, C.ErrNotSupport)
	}
	return fmt.Errorf("smart-pld [%s]: %w", s.Name(), ErrSmartPLDNoCandidates)
}

func (s *SmartPLD) unsupportedUDPError(proxy C.Proxy) error {
	return fmt.Errorf("smart-pld [%s]: selected proxy [%s] UDP is not supported: %w", s.Name(), proxy.Name(), C.ErrNotSupport)
}

// unavailableProxy preserves Unwrap's public signature and existing callers'
// non-nil assumption. It is a terminal, non-L3 adapter, not a candidate: its
// inherited Unwrap returns nil and both connection methods fail explicitly.
// In particular it is not Pass/Rematch, so tunnel matching keeps the original
// group/rule and does not proceed to a different route.
func (s *SmartPLD) unavailableProxy(err error) C.Proxy {
	return adapter.NewProxy(&pldUnavailableAdapter{
		Base: outbound.NewBase(outbound.BaseOption{Name: s.Name(), Type: C.Reject}),
		err:  err,
	})
}

type pldUnavailableAdapter struct {
	*outbound.Base
	err error
}

func (p *pldUnavailableAdapter) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, p.err
}

func (p *pldUnavailableAdapter) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	return nil, p.err
}

// GroupBase may expose an unset empty-fallback as a nil entry in fixtures or
// during an empty provider snapshot. Never feed it to selector/node methods.
// Preserve every non-nil proxy, including the configured empty-fallback.
func nonNilPLDProxies(proxies []C.Proxy) []C.Proxy {
	for _, p := range proxies {
		if p == nil {
			result := make([]C.Proxy, 0, len(proxies))
			for _, p := range proxies {
				if p != nil {
					result = append(result, p)
				}
			}
			return result
		}
	}
	return proxies
}
