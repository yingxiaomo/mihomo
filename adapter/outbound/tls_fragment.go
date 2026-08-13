package outbound

import (
	"fmt"
	"strconv"
	"strings"

	transportVMess "github.com/metacubex/mihomo/transport/vmess"
)

// TLSFragmentOptions controls TLS write fragmentation behavior.
//
// Example:
// tls-fragment:
//   enabled: true
//   packets: tlshello
//   length: 100-220
//   interval: 1-3
//   max-split: 3-8
type TLSFragmentOptions struct {
	Enabled  bool   `proxy:"enabled,omitempty"`
	Packets  string `proxy:"packets,omitempty"`
	Length   string `proxy:"length,omitempty"`
	Interval string `proxy:"interval,omitempty"`
	MaxSplit string `proxy:"max-split,omitempty"`
}

func (o TLSFragmentOptions) Build() (*transportVMess.TLSFragmentConfig, error) {
	if !o.Enabled && !o.hasCustomValue() {
		return nil, nil
	}

	packetsFrom, packetsTo, err := parseTLSFragmentPackets(o.Packets)
	if err != nil {
		return nil, err
	}

	lengthMin, lengthMax, err := parseTLSFragmentRange(o.Length, 100, 220, false, "length")
	if err != nil {
		return nil, err
	}

	intervalMin, intervalMax, err := parseTLSFragmentRange(o.Interval, 1, 3, true, "interval")
	if err != nil {
		return nil, err
	}

	maxSplitMin, maxSplitMax, err := parseTLSFragmentRange(o.MaxSplit, 3, 8, true, "max-split")
	if err != nil {
		return nil, err
	}

	return &transportVMess.TLSFragmentConfig{
		PacketsFrom: packetsFrom,
		PacketsTo:   packetsTo,
		LengthMin:   lengthMin,
		LengthMax:   lengthMax,
		IntervalMin: intervalMin,
		IntervalMax: intervalMax,
		MaxSplitMin: maxSplitMin,
		MaxSplitMax: maxSplitMax,
	}, nil
}

func (o TLSFragmentOptions) hasCustomValue() bool {
	return strings.TrimSpace(o.Packets) != "" ||
		strings.TrimSpace(o.Length) != "" ||
		strings.TrimSpace(o.Interval) != "" ||
		strings.TrimSpace(o.MaxSplit) != ""
}

func parseTLSFragmentPackets(raw string) (uint64, uint64, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "", "tlshello":
		return 0, 1, nil
	case "all":
		return 0, 0, nil
	}

	from, to, err := parseTLSFragmentRange(v, 0, 0, true, "packets")
	if err != nil {
		return 0, 0, err
	}
	if from == 0 {
		return 0, 0, fmt.Errorf("invalid tls-fragment.packets %q: range start must be >= 1", raw)
	}
	return from, to, nil
}

func parseTLSFragmentRange(raw string, defaultMin, defaultMax uint64, allowZero bool, field string) (uint64, uint64, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return defaultMin, defaultMax, nil
	}

	if !strings.Contains(v, "-") {
		n, err := parseTLSFragmentUint(v, field)
		if err != nil {
			return 0, 0, err
		}
		if !allowZero && n == 0 {
			return 0, 0, fmt.Errorf("invalid tls-fragment.%s %q: value must be >= 1", field, raw)
		}
		return n, n, nil
	}

	parts := strings.SplitN(v, "-", 2)
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	if left == "" || right == "" {
		return 0, 0, fmt.Errorf("invalid tls-fragment.%s %q: must be <min>-<max>", field, raw)
	}

	from, err := parseTLSFragmentUint(left, field)
	if err != nil {
		return 0, 0, err
	}
	to, err := parseTLSFragmentUint(right, field)
	if err != nil {
		return 0, 0, err
	}
	if from > to {
		return 0, 0, fmt.Errorf("invalid tls-fragment.%s %q: min is greater than max", field, raw)
	}
	if !allowZero && from == 0 {
		return 0, 0, fmt.Errorf("invalid tls-fragment.%s %q: min must be >= 1", field, raw)
	}

	return from, to, nil
}

func parseTLSFragmentUint(raw string, field string) (uint64, error) {
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid tls-fragment.%s %q", field, raw)
	}
	return v, nil
}
