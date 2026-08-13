package outbound

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTLSFragmentOptionsBuildDisabled(t *testing.T) {
	cfg, err := (TLSFragmentOptions{}).Build()
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func TestTLSFragmentOptionsBuildDefaults(t *testing.T) {
	cfg, err := (TLSFragmentOptions{Enabled: true}).Build()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, uint64(0), cfg.PacketsFrom)
	require.Equal(t, uint64(1), cfg.PacketsTo)
	require.Equal(t, uint64(100), cfg.LengthMin)
	require.Equal(t, uint64(220), cfg.LengthMax)
	require.Equal(t, uint64(1), cfg.IntervalMin)
	require.Equal(t, uint64(3), cfg.IntervalMax)
	require.Equal(t, uint64(3), cfg.MaxSplitMin)
	require.Equal(t, uint64(8), cfg.MaxSplitMax)
}

func TestTLSFragmentOptionsBuildCustom(t *testing.T) {
	cfg, err := (TLSFragmentOptions{
		Packets:  "2-5",
		Length:   "60-90",
		Interval: "0",
		MaxSplit: "1-2",
	}).Build()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, uint64(2), cfg.PacketsFrom)
	require.Equal(t, uint64(5), cfg.PacketsTo)
	require.Equal(t, uint64(60), cfg.LengthMin)
	require.Equal(t, uint64(90), cfg.LengthMax)
	require.Equal(t, uint64(0), cfg.IntervalMin)
	require.Equal(t, uint64(0), cfg.IntervalMax)
	require.Equal(t, uint64(1), cfg.MaxSplitMin)
	require.Equal(t, uint64(2), cfg.MaxSplitMax)
}

func TestTLSFragmentOptionsBuildInvalidPackets(t *testing.T) {
	_, err := (TLSFragmentOptions{Enabled: true, Packets: "0-2"}).Build()
	require.Error(t, err)
}
