package outboundgroup

import (
	"testing"
	"time"
)

func TestTTFRShadowSendConfirmationBeforeRegistration(t *testing.T) {
	o := &pldTTFRObservation{isUDP: true}
	if o.beginPacketSend(0) != nil {
		t.Fatal("empty send created an observation")
	}
	o.finishPacketSend(nil, 0)
	send := o.beginPacketSend(1)
	o.downlink(1)
	// The closer observed a still-pending send, but confirmation won the
	// race before it registered the metric-only continuation.
	o.finishPacketSend(send, 1)
	calls := 0
	o.afterPacketSettlement(func(ms float64) {
		calls++
		if ms < 0 {
			t.Fatal("negative response time")
		}
	})
	o.afterPacketSettlement(func(float64) { t.Fatal("duplicate registration") })
	if calls != 1 {
		t.Fatalf("confirmed response not delivered exactly once: %d", calls)
	}
}

func TestTTFRShadowWrapperCapabilitiesAndPendingOrder(t *testing.T) {
	tcp := &pldTTFRConn{observation: &pldTTFRObservation{origin: time.Now()}}
	if tcp.ReaderReplaceable() || !tcp.WriterReplaceable() || tcp.Upstream() != tcp.Conn {
		t.Fatal("TCP wrapper changed copy capability or upstream identity")
	}
	o := &pldTTFRObservation{isUDP: true}
	pc := &pldTTFRPacketConn{observation: o}
	if pc.ReaderReplaceable() || pc.WriterReplaceable() || pc.Upstream() != pc.PacketConn {
		t.Fatal("unobserved UDP wrapper became peelable")
	}
	first, second := o.beginPacketSend(1), o.beginPacketSend(1)
	o.downlink(1)
	o.finishPacketSend(second, 1)
	if !pc.ReaderReplaceable() || !pc.WriterReplaceable() || o.beginPacketSend(1) != nil {
		t.Fatal("positive payload/response did not preserve UDP capability")
	}
	calls := 0
	o.afterPacketSettlement(func(float64) { calls++ })
	if calls != 0 {
		t.Fatal("first pending send's origin was decided before confirmation")
	}
	o.finishPacketSend(first, 1)
	if calls != 1 || !o.origin.Equal(first.start) {
		t.Fatal("completion lost earliest confirmed send origin")
	}
}
