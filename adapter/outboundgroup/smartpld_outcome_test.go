package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/bufio"
)

func newOutcomeTestGroup(t *testing.T) (*SmartPLD, C.Proxy, *C.Metadata) {
	t.Helper()
	db, e := bbolt.Open(filepath.Join(t.TempDir(), "smart.db"), 0600, nil)
	if e != nil {
		t.Fatal(e)
	}
	db.MaxBatchDelay = time.Millisecond
	s := &SmartPLD{GroupBase: NewGroupBase(GroupBaseOption{Name: fmt.Sprintf("outcome-%d", time.Now().UnixNano())}), store: smart.NewStore(db), configName: "cfg", ctx: context.Background(), usePLD: true, sampleRate: 1, failureCooldown: time.Minute}
	s.maxFailedTimes = 20
	s.hostFailLimit.Store(20)
	t.Cleanup(func() {
		s.rewardCache().Close()
		s.store.FlushQueue(true)
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	p := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "A"}))
	md := &C.Metadata{NetWork: C.TCP, Host: "verify.example", SmartTarget: "verify.example", WildcardTarget: "verify.example", DstPort: 80, DstIPASN: "unknown"}
	return s, p, md
}
func outcomeHostStrikes(t *testing.T, s *SmartPLD, md *C.Metadata, node string) int {
	t.Helper()
	s.store.FlushQueue(true)
	data, e := s.store.DBViewGetItem(smart.FormatDBKey(smart.KeyTypeHostFailures, s.configName, s.Name(), md.WildcardTarget))
	if e != nil {
		if e.Error() == "item not found" || e.Error() == "bucket not found" {
			return 0 // no failure state has ever been stored for this fixture
		}
		t.Fatal(e)
	}
	if len(data) == 0 {
		return 0
	}
	var hs smart.HostStatus
	if e = json.Unmarshal(data, &hs); e != nil {
		t.Fatal(e)
	}
	total := 0
	for _, cs := range hs.Codes {
		if cs != nil {
			total += cs.FailCounts[node]
			if _, blocked := cs.Nodes[node]; blocked {
				total++
			}
		}
	}
	return total
}
func recordOutcomeSample(s *SmartPLD, p C.Proxy, md *C.Metadata, o *pldConnectionOutcome, e error, download int64) {
	s.recordConnectionStats(md, p, 5, 20, 1024, download, 1024, 1024, 1000, nil, 0, e, o)
}
func TestPLDConnectionOutcomeExactlyOnce(t *testing.T) {
	real := errors.New("upstream connection reset")
	cases := []struct {
		name               string
		gate, twice, local bool
		err                error
		download           int64
		strikes            int
		samples            int64
	}{
		{"healthy", false, false, false, nil, 1024, 0, 1},
		{"transport_error", false, false, false, real, 1024, 1, 1},
		{"gate_then_error", true, false, false, real, 1024, 1, 1},
		{"gate_then_healthy_close", true, false, false, nil, 1024, 1, 1},
		{"both_gates_then_error", true, true, false, real, 1024, 1, 1},
		{"local_cancel", false, false, true, nil, 0, 0, 0},
		{"wrapped_local_cancel", false, false, false, fmt.Errorf("stream: %w", context.Canceled), 0, 0, 0},
		{"local_then_real_failure", false, false, true, real, 0, 1, 1},
		{"normal_eof", false, false, false, io.EOF, 1024, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			o := &pldConnectionOutcome{}
			if tc.local {
				if e := o.readError(context.Canceled); e != nil {
					t.Fatal(e)
				}
			}
			if tc.gate && !s.failPLDConnection(o, md, p.Name(), 3) {
				t.Fatal("first gate did not record")
			}
			if tc.twice && s.failPLDConnection(o, md, p.Name(), 7) {
				t.Fatal("second gate charged again")
			}
			o.finish()
			recordOutcomeSample(s, p, md, o, tc.err, tc.download)
			// A repeated final callback cannot record either layer again.
			recordOutcomeSample(s, p, md, o, tc.err, tc.download)
			if got := outcomeHostStrikes(t, s, md, p.Name()); got != tc.strikes {
				t.Fatalf("host strikes=%d want=%d", got, tc.strikes)
			}
			_, _, count := s.getPLDRecord(md.SmartTarget, p.Name()).ChannelStats(false)
			if count != tc.samples {
				t.Fatalf("PLD updates=%d want=%d", count, tc.samples)
			}
			if s.failPLDConnection(o, md, p.Name(), 3) {
				t.Fatal("late gate punished completed connection")
			}
		})
	}
}
func TestPLDConcurrentGateAndClose(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	for i := 0; i < 32; i++ {
		m := *md
		m.Host = fmt.Sprintf("race-%d.example", i)
		m.SmartTarget = m.Host
		m.WildcardTarget = m.Host
		o := &pldConnectionOutcome{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; s.failPLDConnection(o, &m, p.Name(), 3) }()
		go func() {
			defer wg.Done()
			<-start
			o.finish()
			recordOutcomeSample(s, p, &m, o, errors.New("reset"), 1024)
		}()
		close(start)
		wg.Wait()
		if n := outcomeHostStrikes(t, s, &m, p.Name()); n != 1 {
			t.Fatalf("iteration %d strikes=%d", i, n)
		}
		_, _, n := s.getPLDRecord(m.SmartTarget, p.Name()).ChannelStats(false)
		if n != 1 {
			t.Fatalf("iteration %d PLD samples=%d", i, n)
		}
	}
}
func TestPLDOldGatePreservesNewPrimary(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	b := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "B"}))
	s.store.StoreUnwrapResult(s.Name(), s.configName, md.SmartTarget, []C.Proxy{p})
	s.store.ForceStoreUnwrapResult(s.Name(), s.configName, md.SmartTarget, []C.Proxy{b})
	if !s.failPLDConnection(&pldConnectionOutcome{}, md, p.Name(), 3) {
		t.Fatal("gate did not fire")
	}
	got, _ := s.store.GetUnwrapResult(s.Name(), s.configName, md.SmartTarget)
	if len(got) != 1 || got[0] != "B" {
		t.Fatalf("old A gate evicted current B: %v", got)
	}
}

type outcomeReadErrorConn struct {
	net.Conn
	err error
}

func (c *outcomeReadErrorConn) Read([]byte) (int, error) { return 0, c.err }
func TestPLDFirstReadLocalCloseIsNeutral(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	r, w := net.Pipe()
	defer w.Close()
	c := s.WrapConnWithMetric(outbound.NewConn(&outcomeReadErrorConn{Conn: r, err: context.Canceled}, p.Adapter()), p, md, 5)
	tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 1024, 0, false)
	if _, e := tracker.Read(make([]byte, 1)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e := tracker.Close(); e != nil {
		t.Fatal(e)
	}
	// Closure statistics are asynchronous. A canceled, response-less stream
	// must remain neutral, not be turned into code-4 zero-traffic degradation.
	time.Sleep(30 * time.Millisecond)
	if got := outcomeHostStrikes(t, s, md, p.Name()); got != 0 {
		t.Fatal(got)
	}
	_, _, n := s.getPLDRecord(md.SmartTarget, p.Name()).ChannelStats(false)
	if n != 0 {
		t.Fatalf("local cancel updated reward: %d", n)
	}
}
func TestPLDPacketPairActualMetricRelay(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	r, w := net.Pipe()
	defer r.Close()
	defer w.Close()
	c := s.WrapConnWithMetric(outbound.NewConn(r, p.Adapter()), p, md, 5)
	defer c.Close()
	var pp *callback.PacketPairCallBackConn
	var current any = c
	for i := 0; i < 12; i++ {
		if x, ok := current.(*callback.PacketPairCallBackConn); ok {
			pp = x
			break
		}
		if x, ok := current.(interface{ Upstream() any }); ok {
			current = x.Upstream()
		} else {
			break
		}
	}
	if pp == nil {
		t.Fatal("measurement wrapper missing")
	}
	done := make(chan error, 1)
	go func() {
		defer w.Close()
		_, e := w.Write(make([]byte, callback.PPProbeBytes/2))
		if e == nil {
			time.Sleep(15 * time.Millisecond)
			_, e = w.Write(make([]byte, callback.PPProbeBytes/2))
		}
		done <- e
	}()
	n, e := bufio.Copy(io.Discard, c)
	if e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if n != callback.PPProbeBytes || pp.Readout().BytesTo64KMs <= 0 {
		t.Fatalf("actual metric chain lost measurement: bytes=%d timing=%+v", n, pp.Readout())
	}
	t.Logf("actual WrapConnWithMetric relay bytes=%d timing=%+v", n, pp.Readout())
}
