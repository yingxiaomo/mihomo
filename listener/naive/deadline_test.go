package naive

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type observedBody struct {
	*io.PipeReader
	reads   atomic.Int32
	closes  atomic.Int32
	entered chan struct{}
	exited  chan struct{}
}

func (b *observedBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	select {
	case b.entered <- struct{}{}:
	default:
	}
	n, err := b.PipeReader.Read(p)
	select {
	case b.exited <- struct{}{}:
	default:
	}
	return n, err
}
func (b *observedBody) Close() error { b.closes.Add(1); return b.PipeReader.Close() }

func pipeConn(t *testing.T) (*naiveH2Conn, *observedBody, *io.PipeWriter) {
	t.Helper()
	pr, pw := io.Pipe()
	body := &observedBody{PipeReader: pr, entered: make(chan struct{}, 32), exited: make(chan struct{}, 32)}
	conn := &naiveH2Conn{reader: body, writer: io.Discard}
	t.Cleanup(func() { _ = conn.Close(); _ = pw.Close() })
	return conn, body, pw
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked operation did not reach expected state")
	}
}
func awaitRead(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not unblock")
		return nil
	}
}
func expectTimeout(t *testing.T, n int, err error) {
	t.Helper()
	var ne net.Error
	if n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Read = (%d, %v), want deadline net.Error", n, err)
	}
}

func TestNaiveReadDeadlineRecover(t *testing.T) {
	c, body, pw := pipeConn(t)
	c.readPadding = paddingCount
	buf := bytes.Repeat([]byte{0xa5}, 32)
	if err := c.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := c.Read(buf)
	expectTimeout(t, n, err)
	if body.closes.Load() != 0 {
		t.Fatal("timeout closed request body")
	}
	pending := c.pending
	// Repeated expiry cannot spawn extra workers or consume more stream data.
	for i := 0; i < 20; i++ {
		n, err = c.Read(buf)
		expectTimeout(t, n, err)
		if c.pending != pending {
			t.Fatal("deadline replaced pending worker")
		}
	}
	if body.reads.Load() != 1 {
		t.Fatalf("underlying reads = %d, want 1", body.reads.Load())
	}
	if err = c.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() { _, err := pw.Write([]byte("recovered")); writeDone <- err }()
	awaitSignal(t, body.exited)
	// The worker must never modify the buffer of the timed-out caller.
	if !bytes.Equal(buf, bytes.Repeat([]byte{0xa5}, 32)) {
		t.Fatal("worker changed caller buffer after timeout")
	}
	got := make([]byte, 9)
	if _, err = io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "recovered" {
		t.Fatalf("recovered = %q", got)
	}
	if err = awaitRead(t, writeDone); err != nil {
		t.Fatal(err)
	}
}

func TestNaiveReadDeadlineUpdateBlocked(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline time.Time
	}{
		{"past", time.Now().Add(-time.Hour)}, {"immediate", time.Now()}, {"future", time.Now().Add(100 * time.Millisecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, body, _ := pipeConn(t)
			c.readPadding = paddingCount
			done := make(chan error, 1)
			go func() { _, err := c.Read(make([]byte, 8)); done <- err }()
			awaitSignal(t, body.entered)
			if err := c.SetReadDeadline(tc.deadline); err != nil {
				t.Fatal(err)
			}
			expectTimeout(t, 0, awaitRead(t, done))
			if body.closes.Load() != 0 {
				t.Fatal("deadline closed body")
			}
		})
	}
}

func TestNaiveReadDeadlineRenewAndClear(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(map[bool]string{false: "renew", true: "clear"}[clear], func(t *testing.T) {
			c, body, pw := pipeConn(t)
			c.readPadding = paddingCount
			_ = c.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
			done := make(chan error, 1)
			go func() {
				b := make([]byte, 1)
				_, err := c.Read(b)
				if err == nil && b[0] != 'x' {
					err = errors.New("wrong byte")
				}
				done <- err
			}()
			awaitSignal(t, body.entered)
			until := time.Now().Add(time.Second)
			if clear {
				until = time.Time{}
			}
			if err := c.SetReadDeadline(until); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				t.Fatalf("old timer fired: %v", err)
			case <-time.After(120 * time.Millisecond):
			}
			if _, err := pw.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if err := awaitRead(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNaiveDeadlinePartialPadding(t *testing.T) {
	c, body, pw := pipeConn(t)
	var got []byte
	// Each timeout leaves the same decoder in the middle of a header,
	// payload, or padding. Every supplied byte must be consumed exactly once.
	for _, step := range []struct {
		wire    []byte
		want    string
		timeout bool
	}{
		{[]byte{0}, "", true},
		{[]byte{6, 4, 'a', 'b'}, "ab", false},
		{nil, "", true},
		{[]byte{'c', 'd', 'e', 'f'}, "cdef", false},
		{[]byte{0, 0}, "", true},
		{[]byte{0, 0, 0}, "", true},
		{[]byte{3, 1, 'x', 'y', 'z'}, "xyz", false},
	} {
		_ = c.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
		done := make(chan error, 1)
		buffer := make([]byte, 16)
		var n int
		go func() { var err error; n, err = c.Read(buffer); done <- err }()
		if len(step.wire) > 0 {
			if _, err := pw.Write(step.wire); err != nil {
				t.Fatal(err)
			}
		}
		err := awaitRead(t, done)
		if step.timeout {
			expectTimeout(t, n, err)
		} else if err != nil {
			t.Fatal(err)
		}
		if string(buffer[:n]) != step.want {
			t.Fatalf("payload=%q, want %q", buffer[:n], step.want)
		}
		got = append(got, buffer[:n]...)
		if body.closes.Load() != 0 {
			t.Fatal("partial frame timeout closed body")
		}
		_ = c.SetReadDeadline(time.Time{})
	}
	if string(got) != "abcdefxyz" {
		t.Fatalf("payload = %q", got)
	}
}

type endReader struct {
	sent    bool
	release chan struct{}
}

func (r *endReader) Read(p []byte) (int, error) {
	<-r.release
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	return copy(p, "abcdef"), io.EOF
}
func TestNaiveDeadlineCachedResult(t *testing.T) {
	r := &endReader{release: make(chan struct{})}
	c := &naiveH2Conn{reader: r, writer: io.Discard, paddingConn: paddingConn{readPadding: paddingCount}}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	n, err := c.Read(make([]byte, 10))
	expectTimeout(t, n, err)
	close(r.release)
	_ = c.SetReadDeadline(time.Time{})
	b := make([]byte, 2)
	for _, want := range []string{"ab", "cd", "ef"} {
		n, err := c.Read(b)
		if n != 2 || string(b) != want {
			t.Fatalf("cache Read = %d %q", n, b)
		}
		if want == "ef" {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if n, err := c.Read(nil); n != 0 || err != nil {
		t.Fatalf("empty Read=%d,%v", n, err)
	}
	if n, err := c.Read(b); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("EOF = %d,%v", n, err)
	}
}

func TestNaiveDeadlineClose(t *testing.T) {
	c, body, _ := pipeConn(t)
	c.readPadding = paddingCount
	_ = c.SetReadDeadline(time.Now().Add(time.Hour))
	done := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
	awaitSignal(t, body.entered)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(awaitRead(t, done), net.ErrClosed) {
		t.Fatal("close did not interrupt Read")
	}
	awaitSignal(t, body.exited)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if body.closes.Load() != 1 {
		t.Fatalf("body closed %d times", body.closes.Load())
	}
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	for _, set := range []func(time.Time) error{c.SetDeadline, c.SetReadDeadline, c.SetWriteDeadline} {
		if err := set(time.Time{}); !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	}
	other := &naiveH2Conn{reader: bytes.NewReader(nil), writer: io.Discard}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
}

type deadlineWriter struct {
	bytes.Buffer
	until    time.Time
	flushErr error
	flushes  int
}

func (w *deadlineWriter) SetWriteDeadline(t time.Time) error { w.until = t; return nil }
func (w *deadlineWriter) Write(b []byte) (int, error) {
	if !w.until.IsZero() && !time.Now().Before(w.until) {
		return 0, os.ErrDeadlineExceeded
	}
	return w.Buffer.Write(b)
}
func (w *deadlineWriter) Flush()            { w.flushes++ }
func (w *deadlineWriter) FlushError() error { w.flushes++; return w.flushErr }

type plainFlusher struct{ flushes int }

func (f *plainFlusher) Flush() { f.flushes++ }

type shortWriter struct{ calls int }

func (w *shortWriter) Write([]byte) (int, error) { w.calls++; return 1, nil }

func TestNaiveWritePartialFrameTerminal(t *testing.T) {
	w := &shortWriter{}
	c := &naiveH2Conn{reader: bytes.NewReader(nil), writer: w}
	defer c.Close()
	for i := 0; i < 2; i++ {
		if n, err := c.Write([]byte("data")); n != 0 || !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("partial frame write=%d,%v", n, err)
		}
	}
	if w.calls != 1 {
		t.Fatalf("continued partial framed write: %d calls", w.calls)
	}
}

func TestNaiveWriteDeadline(t *testing.T) {
	w := &deadlineWriter{}
	c := &naiveH2Conn{reader: bytes.NewReader(nil), writer: w, flusher: w}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write=%d,%v", n, err)
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Write([]byte("ok")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("failed framed write resumed: %d,%v", n, err)
	}
	w = &deadlineWriter{}
	c = &naiveH2Conn{reader: bytes.NewReader(nil), writer: w, flusher: w}
	defer c.Close()
	if n, err := c.Write([]byte("ok")); n != 2 || err != nil {
		t.Fatalf("Write=%d,%v", n, err)
	}
	w.flushErr = io.ErrClosedPipe
	if _, err := c.Write([]byte("err")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	f := &plainFlusher{}
	plain := &naiveH2Conn{reader: bytes.NewReader(nil), writer: io.Discard, flusher: f}
	defer plain.Close()
	if err := plain.SetWriteDeadline(time.Now()); !errors.Is(err, os.ErrNoDeadline) {
		t.Fatal(err)
	}
	if err := plain.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Write([]byte("ok")); err != nil || f.flushes != 1 {
		t.Fatalf("flush=%d err=%v", f.flushes, err)
	}
	plain.flusher = nil
	if _, err := plain.Write(nil); err != nil {
		t.Fatal(err)
	}
}
