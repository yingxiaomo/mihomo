package net

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

type copyTestReadObserver struct {
	io.Reader
	errors []error
}

func (r *copyTestReadObserver) UnwrapReadError() (io.Reader, func(error)) {
	return r.Reader, func(err error) { r.errors = append(r.errors, err) }
}
func (r *copyTestReadObserver) Upstream() any { return r.Reader }

type copyTestWriteObserver struct {
	io.Writer
	errors []error
}

func (w *copyTestWriteObserver) UnwrapWriteError() (io.Writer, func(error)) {
	return w.Writer, func(err error) { w.errors = append(w.errors, err) }
}

// This reader only supports the accelerated read-waiter path. Neither Read nor
// ReadBuffer may be used; CopyExtended must retain ReadWaitCreator and hooks.
type copyTestWaitReader struct {
	options N.ReadWaitOptions
	waits   int
}

func (*copyTestWaitReader) Read([]byte) (int, error)                 { panic("Read bypassed read waiter") }
func (*copyTestWaitReader) ReadBuffer(*buf.Buffer) error             { panic("ReadBuffer bypassed read waiter") }
func (r *copyTestWaitReader) CreateReadWaiter() (N.ReadWaiter, bool) { return r, true }
func (r *copyTestWaitReader) InitializeReadWaiter(options N.ReadWaitOptions) bool {
	r.options = options
	return false
}
func (r *copyTestWaitReader) WaitReadBuffer() (*buf.Buffer, error) {
	r.waits++
	if r.waits > 1 {
		return nil, io.EOF
	}
	buffer := r.options.NewBuffer()
	_, _ = buffer.Write([]byte("waiter"))
	r.options.PostReturn(buffer)
	return buffer, nil
}

type copyTestHeadroomWriter struct{ bytes.Buffer }

func (*copyTestHeadroomWriter) FrontHeadroom() int { return 8 }
func (*copyTestHeadroomWriter) RearHeadroom() int  { return 4 }
func (w *copyTestHeadroomWriter) WriteBuffer(buffer *buf.Buffer) error {
	defer buffer.Release()
	if buffer.Start() < 8 || buffer.FreeLen() < 4 {
		return errors.New("headroom lost")
	}
	_, err := w.Write(buffer.Bytes())
	return err
}

func TestCopyExtendedRetainsReadWaiterHeadroomAndErrorHook(t *testing.T) {
	waiter := &copyTestWaitReader{}
	reader := &copyTestReadObserver{Reader: waiter}
	writer := &copyTestHeadroomWriter{}
	n, err := Copy(writer, reader)
	if err != nil || n != 6 || writer.String() != "waiter" || waiter.waits != 2 {
		t.Fatalf("read waiter copy failed: n=%d err=%v data=%q waits=%d", n, err, writer.String(), waiter.waits)
	}
	if waiter.options.FrontHeadroom != 8 || waiter.options.RearHeadroom != 4 || len(reader.errors) != 1 || !errors.Is(reader.errors[0], io.EOF) {
		t.Fatalf("waiter options/hook lost: options=%+v errors=%v", waiter.options, reader.errors)
	}
}

type copyTestHandshakeReader struct {
	io.Reader
	handshakeFailures int
	readError         error
}

func (r *copyTestHandshakeReader) Read(p []byte) (int, error) {
	if r.readError != nil {
		return 0, r.readError
	}
	return r.Reader.Read(p)
}
func (r *copyTestHandshakeReader) HandshakeFailure(error) error {
	r.handshakeFailures++
	return nil
}

type copyTestFailWriter struct {
	err   error
	calls int
}

func (w *copyTestFailWriter) Write([]byte) (int, error) { w.calls++; return 0, w.err }

func TestCopyHandshakeFailureOnlyOnWrite(t *testing.T) {
	failure := errors.New("test transport failure")
	for _, failingRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "read"}[failingRead], func(t *testing.T) {
			handshake := &copyTestHandshakeReader{Reader: bytes.NewBufferString("payload")}
			if failingRead {
				handshake.readError = failure
			}
			reader := &copyTestReadObserver{Reader: handshake}
			failWriter := &copyTestFailWriter{err: failure}
			writer := &copyTestWriteObserver{Writer: failWriter}
			_, err := Copy(writer, reader)
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if failingRead {
				if handshake.handshakeFailures != 0 || len(reader.errors) != 1 || len(writer.errors) != 0 || failWriter.calls != 0 {
					t.Fatalf("read failure charged to write/handshake: hs=%d reads=%v writes=%v", handshake.handshakeFailures, reader.errors, writer.errors)
				}
			} else if handshake.handshakeFailures != 1 || len(writer.errors) != 1 || len(reader.errors) != 0 {
				t.Fatalf("write failure handshake/hook lost: hs=%d reads=%v writes=%v", handshake.handshakeFailures, reader.errors, writer.errors)
			}
		})
	}
}

type copyTestCachedReader struct {
	io.Reader
	cached *buf.Buffer
}

func (r *copyTestCachedReader) ReadCached() *buf.Buffer {
	buffer := r.cached
	r.cached = nil
	return buffer
}

type copyTestShortWriter struct{}

func (*copyTestShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestCopyCachedShortWriteAndTransferredBytes(t *testing.T) {
	reader := &copyTestReadObserver{Reader: &copyTestCachedReader{Reader: bytes.NewReader(nil), cached: buf.As([]byte("cache"))}}
	writer := &copyTestWriteObserver{Writer: &copyTestShortWriter{}}
	n, err := Copy(writer, reader)
	if n != 4 || !errors.Is(err, io.ErrShortWrite) || len(reader.errors) != 0 || len(writer.errors) != 1 || !errors.Is(writer.errors[0], io.ErrShortWrite) {
		t.Fatalf("cached short write lost/misreported: n=%d err=%v reads=%v writes=%v", n, err, reader.errors, writer.errors)
	}
}

type copyTestReadCounter struct {
	io.Reader
	count int64
}

func (r *copyTestReadCounter) UnwrapReader() (io.Reader, []N.CountFunc) {
	return r.Reader, []N.CountFunc{func(n int64) { r.count += n }}
}

type copyTestWriteCounter struct {
	io.Writer
	count int64
}

func (w *copyTestWriteCounter) UnwrapWriter() (io.Writer, []N.CountFunc) {
	return w.Writer, []N.CountFunc{func(n int64) { w.count += n }}
}

type copyTestReaderUpstream struct{ io.Reader }

func (r *copyTestReaderUpstream) UpstreamReader() any { return r.Reader }

type copyTestWriterUpstream struct{ io.Writer }

func (w *copyTestWriterUpstream) UpstreamWriter() any { return w.Writer }

type copyTestLazyReader struct{ *copyTestHandshakeReader }

func (*copyTestLazyReader) ReaderReplaceable() bool         { return false }
func (*copyTestLazyReader) ReaderPossiblyReplaceable() bool { return true }
func (r *copyTestLazyReader) Upstream() any                 { return r.copyTestHandshakeReader }

type copyTestLazyWriter struct{ io.Writer }

func (*copyTestLazyWriter) WriterReplaceable() bool         { return false }
func (*copyTestLazyWriter) WriterPossiblyReplaceable() bool { return true }

func TestCopyAlternatesCountersErrorsAndCachedData(t *testing.T) {
	cached := &copyTestCachedReader{Reader: bytes.NewBufferString("tail"), cached: buf.As([]byte("cache"))}
	innerRead := &copyTestReadCounter{Reader: cached}
	readObserver := &copyTestReadObserver{Reader: innerRead}
	outerRead := &copyTestReadCounter{Reader: readObserver}
	var output bytes.Buffer
	innerWrite := &copyTestWriteCounter{Writer: &output}
	writeObserver := &copyTestWriteObserver{Writer: innerWrite}
	outerWrite := &copyTestWriteCounter{Writer: writeObserver}
	n, err := Copy(outerWrite, outerRead)
	if err != nil || n != 9 || output.String() != "cachetail" {
		t.Fatalf("cached copy: n=%d err=%v output=%q", n, err, output.String())
	}
	for _, count := range []int64{innerRead.count, outerRead.count, innerWrite.count, outerWrite.count} {
		if count != n {
			t.Fatalf("lost or duplicated counter: got %d want %d", count, n)
		}
	}
	if len(writeObserver.errors) != 0 {
		t.Fatal(writeObserver.errors)
	}
}

func TestCopyDiscoveryRetainsDirectionalUpstreamAndBoundsCycles(t *testing.T) {
	readObserver := &copyTestReadObserver{Reader: bytes.NewReader(nil)}
	writeObserver := &copyTestWriteObserver{Writer: io.Discard}
	if !hasCopyErrorObserver(&copyTestReaderUpstream{Reader: readObserver}, true) ||
		!hasCopyErrorObserver(&copyTestWriterUpstream{Writer: writeObserver}, false) {
		t.Fatal("directional upstream observer not discovered")
	}
	if hasCopyErrorObserver(&copyTestReaderUpstream{Reader: bytes.NewReader(nil)}, true) ||
		hasCopyErrorObserver(&copyTestWriterUpstream{Writer: io.Discard}, false) {
		t.Fatal("unobserved chain detected as observed")
	}
	cycle := &copyTestReaderUpstream{}
	cycle.Reader = cycle
	if !hasCopyErrorObserver(cycle, true) {
		t.Fatal("cyclic discovery bypassed observer safety")
	}
}

func TestCopyHookByteOperationsReportActualErrors(t *testing.T) {
	var reads, writes []error
	reader := newCopyHookReader(bytes.NewReader(nil), copyErrorHooks{func(err error) { reads = append(reads, err) }})
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	failure := errors.New("byte write failure")
	writer := newCopyHookWriter(&copyTestFailWriter{err: failure}, copyErrorHooks{func(err error) { writes = append(writes, err) }})
	if _, err := writer.Write([]byte("byte")); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(reads) != 1 || len(writes) != 1 {
		t.Fatalf("reads=%v writes=%v", reads, writes)
	}
}

func TestCopyLazyFirstOperationErrorsAndEOF(t *testing.T) {
	failure := errors.New("lazy transport failure")
	for _, direction := range []string{"read", "write", "eof", "lazy-writer"} {
		t.Run(direction, func(t *testing.T) {
			handshake := &copyTestHandshakeReader{Reader: bytes.NewBufferString("first")}
			if direction == "read" {
				handshake.readError = failure
			}
			reader := &copyTestReadObserver{Reader: &copyTestLazyReader{handshake}}
			var output bytes.Buffer
			var destination io.Writer = &output
			if direction == "write" {
				destination = &copyTestFailWriter{err: failure}
			}
			if direction == "lazy-writer" {
				destination = &copyTestLazyWriter{Writer: &output}
			}
			n, err := Copy(destination, reader)
			if direction == "read" || direction == "write" {
				if n != 0 || !errors.Is(err, failure) {
					t.Fatalf("n=%d err=%v", n, err)
				}
				wantHandshake := 0
				if direction == "write" {
					wantHandshake = 1
				}
				if handshake.handshakeFailures != wantHandshake {
					t.Fatal(handshake.handshakeFailures)
				}
			} else if err != nil || n != 5 || output.String() != "first" {
				t.Fatalf("lazy copy/EOF: n=%d err=%v output=%q", n, err, output.String())
			}
		})
	}
}

func TestCopyWithoutObserverAndNilEndpoints(t *testing.T) {
	var destination bytes.Buffer
	n, err := Copy(&destination, bytes.NewBufferString("unobserved"))
	if n != 10 || err != nil || destination.String() != "unobserved" {
		t.Fatalf("n=%d err=%v", n, err)
	}
	var baseline bytes.Buffer
	baselineN, baselineErr := bufio.Copy(&baseline, bytes.NewBufferString("unobserved"))
	if n != baselineN || err != baselineErr || destination.String() != baseline.String() {
		t.Fatal("unobserved copy changed sing behavior")
	}
	if _, err := Copy(io.Discard, nil); err == nil {
		t.Fatal("nil source accepted")
	}
	if _, err := Copy(nil, bytes.NewReader(nil)); err == nil {
		t.Fatal("nil destination accepted")
	}
}
