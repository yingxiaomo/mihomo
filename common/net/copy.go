package net

import (
	"errors"
	"io"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

// ReadErrorUnwrapper and WriteErrorUnwrapper let the relay retain observers
// while unwrapping to a raw socket. Ordinary sing copies must not unwrap them.
// The returned sink receives only errors from that endpoint's I/O direction.
type ReadErrorUnwrapper interface {
	UnwrapReadError() (io.Reader, func(error))
}

type WriteErrorUnwrapper interface {
	UnwrapWriteError() (io.Writer, func(error))
}

type copyErrorHooks []func(error)

func (hooks copyErrorHooks) report(err error) {
	if err != nil {
		for _, hook := range hooks {
			hook(err)
		}
	}
}

func unwrapCopyReader(reader io.Reader, counters []N.CountFunc, hooks copyErrorHooks) (io.Reader, []N.CountFunc, copyErrorHooks) {
	for {
		reader, counters = N.UnwrapCountReader(reader, counters)
		observer, ok := reader.(ReadErrorUnwrapper)
		if !ok {
			return reader, counters, hooks
		}
		var hook func(error)
		reader, hook = observer.UnwrapReadError()
		if hook != nil {
			hooks = append(hooks, hook)
		}
	}
}

func unwrapCopyWriter(writer io.Writer, counters []N.CountFunc, hooks copyErrorHooks) (io.Writer, []N.CountFunc, copyErrorHooks) {
	for {
		writer, counters = N.UnwrapCountWriter(writer, counters)
		observer, ok := writer.(WriteErrorUnwrapper)
		if !ok {
			return writer, counters, hooks
		}
		var hook func(error)
		writer, hook = observer.UnwrapWriteError()
		if hook != nil {
			hooks = append(hooks, hook)
		}
	}
}

func countCopy(n int64, readCounters, writeCounters []N.CountFunc) {
	if n > 0 {
		for _, counter := range readCounters {
			counter(n)
		}
		for _, counter := range writeCounters {
			counter(n)
		}
	}
}

// Discovery does not replace a wrapper or perform I/O. In particular, a
// first-read callback may hide the observer until its first successful read.
// Unwrap it only in the Copy loop after ReaderReplaceable permits doing so.
func hasCopyErrorObserver(value any, read bool) bool {
	for depth := 0; depth < 64; depth++ {
		if read {
			if _, ok := value.(ReadErrorUnwrapper); ok {
				return true
			}
			if upstream, ok := value.(N.WithUpstreamReader); ok {
				value = upstream.UpstreamReader()
				continue
			}
		} else {
			if _, ok := value.(WriteErrorUnwrapper); ok {
				return true
			}
			if upstream, ok := value.(N.WithUpstreamWriter); ok {
				value = upstream.UpstreamWriter()
				continue
			}
		}
		if upstream, ok := value.(common.WithUpstream); ok {
			value = upstream.Upstream()
			continue
		}
		if read {
			if counter, ok := value.(N.ReadCounter); ok {
				value, _ = counter.UnwrapReader()
				continue
			}
		} else if counter, ok := value.(N.WriteCounter); ok {
			value, _ = counter.UnwrapWriter()
			continue
		}
		return false
	}
	return true // Unusually deep/cyclic chains must not bypass observers.
}

// Copy is the production stream copy engine. It collects sing counters and
// direction-specific error sinks before selecting raw splice or extended I/O.
// A source read error is never reported to a destination write observer.
func Copy(destination io.Writer, source io.Reader) (n int64, err error) {
	if source == nil {
		return 0, errors.New("nil reader")
	}
	if destination == nil {
		return 0, errors.New("nil writer")
	}
	if !hasCopyErrorObserver(source, true) && !hasCopyErrorObserver(destination, false) {
		return bufio.Copy(destination, source)
	}
	originSource := source
	var readCounters, writeCounters []N.CountFunc
	var readHooks, writeHooks copyErrorHooks
	possibly := bufio.MaxCopyExtendedOnceTimes
	for {
		source, readCounters, readHooks = unwrapCopyReader(source, readCounters, readHooks)
		destination, writeCounters, writeHooks = unwrapCopyWriter(destination, writeCounters, writeHooks)
		if cached, ok := source.(N.CachedReader); ok {
			if buffer := cached.ReadCached(); buffer != nil {
				length := buffer.Len()
				written, writeErr := destination.Write(buffer.Bytes())
				buffer.Release()
				if writeErr == nil && written != length {
					writeErr = io.ErrShortWrite
				}
				n += int64(written)
				countCopy(int64(written), readCounters, writeCounters)
				if writeErr != nil {
					writeHooks.report(writeErr)
					if n == int64(written) {
						writeErr = N.ReportHandshakeFailure(originSource, writeErr)
					}
					return n, writeErr
				}
				continue
			}
		}
		reader, readerMayChange := source.(N.ReaderPossiblyReplaceable)
		writer, writerMayChange := destination.(N.WriterPossiblyReplaceable)
		if possibly > 0 && ((readerMayChange && reader.ReaderPossiblyReplaceable()) ||
			(writerMayChange && writer.WriterPossiblyReplaceable())) {
			possibly--
			// One I/O may complete a lazy handshake and expose a raw socket.
			var readFailed bool
			onceReadHooks := copyErrorHooks{func(err error) { readFailed = true; readHooks.report(err) }}
			copied, copyErr := bufio.CopyExtendedOnce(
				newCopyHookWriter(destination, writeHooks), newCopyHookReader(source, onceReadHooks), readCounters, writeCounters)
			n += copied
			if copyErr != nil {
				if n == copied && !readFailed {
					copyErr = N.ReportHandshakeFailure(originSource, copyErr)
				}
				if errors.Is(copyErr, io.EOF) {
					copyErr = nil
				}
				return n, copyErr
			}
			continue
		}
		if handled, copied, copyErr := copyRaw(destination, source, readCounters, writeCounters, readHooks, writeHooks); handled {
			return n + copied, copyErr
		}
		copied, copyErr := bufio.CopyExtended(originSource,
			newCopyHookWriter(destination, writeHooks), newCopyHookReader(source, readHooks), readCounters, writeCounters)
		return n + copied, copyErr
	}
}

// The hook adapters intentionally do not implement Reader/WriterReplaceable:
// CopyExtended must observe the actual operation, not just its final error.
// Upstream retains sing's headroom/MTU discovery and ReadWaitCreator preserves
// optimized ReadWaiter (including native socket read waiters) on fallback paths.
type copyHookReader struct {
	N.ExtendedReader
	reader io.Reader
	hooks  copyErrorHooks
}

func newCopyHookReader(reader io.Reader, hooks copyErrorHooks) *copyHookReader {
	return &copyHookReader{bufio.NewExtendedReader(reader), reader, hooks}
}

func (r *copyHookReader) Read(p []byte) (int, error) {
	n, err := r.ExtendedReader.Read(p)
	r.hooks.report(err)
	return n, err
}

func (r *copyHookReader) ReadBuffer(buffer *buf.Buffer) error {
	err := r.ExtendedReader.ReadBuffer(buffer)
	r.hooks.report(err)
	return err
}

func (r *copyHookReader) Upstream() any { return r.reader }

func (r *copyHookReader) CreateReadWaiter() (N.ReadWaiter, bool) {
	waiter, ok := bufio.CreateReadWaiter(r.reader)
	if !ok {
		return nil, false
	}
	return &copyHookReadWaiter{waiter, r.hooks}, true
}

type copyHookReadWaiter struct {
	N.ReadWaiter
	hooks copyErrorHooks
}

func (r *copyHookReadWaiter) WaitReadBuffer() (*buf.Buffer, error) {
	buffer, err := r.ReadWaiter.WaitReadBuffer()
	r.hooks.report(err)
	return buffer, err
}

type copyHookWriter struct {
	N.ExtendedWriter
	writer io.Writer
	hooks  copyErrorHooks
}

func newCopyHookWriter(writer io.Writer, hooks copyErrorHooks) *copyHookWriter {
	return &copyHookWriter{bufio.NewExtendedWriter(writer), writer, hooks}
}

func (w *copyHookWriter) Write(p []byte) (int, error) {
	n, err := w.ExtendedWriter.Write(p)
	w.hooks.report(err)
	return n, err
}

func (w *copyHookWriter) WriteBuffer(buffer *buf.Buffer) error {
	err := w.ExtendedWriter.WriteBuffer(buffer)
	w.hooks.report(err)
	return err
}

func (w *copyHookWriter) Upstream() any { return w.writer }
