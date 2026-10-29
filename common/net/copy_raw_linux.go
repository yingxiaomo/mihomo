// SPDX-License-Identifier: GPL-3.0-or-later
// Adapted from github.com/metacubex/sing/common/bufio/splice_linux.go.
// Copyright (C) 2022 by nekohasekai <contact-sagernet@sekai.icu>
// Changes: direction-specific error hooks, interrupted syscall retries,
// successful/partial byte accounting, and safe pre-transfer fallback.

package net

import (
	"fmt"
	"io"
	"syscall"

	N "github.com/metacubex/sing/common/network"
	"golang.org/x/sys/unix"
)

const maxCopySpliceSize = 1 << 20

func copyRaw(destination io.Writer, source io.Reader, readCounters, writeCounters []N.CountFunc, readHooks, writeHooks copyErrorHooks) (handled bool, n int64, err error) {
	src, srcOK := source.(syscall.Conn)
	dst, dstOK := destination.(syscall.Conn)
	if !srcOK || !dstOK {
		return
	}
	rawSource, srcErr := src.SyscallConn()
	rawDestination, dstErr := dst.SyscallConn()
	if srcErr != nil || dstErr != nil {
		return // No Read/Write occurred: use extended I/O, not error sinks.
	}
	return copySplice(rawSource, rawDestination, readCounters, writeCounters, readHooks, writeHooks)
}

func copySplice(source, destination syscall.RawConn, readCounters, writeCounters []N.CountFunc, readHooks, writeHooks copyErrorHooks) (handled bool, n int64, err error) {
	var pipeFDs [2]int
	if err = unix.Pipe2(pipeFDs[:], unix.O_CLOEXEC|unix.O_NONBLOCK); err != nil {
		return false, 0, nil // Pipe resource failure must not lose the transfer.
	}
	defer unix.Close(pipeFDs[0])
	defer unix.Close(pipeFDs[1])
	_, _ = unix.FcntlInt(uintptr(pipeFDs[0]), unix.F_SETPIPE_SZ, maxCopySpliceSize)
	handled = true
	for {
		var readN int64
		var readErr error
			err = source.Read(func(fd uintptr) bool {
				for {
					n, spliceErr := unix.Splice(int(fd), nil, pipeFDs[1], nil, maxCopySpliceSize, unix.SPLICE_F_NONBLOCK)
					readN, readErr = int64(n), spliceErr
					if readErr != unix.EINTR {
						return readErr != unix.EAGAIN
					}
				}
			})
		if err != nil {
			readErr = err
		}
		if readErr != nil {
			if n == 0 && (readErr == unix.EINVAL || readErr == unix.ENOSYS) {
				return false, 0, nil // Unsupported source; no bytes consumed.
			}
			err = fmt.Errorf("splice read: %w", readErr)
			readHooks.report(err)
			return
		}
		if readN == 0 {
			return true, n, nil
		}
		remaining := int(readN)
		var writeErr error
		err = destination.Write(func(fd uintptr) bool {
			for remaining > 0 {
				written, spliceErr := unix.Splice(pipeFDs[0], nil, int(fd), nil, remaining, unix.SPLICE_F_NONBLOCK|unix.SPLICE_F_MOVE)
				if written > 0 {
					remaining -= int(written)
				}
				if spliceErr == unix.EINTR {
					continue
				}
				writeErr = spliceErr
				if spliceErr != nil {
					return spliceErr != unix.EAGAIN
				}
				if written == 0 {
					writeErr = io.ErrNoProgress
					return true
				}
			}
			return true
		})
		// User counters run outside RawConn's descriptor lock, including a
		// successfully written prefix before a later write failure.
		written := readN - int64(remaining)
		n += written
		countCopy(written, readCounters, writeCounters)
		if err != nil {
			writeErr = err
		}
		if writeErr != nil {
			err = fmt.Errorf("splice write: %w", writeErr)
			writeHooks.report(err)
			return
		}
	}
}
