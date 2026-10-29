//go:build !linux

package net

import (
	"io"

	N "github.com/metacubex/sing/common/network"
)

// Non-Linux platforms use sing's public CopyExtended and native ReadWaiter.
func copyRaw(_ io.Writer, _ io.Reader, _, _ []N.CountFunc, _, _ copyErrorHooks) (bool, int64, error) {
	return false, 0, nil
}
