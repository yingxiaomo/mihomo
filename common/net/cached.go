package net

import (
	"net"

	"github.com/metacubex/mihomo/common/buf"
)

var _ ExtendedConn = (*CachedConn)(nil)

type CachedConn struct {
	ExtendedConn
	data []byte
}

func NewCachedConn(c net.Conn, data []byte) *CachedConn {
	return &CachedConn{NewExtendedConn(c), data}
}

func (c *CachedConn) Read(b []byte) (n int, err error) {
	if len(c.data) > 0 {
		n = copy(b, c.data)
		c.data = c.data[n:]
		return
	}
	return c.ExtendedConn.Read(b)
}

// ReadBuffer must consume the cache too: a non-replaceable outer first-read
// callback can select extended I/O before Copy is allowed to call ReadCached.
func (c *CachedConn) ReadBuffer(buffer *buf.Buffer) error {
	if len(c.data) > 0 {
		_, err := buffer.ReadOnceFrom(c)
		return err
	}
	return c.ExtendedConn.ReadBuffer(buffer)
}

func (c *CachedConn) ReadCached() *buf.Buffer { // call in sing/common/bufio.Copy
	if len(c.data) > 0 {
		data := c.data
		c.data = nil
		return buf.As(data)
	}
	return nil
}

func (c *CachedConn) Upstream() any {
	return c.ExtendedConn
}

func (c *CachedConn) ReaderReplaceable() bool {
	if len(c.data) > 0 {
		return false
	}
	return true
}

func (c *CachedConn) WriterReplaceable() bool {
	return true
}
