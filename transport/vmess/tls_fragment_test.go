package vmess

import (
	"strings"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type captureConn struct {
	writes [][]byte
}

func (c *captureConn) Read(_ []byte) (int, error) { return 0, nil }

func (c *captureConn) Write(p []byte) (int, error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	c.writes = append(c.writes, cp)
	return len(p), nil
}

func (c *captureConn) Close() error { return nil }

func (c *captureConn) LocalAddr() net.Addr  { return nil }
func (c *captureConn) RemoteAddr() net.Addr { return nil }

func (c *captureConn) SetDeadline(_ time.Time) error      { return nil }
func (c *captureConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *captureConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestTLSFragmentConnClientHello(t *testing.T) {
	rawConn := &captureConn{}
	conn := newTLSFragmentConn(rawConn, &TLSFragmentConfig{
		PacketsFrom: 0,
		PacketsTo:   1,
		LengthMin:   1,
		LengthMax:   1,
		IntervalMin: 0,
		IntervalMax: 0,
		MaxSplitMin: 0,
		MaxSplitMax: 0,
	})

	input := []byte{22, 3, 3, 0, 4, 1, 2, 3, 4}
	n, err := conn.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	require.Len(t, rawConn.writes, 1)

	out := rawConn.writes[0]
	require.Len(t, out, (5+1)*4)
	for i := 0; i < 4; i++ {
		off := i * 6
		require.Equal(t, byte(22), out[off])
		require.Equal(t, byte(3), out[off+1])
		require.Equal(t, byte(3), out[off+2])
		require.Equal(t, byte(0), out[off+3])
		require.Equal(t, byte(1), out[off+4])
		require.Equal(t, byte(i+1), out[off+5])
	}

	_, err = conn.Write([]byte{9, 8, 7})
	require.NoError(t, err)
	require.Len(t, rawConn.writes, 2)
	require.Equal(t, []byte{9, 8, 7}, rawConn.writes[1])
}

func TestTLSFragmentConnPacketRange(t *testing.T) {
	rawConn := &captureConn{}
	conn := newTLSFragmentConn(rawConn, &TLSFragmentConfig{
		PacketsFrom: 2,
		PacketsTo:   2,
		LengthMin:   2,
		LengthMax:   2,
		IntervalMin: 0,
		IntervalMax: 0,
		MaxSplitMin: 0,
		MaxSplitMax: 0,
	})

	first := []byte("abcdef")
	_, err := conn.Write(first)
	require.NoError(t, err)
	require.Len(t, rawConn.writes, 1)
	require.Equal(t, first, rawConn.writes[0])

	second := []byte("abcdef")
	_, err = conn.Write(second)
	require.NoError(t, err)
	require.Len(t, rawConn.writes, 4)
	require.Equal(t, []byte("ab"), rawConn.writes[1])
	require.Equal(t, []byte("cd"), rawConn.writes[2])
	require.Equal(t, []byte("ef"), rawConn.writes[3])
}

func TestTLSFragmentStructureAware(t *testing.T) {
	rawConn := &captureConn{}
	conn := newTLSFragmentConn(rawConn, &TLSFragmentConfig{
		PacketsFrom: 0,
		PacketsTo:   1,
		LengthMin:   80,
		LengthMax:   220,
		IntervalMin: 0,
		IntervalMax: 0,
		MaxSplitMin: 0,
		MaxSplitMax: 0,
	})

	input, data := buildClientHelloRecord(0, []byte{0x00, 0x00}, []byte("a"))

	n, err := conn.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	require.Len(t, rawConn.writes, 1)

	out := rawConn.writes[0]
	// first TLS record length
	firstRecLen := int(out[3])<<8 | int(out[4])

	// compute extensions start offset using the same parser
	extStart, ok := parseClientHelloExtensionsOffset(data)
	require.True(t, ok)

	// ensure first record payload does not include extensions
	require.LessOrEqual(t, firstRecLen, extStart)
}

func TestTLSFragmentStructureAwareServerNameOffset(t *testing.T) {
	rawConn := &captureConn{}
	conn := newTLSFragmentConn(rawConn, &TLSFragmentConfig{
		PacketsFrom: 0,
		PacketsTo:   1,
		LengthMin:   1024,
		LengthMax:   1024,
		IntervalMin: 0,
		IntervalMax: 0,
		MaxSplitMin: 0,
		MaxSplitMax: 0,
	})

	// Put one extension before server_name to verify we cut at server_name offset.
	input, data := buildClientHelloRecord(32, []byte{0x00, 0x17}, []byte("example.invalid"))

	n, err := conn.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	require.Len(t, rawConn.writes, 1)

	out := rawConn.writes[0]
	firstRecLen := int(out[3])<<8 | int(out[4])

	sniOffset, ok := parseClientHelloServerNameOffset(data)
	require.True(t, ok)

	// First record should end before the server_name extension header.
	require.LessOrEqual(t, firstRecLen, sniOffset)
	require.False(t, strings.Contains(string(out[5:5+firstRecLen]), "example.invalid"))
}

func buildClientHelloRecord(sessionIDLen int, extType []byte, serverName []byte) ([]byte, []byte) {
	body := []byte{}

	// legacy_version
	body = append(body, 0x03, 0x03)
	// random (32 bytes)
	body = append(body, make([]byte, 32)...)

	// session_id
	body = append(body, byte(sessionIDLen))
	body = append(body, make([]byte, sessionIDLen)...)

	// cipher_suites_len = 2, one suite 0x00 0x2f
	body = append(body, 0x00, 0x02, 0x00, 0x2f)
	// compression_methods_len = 1, method 0
	body = append(body, 0x01, 0x00)

	// extension before server_name
	preExt := []byte{extType[0], extType[1], 0x00, 0x00}

	// server_name extension
	nameLen := len(serverName)
	serverNameData := []byte{
		byte(((nameLen + 3) >> 8) & 0xff), byte((nameLen + 3) & 0xff),
		0x00,
		byte((nameLen >> 8) & 0xff), byte(nameLen & 0xff),
	}
	serverNameData = append(serverNameData, serverName...)
	sniExt := []byte{0x00, 0x00, byte((len(serverNameData) >> 8) & 0xff), byte(len(serverNameData) & 0xff)}
	sniExt = append(sniExt, serverNameData...)

	extData := append(preExt, sniExt...)
	extBlock := []byte{byte((len(extData) >> 8) & 0xff), byte(len(extData) & 0xff)}
	extBlock = append(extBlock, extData...)
	body = append(body, extBlock...)

	hsLen := len(body)
	hsHeader := []byte{0x01, byte((hsLen >> 16) & 0xff), byte((hsLen >> 8) & 0xff), byte(hsLen & 0xff)}
	data := append(hsHeader, body...)

	recLen := len(data)
	recHeader := []byte{0x16, 0x03, 0x03, byte((recLen >> 8) & 0xff), byte(recLen & 0xff)}
	input := append(recHeader, data...)

	return input, data
}
