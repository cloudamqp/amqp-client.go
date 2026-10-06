package amqp

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Encoding is done by appending to byte slices, which lets the frame writer
// build frames directly in the bufio.Writer's free buffer without copying.

var be = binary.BigEndian

func appendShortStr(b []byte, s string) []byte {
	b = append(b, byte(len(s)))
	return append(b, s...)
}

func appendLongStr(b []byte, s string) []byte {
	b = be.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func appendLongBytes(b []byte, s []byte) []byte {
	b = be.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func appendBits(b []byte, bits ...bool) []byte {
	var v byte
	for i, bit := range bits {
		if bit {
			v |= 1 << i
		}
	}
	return append(b, v)
}

// beginFrame appends a frame header with a placeholder size and returns the
// offset where the payload starts. endFrame patches the size and terminates
// the frame.
func beginFrame(b []byte, typ byte, channel uint16) ([]byte, int) {
	b = append(b, typ, byte(channel>>8), byte(channel), 0, 0, 0, 0)
	return b, len(b)
}

func endFrame(b []byte, start int) []byte {
	be.PutUint32(b[start-4:start], uint32(len(b)-start))
	return append(b, frameEnd)
}

func beginMethod(b []byte, channel uint16, cm uint32) ([]byte, int) {
	b, start := beginFrame(b, frameMethod, channel)
	return be.AppendUint32(b, cm), start
}

// errShortStr is returned when a value doesn't fit in an AMQP short string.
type errShortStr struct{ field, value string }

func (e errShortStr) Error() string {
	return fmt.Sprintf("amqp: %s is longer than 255 bytes: %.40q", e.field, e.value)
}

func checkShortStr(field, value string) error {
	if len(value) > 255 {
		return errShortStr{field, value}
	}
	return nil
}

var errMalformed = errors.New("amqp: malformed frame")

// decoder reads protocol fields from a frame payload. The first error is
// sticky, so callers can read all fields and check err once.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() {
	if d.err == nil {
		d.err = errMalformed
	}
	d.b = nil
}

func (d *decoder) u8() uint8 {
	if len(d.b) < 1 {
		d.fail()
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) u16() uint16 {
	if len(d.b) < 2 {
		d.fail()
		return 0
	}
	v := be.Uint16(d.b)
	d.b = d.b[2:]
	return v
}

func (d *decoder) u32() uint32 {
	if len(d.b) < 4 {
		d.fail()
		return 0
	}
	v := be.Uint32(d.b)
	d.b = d.b[4:]
	return v
}

func (d *decoder) u64() uint64 {
	if len(d.b) < 8 {
		d.fail()
		return 0
	}
	v := be.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *decoder) bytes(n int) []byte {
	if n < 0 || len(d.b) < n {
		d.fail()
		return nil
	}
	v := d.b[:n:n]
	d.b = d.b[n:]
	return v
}

// shortStrBytes returns a short string without copying, it's only valid
// until the read buffer is reused.
func (d *decoder) shortStrBytes() []byte {
	return d.bytes(int(d.u8()))
}

func (d *decoder) shortStr() string {
	return string(d.shortStrBytes())
}

// longStrBytes returns a long string without copying.
func (d *decoder) longStrBytes() []byte {
	n := d.u32()
	if uint64(n) > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	return d.bytes(int(n))
}

func (d *decoder) longStr() string {
	return string(d.longStrBytes())
}
