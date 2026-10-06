package amqp

import (
	"fmt"
	"math"
	"math/big"
	"time"
)

// Table is an AMQP field table, used for message headers and for the
// arguments to declarations, bindings and consumers.
//
// These Go types can be encoded:
//
//	nil                 void
//	bool                boolean ('t')
//	int8, uint8         short-short int ('b', 'B')
//	int16, uint16       short int ('s', 'u')
//	int32, uint32       long int ('I', 'i')
//	int, int64, uint,   long-long int ('l')
//	uint64 (≤ MaxInt64)
//	float32, float64    float ('f'), double ('d')
//	Decimal             decimal ('D')
//	string              long string ('S')
//	[]byte              byte array ('x')
//	time.Time           timestamp ('T'), second precision
//	Table,              nested table ('F')
//	map[string]any
//	[]any, []string,    array ('A')
//	[]Table, []int,
//	[]int64
//
// Decoded tables only contain bool, int8, uint8, int16, uint16, int32,
// uint32, int64, float32, float64, Decimal, string, []byte, time.Time, Table,
// []any and nil values.
type Table map[string]any

// Decimal is an AMQP decimal value, Value × 10^-Scale.
type Decimal struct {
	Scale uint8
	Value int32
}

// String formats the decimal, e.g. Decimal{Scale: 2, Value: 1234} is "12.34".
func (d Decimal) String() string {
	return new(big.Rat).SetFrac(big.NewInt(int64(d.Value)), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil)).FloatString(int(d.Scale))
}

// appendTable encodes a field table including its 32-bit length prefix.
func appendTable(b []byte, t Table) ([]byte, error) {
	return appendMap(b, t)
}

func appendMap(b []byte, t map[string]any) ([]byte, error) {
	b = append(b, 0, 0, 0, 0)
	start := len(b)
	var err error
	for k, v := range t {
		if len(k) > 255 {
			return b, fmt.Errorf("amqp: table key longer than 255 bytes: %.40q", k)
		}
		b = appendShortStr(b, k)
		if b, err = appendFieldValue(b, v); err != nil {
			return b, fmt.Errorf("amqp: table key %q: %w", k, err)
		}
	}
	be.PutUint32(b[start-4:], uint32(len(b)-start))
	return b, nil
}

func appendArray[T any](b []byte, a []T) ([]byte, error) {
	b = append(b, 0, 0, 0, 0)
	start := len(b)
	var err error
	for _, v := range a {
		if b, err = appendFieldValue(b, v); err != nil {
			return b, err
		}
	}
	be.PutUint32(b[start-4:], uint32(len(b)-start))
	return b, nil
}

func appendFieldValue(b []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(b, 'V'), nil
	case bool:
		if v {
			return append(b, 't', 1), nil
		}
		return append(b, 't', 0), nil
	case int8:
		return append(b, 'b', byte(v)), nil
	case uint8:
		return append(b, 'B', v), nil
	case int16:
		return be.AppendUint16(append(b, 's'), uint16(v)), nil
	case uint16:
		return be.AppendUint16(append(b, 'u'), v), nil
	case int32:
		return be.AppendUint32(append(b, 'I'), uint32(v)), nil
	case uint32:
		return be.AppendUint32(append(b, 'i'), v), nil
	case int:
		return be.AppendUint64(append(b, 'l'), uint64(v)), nil
	case int64:
		return be.AppendUint64(append(b, 'l'), uint64(v)), nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return b, fmt.Errorf("uint %d overflows int64", v)
		}
		return be.AppendUint64(append(b, 'l'), uint64(v)), nil
	case uint64:
		if v > math.MaxInt64 {
			return b, fmt.Errorf("uint64 %d overflows int64", v)
		}
		return be.AppendUint64(append(b, 'l'), v), nil
	case float32:
		return be.AppendUint32(append(b, 'f'), math.Float32bits(v)), nil
	case float64:
		return be.AppendUint64(append(b, 'd'), math.Float64bits(v)), nil
	case Decimal:
		return be.AppendUint32(append(b, 'D', v.Scale), uint32(v.Value)), nil
	case string:
		return appendLongStr(append(b, 'S'), v), nil
	case []byte:
		return appendLongBytes(append(b, 'x'), v), nil
	case time.Time:
		return be.AppendUint64(append(b, 'T'), uint64(v.Unix())), nil
	case Table:
		return appendMap(append(b, 'F'), v)
	case map[string]any:
		return appendMap(append(b, 'F'), v)
	case []any:
		return appendArray(append(b, 'A'), v)
	case []string:
		return appendArray(append(b, 'A'), v)
	case []Table:
		return appendArray(append(b, 'A'), v)
	case []int:
		return appendArray(append(b, 'A'), v)
	case []int64:
		return appendArray(append(b, 'A'), v)
	}
	return b, fmt.Errorf("unsupported field value type %T", v)
}

func (d *decoder) table() Table {
	n := d.u32()
	if uint64(n) > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	td := decoder{b: d.b[:n]}
	d.b = d.b[n:]
	t := make(Table)
	for len(td.b) > 0 && td.err == nil {
		k := td.shortStr()
		t[k] = td.fieldValue()
	}
	if td.err != nil {
		d.err = td.err
		return nil
	}
	return t
}

func (d *decoder) array() []any {
	n := d.u32()
	if uint64(n) > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	ad := decoder{b: d.b[:n]}
	d.b = d.b[n:]
	a := []any{}
	for len(ad.b) > 0 && ad.err == nil {
		a = append(a, ad.fieldValue())
	}
	if ad.err != nil {
		d.err = ad.err
		return nil
	}
	return a
}

func (d *decoder) fieldValue() any {
	switch d.u8() {
	case 't':
		return d.u8() != 0
	case 'b':
		return int8(d.u8())
	case 'B':
		return d.u8()
	case 's':
		return int16(d.u16())
	case 'u':
		return d.u16()
	case 'I':
		return int32(d.u32())
	case 'i':
		return d.u32()
	case 'l', 'L':
		return int64(d.u64())
	case 'f':
		return math.Float32frombits(d.u32())
	case 'd':
		return math.Float64frombits(d.u64())
	case 'D':
		scale := d.u8()
		return Decimal{Scale: scale, Value: int32(d.u32())}
	case 'S':
		return d.longStr()
	case 'x':
		return append([]byte(nil), d.longStrBytes()...)
	case 'T':
		return time.Unix(int64(d.u64()), 0)
	case 'F':
		return d.table()
	case 'A':
		return d.array()
	case 'V':
		return nil
	}
	d.fail()
	return nil
}
