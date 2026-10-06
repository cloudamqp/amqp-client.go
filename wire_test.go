package amqp

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTableRoundTrip(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	in := Table{
		"bool": true, "false": false, "int8": int8(-1), "uint8": uint8(255),
		"int16": int16(-300), "uint16": uint16(65000), "int32": int32(-70000),
		"uint32": uint32(4000000000), "int64": int64(math.MinInt64), "float32": float32(3.5),
		"float64": math.Pi, "decimal": Decimal{3, -12345}, "string": "héllo",
		"bytes": []byte{0, 1, 2}, "time": ts, "void": nil,
		"table": Table{"nested": Table{"deep": "x"}},
		"array": []any{int32(1), "two", []any{true}, Table{"k": "v"}},
	}
	b, err := appendTable(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	d := decoder{b: b}
	out := d.table()
	if d.err != nil || len(d.b) != 0 {
		t.Fatalf("decode: %v, %d bytes left", d.err, len(d.b))
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip differs:\n in: %#v\nout: %#v", in, out)
	}
}

func TestTableEncodeConversions(t *testing.T) {
	in := Table{
		"int": 7, "uint": uint(8), "uint64": uint64(9), "map": map[string]any{"a": 1},
		"strings": []string{"a", "b"}, "tables": []Table{{"a": "b"}}, "ints": []int{1}, "int64s": []int64{2},
	}
	b, err := appendTable(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	d := decoder{b: b}
	out := d.table()
	want := Table{
		"int": int64(7), "uint": int64(8), "uint64": int64(9), "map": Table{"a": int64(1)},
		"strings": []any{"a", "b"}, "tables": []any{Table{"a": "b"}}, "ints": []any{int64(1)}, "int64s": []any{int64(2)},
	}
	if !reflect.DeepEqual(want, out) {
		t.Fatalf("got %#v", out)
	}
}

func TestTableEncodeErrors(t *testing.T) {
	for name, tbl := range map[string]Table{
		"unsupported type": {"x": struct{}{}},
		"uint64 overflow":  {"x": uint64(math.MaxUint64)},
		"long key":         {strings.Repeat("k", 256): 1},
		"nested":           {"x": Table{"y": make(chan int)}},
	} {
		if _, err := appendTable(nil, tbl); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestTableDecodeMalformed(t *testing.T) {
	good, _ := appendTable(nil, Table{"a": "bcd", "n": Table{"x": []any{int32(1)}}})
	for i := range good {
		d := decoder{b: good[:i]}
		d.table()
		if d.err == nil {
			t.Fatalf("expected an error for %d of %d bytes", i, len(good))
		}
	}
	d := decoder{b: []byte{0, 0, 0, 3, 1, 'a', '?'}}
	d.table()
	if !errors.Is(d.err, errMalformed) {
		t.Fatalf("expected errMalformed for an unknown type, got %v", d.err)
	}
}

func TestDecimalString(t *testing.T) {
	for d, s := range map[Decimal]string{{2, 1234}: "12.34", {0, 5}: "5", {3, -5}: "-0.005"} {
		if d.String() != s {
			t.Errorf("%#v: expected %s, got %s", d, s, d.String())
		}
	}
}

func TestPropertiesRoundTrip(t *testing.T) {
	cases := []Properties{
		{},
		{ContentType: "a"},
		{
			ContentType: "application/json", ContentEncoding: "gzip", Headers: Table{"h": "v"},
			DeliveryMode: Persistent, Priority: 9, CorrelationID: "c", ReplyTo: "r",
			Expiration: "1000", MessageID: "m", Timestamp: time.Unix(1, 0), Type: "t",
			UserID: "u", AppID: "app",
		},
		{Headers: Table{}},
	}
	var cache propCache
	for _, in := range cases {
		b, err := appendProperties(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out Properties
		d := decoder{b: b}
		d.properties(&out, &cache)
		if d.err != nil || len(d.b) != 0 {
			t.Fatalf("decode: %v", d.err)
		}
		if !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip differs:\n in: %#v\nout: %#v", in, out)
		}
	}
}

func TestPropertiesValidate(t *testing.T) {
	p := Properties{MessageID: strings.Repeat("x", 256)}
	if err := p.validate(); err == nil {
		t.Fatal("expected an error for a too long message id")
	}
}

func TestParseURI(t *testing.T) {
	cases := []struct {
		in   string
		want URI
	}{
		{"", URI{Host: "localhost", Port: 5672, Username: "guest", Password: "guest", Vhost: "/"}},
		{"amqp://", URI{Host: "localhost", Port: 5672, Username: "guest", Password: "guest", Vhost: "/"}},
		{"amqp://host", URI{Host: "host", Port: 5672, Username: "guest", Password: "guest", Vhost: "/"}},
		{"amqp://host/", URI{Host: "host", Port: 5672, Username: "guest", Password: "guest", Vhost: "/"}},
		{"amqps://u:p@host:1234/vh", URI{TLS: true, Host: "host", Port: 1234, Username: "u", Password: "p", Vhost: "vh"}},
		{"amqps://host", URI{TLS: true, Host: "host", Port: 5671, Username: "guest", Password: "guest", Vhost: "/"}},
		{"amqp://h/%2f", URI{Host: "h", Port: 5672, Username: "guest", Password: "guest", Vhost: "/"}},
		{"amqp://h/a%2Fb", URI{Host: "h", Port: 5672, Username: "guest", Password: "guest", Vhost: "a/b"}},
		{"amqp://us%40er:p%3Ass@[::1]:5673/", URI{Host: "::1", Port: 5673, Username: "us@er", Password: "p:ss", Vhost: "/"}},
		{"amqp://h?heartbeat=10&frame_max=8192&channel_max=10&name=n&connection_timeout=500&auth_mechanism=external&verify=verify_none",
			URI{Host: "h", Port: 5672, Username: "guest", Password: "guest", Vhost: "/", Heartbeat: 10 * time.Second,
				FrameMax: 8192, ChannelMax: 10, ConnectionName: "n", ConnectTimeout: 500 * time.Millisecond,
				AuthMechanism: "EXTERNAL", InsecureTLS: true}},
		{"amqp://h?heartbeat=0", URI{Host: "h", Port: 5672, Username: "guest", Password: "guest", Vhost: "/", Heartbeat: -1}},
	}
	for _, c := range cases {
		got, err := ParseURI(c.in)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s:\n got %+v\nwant %+v", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"http://h", "amqp://h:99999", "amqp://h?heartbeat=x", "amqp://h?frame_max=-1", "amqp://h/%zz", "://"} {
		if _, err := ParseURI(bad); err == nil {
			t.Errorf("%s: expected an error", bad)
		}
	}
}

func TestURIString(t *testing.T) {
	u, _ := ParseURI("amqps://user:secret@host:1234/my%2Fvhost")
	if s := u.String(); s != "amqps://user@host:1234/my%2Fvhost" {
		t.Fatalf("unexpected %s", s)
	}
}

func TestContainsWord(t *testing.T) {
	if !containsWord("AMQPLAIN PLAIN", "PLAIN") || !containsWord("PLAIN", "PLAIN") ||
		containsWord("AMQPLAIN", "PLAIN") || containsWord("", "PLAIN") {
		t.Fatal("containsWord")
	}
}

func TestCodecs(t *testing.T) {
	c := DefaultCodecs()
	type v struct{ A int }
	for _, enc := range []string{"", "gzip", "deflate", "identity"} {
		p := Properties{ContentType: "application/json; charset=utf-8", ContentEncoding: enc}
		b, err := c.Marshal(v{42}, &p)
		if err != nil {
			t.Fatal(err)
		}
		// Tiny inputs may be stored uncompressed, so check the headers
		if enc == "gzip" && !bytes.HasPrefix(b, []byte{0x1f, 0x8b}) || enc == "deflate" && b[0] != 0x78 {
			t.Fatalf("%s: expected encoded data, got %x", enc, b)
		}
		var out v
		if err := c.Unmarshal(b, &p, &out); err != nil || out.A != 42 {
			t.Fatalf("%s: %v %v", enc, out, err)
		}
	}
	p := Properties{ContentType: "text/plain"}
	if b, err := c.Marshal(12, &p); err != nil || string(b) != "12" {
		t.Fatalf("text: %q %v", b, err)
	}
	var s string
	if err := c.Unmarshal([]byte("hi"), &p, &s); err != nil || s != "hi" {
		t.Fatalf("text: %q %v", s, err)
	}
	var n int
	if err := c.Unmarshal([]byte("1"), &p, &n); err == nil {
		t.Fatal("text: expected an error decoding into *int")
	}
	if _, err := c.Marshal(struct{}{}, &Properties{ContentType: "application/x-unknown"}); !errors.Is(err, ErrUnsupportedContentType) {
		t.Fatalf("expected ErrUnsupportedContentType, got %v", err)
	}
	if err := c.Unmarshal(nil, &Properties{ContentEncoding: "br"}, &s); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("expected ErrUnsupportedContentEncoding, got %v", err)
	}
	if _, err := Gzip.Decode([]byte("not gzip")); err == nil {
		t.Fatal("expected an error decoding invalid gzip")
	}
}

func TestErrorMessages(t *testing.T) {
	e := &Error{Code: NotFound, Reason: "NOT_FOUND - no queue 'q'", ClassID: classQueue, MethodID: 10}
	if e.Error() != "amqp: channel closed: 404 NOT_FOUND - no queue 'q' (queue.declare)" {
		t.Fatal(e.Error())
	}
	if !errors.Is(e, ErrClosed) || !IsCode(e, NotFound) || IsCode(e, AccessRefused) || IsCode(errors.New("x"), NotFound) {
		t.Fatal("unexpected error matching")
	}
	ce := &closedError{errors.New("EOF")}
	if !errors.Is(ce, ErrClosed) || ce.Error() != "amqp: connection lost: EOF" {
		t.Fatal(ce.Error())
	}
}

func TestConfirmTracking(t *testing.T) {
	c := &Connection{done: make(chan struct{})}
	ch := newChannel(c, 1)
	ch.confirmMode.Store(true)
	confs := make([]*Confirmation, 11)
	for tag := uint64(1); tag <= 10; tag++ {
		ch.publishSeq.Store(tag)
		var conf *Confirmation
		if tag%2 == 0 {
			conf = &Confirmation{tag: tag, done: make(chan struct{})}
			confs[tag] = conf
		}
		ch.unconfirmed = append(ch.unconfirmed, unconfirmed{tag, conf})
	}
	resolved := func(tag uint64) bool {
		select {
		case <-confs[tag].Done():
			return true
		default:
			return false
		}
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- ch.WaitForConfirms(context.Background()) }()

	ch.confirmed(4, true, true) // 1-4 acked
	if !resolved(2) || !resolved(4) || resolved(6) || !confs[4].Acked() {
		t.Fatal("expected 2 and 4 resolved")
	}
	ch.confirmed(8, false, false) // out of order nack
	if !resolved(8) || confs[8].Acked() || resolved(6) {
		t.Fatal("expected 8 nacked")
	}
	if err := confs[8].Wait(context.Background()); !errors.Is(err, ErrPublishNacked) {
		t.Fatalf("expected ErrPublishNacked, got %v", err)
	}
	ch.confirmed(5, false, true)
	ch.confirmed(6, false, true)
	select {
	case err := <-waitErr:
		t.Fatalf("WaitForConfirms returned early: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	ch.confirmed(10, true, true) // 7, 9, 10
	if !resolved(10) || len(ch.unconfirmed) != 0 || ch.uhead != 0 {
		t.Fatalf("expected all resolved, %d left", len(ch.unconfirmed)-ch.uhead)
	}
	if err := <-waitErr; !errors.Is(err, ErrPublishNacked) {
		t.Fatalf("expected ErrPublishNacked from WaitForConfirms, got %v", err)
	}
	if err := ch.WaitForConfirms(context.Background()); err != nil {
		t.Fatalf("expected the nack to be reported once, got %v", err)
	}
	// Unknown tags are ignored
	ch.confirmed(99, false, true)
}

// Issue #11
func TestPropertiesContinuationFlags(t *testing.T) {
	b := be.AppendUint16(nil, flagContentType|flagContinuation)
	b = be.AppendUint16(b, 0x8000) // a property this client doesn't know
	b = appendShortStr(b, "text/plain")
	b = append(b, "unknown property"...)
	var p Properties
	d := decoder{b: b}
	if err := d.properties(&p, &propCache{}); err != nil || d.err != nil || p.ContentType != "text/plain" {
		t.Fatalf("unexpected %+v %v %v", p, err, d.err)
	}
}

// Issue #12
func TestDecodedSizeLimit(t *testing.T) {
	for name, enc := range map[string]Encoder{"gzip": NewGzip(1000), "deflate": NewDeflate(1000)} {
		data, err := enc.Encode(make([]byte, 100_000))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := enc.Decode(data); !errors.Is(err, ErrDecodedTooLarge) {
			t.Fatalf("%s: expected ErrDecodedTooLarge, got %v", name, err)
		}
		small, _ := enc.Encode(make([]byte, 1000))
		if out, err := enc.Decode(small); err != nil || len(out) != 1000 {
			t.Fatalf("%s: %d %v", name, len(out), err)
		}
	}
}

// Issue #12
func TestNestingLimit(t *testing.T) {
	var b []byte
	for range 100 {
		b = append([]byte{'A', 0, 0, 0, 0}, b...)
	}
	// fix up the lengths from the inside out
	for i := len(b)/5 - 1; i >= 0; i-- {
		be.PutUint32(b[i*5+1:], uint32(len(b)-(i*5+5)))
	}
	tbl := appendShortStr(nil, "k")
	tbl = append(tbl, b...)
	d := decoder{b: append(be.AppendUint32(nil, uint32(len(tbl))), tbl...)}
	d.table()
	if !errors.Is(d.err, errTooDeep) {
		t.Fatalf("expected errTooDeep, got %v", d.err)
	}
}
