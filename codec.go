package amqp

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

var (
	// ErrUnsupportedContentType is returned when no Serializer is
	// registered for a message's content type.
	ErrUnsupportedContentType = errors.New("amqp: unsupported content type")
	// ErrUnsupportedContentEncoding is returned when no Encoder is
	// registered for a message's content encoding.
	ErrUnsupportedContentEncoding = errors.New("amqp: unsupported content encoding")
)

// Serializer converts values to and from message bodies of a content type,
// such as "application/json".
type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// Encoder applies a content encoding, such as "gzip", to message bodies.
type Encoder interface {
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}

// Codecs is a registry of serializers by content type and encoders by
// content encoding. Register all codecs before the registry is used, it's
// not safe to modify concurrently with use.
type Codecs struct {
	serializers map[string]Serializer
	encoders    map[string]Encoder
}

// NewCodecs returns an empty registry.
func NewCodecs() *Codecs {
	return &Codecs{serializers: map[string]Serializer{}, encoders: map[string]Encoder{}}
}

// DefaultCodecs returns a registry with the built-in codecs:
// "application/json" ([JSON]), "text/plain" ([Text]), "gzip" ([Gzip]) and
// "deflate" ([Deflate]).
func DefaultCodecs() *Codecs {
	return NewCodecs().
		RegisterSerializer("application/json", JSON).
		RegisterSerializer("text/plain", Text).
		RegisterEncoder("gzip", Gzip).
		RegisterEncoder("deflate", Deflate)
}

var defaultCodecs = DefaultCodecs()

// RegisterSerializer registers a serializer for a content type. Parameters
// such as "; charset=utf-8" are ignored when looking up a content type.
func (c *Codecs) RegisterSerializer(contentType string, s Serializer) *Codecs {
	c.serializers[strings.ToLower(contentType)] = s
	return c
}

// RegisterEncoder registers an encoder for a content encoding.
func (c *Codecs) RegisterEncoder(contentEncoding string, e Encoder) *Codecs {
	c.encoders[strings.ToLower(contentEncoding)] = e
	return c
}

func (c *Codecs) serializer(contentType string) Serializer {
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = contentType[:i]
	}
	return c.serializers[strings.ToLower(strings.TrimSpace(contentType))]
}

func (c *Codecs) encoder(contentEncoding string) (Encoder, error) {
	switch contentEncoding {
	case "", "identity":
		return nil, nil
	}
	if e := c.encoders[strings.ToLower(contentEncoding)]; e != nil {
		return e, nil
	}
	return nil, fmt.Errorf("%w %q", ErrUnsupportedContentEncoding, contentEncoding)
}

// Marshal serializes v according to p.ContentType and then encodes it
// according to p.ContentEncoding. A []byte or string v isn't serialized,
// only encoded.
func (c *Codecs) Marshal(v any, p *Properties) ([]byte, error) {
	var data []byte
	switch v := v.(type) {
	case nil:
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		s := c.serializer(p.ContentType)
		if s == nil {
			return nil, fmt.Errorf("%w %q for %T", ErrUnsupportedContentType, p.ContentType, v)
		}
		var err error
		if data, err = s.Marshal(v); err != nil {
			return nil, err
		}
	}
	e, err := c.encoder(p.ContentEncoding)
	if err != nil || e == nil {
		return data, err
	}
	return e.Encode(data)
}

// Unmarshal decodes body according to p.ContentEncoding and deserializes
// it into v according to p.ContentType. If v is a *[]byte or *string it's
// set to the decoded body.
func (c *Codecs) Unmarshal(body []byte, p *Properties, v any) error {
	e, err := c.encoder(p.ContentEncoding)
	if err != nil {
		return err
	}
	if e != nil {
		if body, err = e.Decode(body); err != nil {
			return err
		}
	}
	switch v := v.(type) {
	case *[]byte:
		*v = body
		return nil
	case *string:
		*v = string(body)
		return nil
	}
	s := c.serializer(p.ContentType)
	if s == nil {
		return fmt.Errorf("%w %q", ErrUnsupportedContentType, p.ContentType)
	}
	return s.Unmarshal(body, v)
}

type jsonSerializer struct{}

func (jsonSerializer) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonSerializer) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// JSON serializes values with encoding/json.
var JSON Serializer = jsonSerializer{}

type textSerializer struct{}

func (textSerializer) Marshal(v any) ([]byte, error) {
	switch v := v.(type) {
	case fmt.Stringer:
		return []byte(v.String()), nil
	case error:
		return []byte(v.Error()), nil
	}
	return fmt.Append(nil, v), nil
}

func (textSerializer) Unmarshal(data []byte, v any) error {
	return fmt.Errorf("amqp: text/plain can only be decoded into a *string or *[]byte, not %T", v)
}

// Text serializes values with fmt, e.g. numbers and fmt.Stringers. It can
// only decode into *string and *[]byte.
var Text Serializer = textSerializer{}

type gzipEncoder struct{ writers sync.Pool }

func (g *gzipEncoder) Encode(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(data)/2 + 64)
	w, _ := g.writers.Get().(*gzip.Writer)
	if w == nil {
		w = gzip.NewWriter(&buf)
	} else {
		w.Reset(&buf)
	}
	defer g.writers.Put(w)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *gzipEncoder) Decode(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// Gzip encodes bodies with gzip (RFC 1952).
var Gzip Encoder = &gzipEncoder{}

type deflateEncoder struct{ writers sync.Pool }

func (z *deflateEncoder) Encode(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(data)/2 + 64)
	w, _ := z.writers.Get().(*zlib.Writer)
	if w == nil {
		w = zlib.NewWriter(&buf)
	} else {
		w.Reset(&buf)
	}
	defer z.writers.Put(w)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (z *deflateEncoder) Decode(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// Deflate encodes bodies with zlib-wrapped deflate (RFC 1950), which is
// what HTTP's "deflate" content encoding means.
var Deflate Encoder = &deflateEncoder{}
