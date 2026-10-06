package amqp

import "time"

// Delivery modes for [Properties.DeliveryMode].
const (
	Transient  uint8 = 1
	Persistent uint8 = 2
)

// Properties are the basic class content properties of a message. Zero
// values are not sent on the wire.
type Properties struct {
	ContentType     string    // MIME content type, e.g. "application/json"
	ContentEncoding string    // MIME content encoding, e.g. "gzip"
	Headers         Table     // Application headers
	DeliveryMode    uint8     // Transient (1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationID   string    // Correlate a reply with a request
	ReplyTo         string    // Address to reply to, usually a queue name
	Expiration      string    // Message TTL in milliseconds, as a string
	MessageID       string    // Application message identifier
	Timestamp       time.Time // Message timestamp, second precision
	Type            string    // Message type name
	UserID          string    // Creating user id, validated by the broker
	AppID           string    // Creating application id
}

const (
	flagContentType     = 0x8000
	flagContentEncoding = 0x4000
	flagHeaders         = 0x2000
	flagDeliveryMode    = 0x1000
	flagPriority        = 0x0800
	flagCorrelationID   = 0x0400
	flagReplyTo         = 0x0200
	flagExpiration      = 0x0100
	flagMessageID       = 0x0080
	flagTimestamp       = 0x0040
	flagType            = 0x0020
	flagUserID          = 0x0010
	flagAppID           = 0x0008
	flagClusterID       = 0x0004
	flagContinuation    = 0x0001
)

func (p *Properties) validate() error {
	for _, f := range [...]struct{ name, value string }{
		{"content type", p.ContentType},
		{"content encoding", p.ContentEncoding},
		{"correlation id", p.CorrelationID},
		{"reply to", p.ReplyTo},
		{"expiration", p.Expiration},
		{"message id", p.MessageID},
		{"type", p.Type},
		{"user id", p.UserID},
		{"app id", p.AppID},
	} {
		if err := checkShortStr(f.name, f.value); err != nil {
			return err
		}
	}
	return nil
}

// appendProperties appends the property flags and the property list. The
// caller has to validate the short string lengths first.
func appendProperties(b []byte, p *Properties) ([]byte, error) {
	var flags uint16
	if p.ContentType != "" {
		flags |= flagContentType
	}
	if p.ContentEncoding != "" {
		flags |= flagContentEncoding
	}
	if p.Headers != nil {
		flags |= flagHeaders
	}
	if p.DeliveryMode != 0 {
		flags |= flagDeliveryMode
	}
	if p.Priority != 0 {
		flags |= flagPriority
	}
	if p.CorrelationID != "" {
		flags |= flagCorrelationID
	}
	if p.ReplyTo != "" {
		flags |= flagReplyTo
	}
	if p.Expiration != "" {
		flags |= flagExpiration
	}
	if p.MessageID != "" {
		flags |= flagMessageID
	}
	if !p.Timestamp.IsZero() {
		flags |= flagTimestamp
	}
	if p.Type != "" {
		flags |= flagType
	}
	if p.UserID != "" {
		flags |= flagUserID
	}
	if p.AppID != "" {
		flags |= flagAppID
	}
	b = be.AppendUint16(b, flags)
	if flags == 0 {
		return b, nil
	}
	if flags&flagContentType != 0 {
		b = appendShortStr(b, p.ContentType)
	}
	if flags&flagContentEncoding != 0 {
		b = appendShortStr(b, p.ContentEncoding)
	}
	if flags&flagHeaders != 0 {
		var err error
		if b, err = appendTable(b, p.Headers); err != nil {
			return b, err
		}
	}
	if flags&flagDeliveryMode != 0 {
		b = append(b, p.DeliveryMode)
	}
	if flags&flagPriority != 0 {
		b = append(b, p.Priority)
	}
	if flags&flagCorrelationID != 0 {
		b = appendShortStr(b, p.CorrelationID)
	}
	if flags&flagReplyTo != 0 {
		b = appendShortStr(b, p.ReplyTo)
	}
	if flags&flagExpiration != 0 {
		b = appendShortStr(b, p.Expiration)
	}
	if flags&flagMessageID != 0 {
		b = appendShortStr(b, p.MessageID)
	}
	if flags&flagTimestamp != 0 {
		b = be.AppendUint64(b, uint64(p.Timestamp.Unix()))
	}
	if flags&flagType != 0 {
		b = appendShortStr(b, p.Type)
	}
	if flags&flagUserID != 0 {
		b = appendShortStr(b, p.UserID)
	}
	if flags&flagAppID != 0 {
		b = appendShortStr(b, p.AppID)
	}
	return b, nil
}

// propCache holds the last decoded values of properties that are usually
// the same for all messages, to avoid allocating new strings for them.
type propCache struct {
	contentType     string
	contentEncoding string
	appID           string
}

func cached(b []byte, last *string) string {
	if string(b) != *last {
		*last = string(b)
	}
	return *last
}

// properties decodes a property list. A headers table that can't be
// decoded is skipped and its error returned, the other properties are
// still decoded.
func (d *decoder) properties(p *Properties, cache *propCache) (headersErr error) {
	flags := d.u16()
	// Further flag words follow while the continuation bit is set. They're
	// for properties this client doesn't know, which come after the known
	// ones and are ignored.
	for f := flags; f&flagContinuation != 0 && d.err == nil; {
		f = d.u16()
	}
	if flags&^flagContinuation == 0 {
		return nil
	}
	if flags&flagContentType != 0 {
		p.ContentType = cached(d.shortStrBytes(), &cache.contentType)
	}
	if flags&flagContentEncoding != 0 {
		p.ContentEncoding = cached(d.shortStrBytes(), &cache.contentEncoding)
	}
	if flags&flagHeaders != 0 {
		p.Headers, headersErr = d.tableLenient()
	}
	if flags&flagDeliveryMode != 0 {
		p.DeliveryMode = d.u8()
	}
	if flags&flagPriority != 0 {
		p.Priority = d.u8()
	}
	if flags&flagCorrelationID != 0 {
		p.CorrelationID = d.shortStr()
	}
	if flags&flagReplyTo != 0 {
		p.ReplyTo = d.shortStr()
	}
	if flags&flagExpiration != 0 {
		p.Expiration = d.shortStr()
	}
	if flags&flagMessageID != 0 {
		p.MessageID = d.shortStr()
	}
	if flags&flagTimestamp != 0 {
		p.Timestamp = time.Unix(int64(d.u64()), 0)
	}
	if flags&flagType != 0 {
		p.Type = d.shortStr()
	}
	if flags&flagUserID != 0 {
		p.UserID = d.shortStr()
	}
	if flags&flagAppID != 0 {
		p.AppID = cached(d.shortStrBytes(), &cache.appID)
	}
	if flags&flagClusterID != 0 {
		d.shortStrBytes() // deprecated, ignored
	}
	return headersErr
}
