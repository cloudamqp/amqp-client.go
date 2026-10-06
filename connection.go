package amqp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultFrameMax   = 131072
	defaultChannelMax = 2047
	// defaultConnectTimeout bounds the Client's reconnect attempts.
	defaultConnectTimeout = 30 * time.Second
)

// closeTimeout is how long Close waits for the broker's close-ok.
const closeTimeout = 10 * time.Second

// Config holds the optional settings for a connection. Settings in the URI
// query string are used for the fields left unset here.
type Config struct {
	// ConnectionName is shown in the broker's management UI. Defaults to
	// the program name.
	ConnectionName string
	// Heartbeat is the requested heartbeat interval. Zero uses the broker's
	// suggestion, a negative value disables heartbeats.
	Heartbeat time.Duration
	// FrameMax is the largest frame size the client accepts, defaults to
	// 131072. The smaller of the client's and broker's values is used.
	FrameMax uint32
	// ChannelMax is the maximum number of open channels, defaults to 2047.
	// The smaller of the client's and broker's values is used.
	ChannelMax uint16
	// ConnectTimeout bounds the TCP connect, TLS handshake and AMQP
	// handshake, in addition to the context passed to Dial. Defaults to
	// the URI's connection_timeout, and for the Client's reconnects to 30s.
	ConnectTimeout time.Duration
	// TLSConfig is used for amqps:// URIs. ServerName defaults to the host.
	TLSConfig *tls.Config
	// Dial overrides how the network connection is established, e.g. to
	// use a proxy or a UNIX socket. The connection is wrapped in TLS
	// afterwards if the URI scheme is amqps.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// ClientProperties are added to the client properties sent to the broker.
	ClientProperties Table
	// Codecs used by [Delivery.Decode] (and the [Client] when publishing).
	Codecs *Codecs
	// Logger receives warnings, such as frames for unknown channels.
	// Defaults to slog.Default().
	Logger *slog.Logger
	// OnBlocked is called when the broker blocks publishing on the
	// connection, e.g. because of a memory or disk alarm. It's called from
	// the connection's read loop and must not block.
	OnBlocked func(reason string)
	// OnUnblocked is called when the broker lifts the block.
	OnUnblocked func()
}

// Connection is an AMQP connection. It's safe for concurrent use.
//
// Frames are appended to a write buffer and written to the socket by a
// background goroutine, which coalesces frames written concurrently, or in
// quick succession, into fewer syscalls (see writer.go).
type Connection struct {
	conn net.Conn
	rd   *deadlineReader
	br   *bufio.Reader
	// pendingDiscard is the size of the last frame returned by readFrame,
	// which is still in br's buffer.
	pendingDiscard int
	readBuf        []byte // for frames larger than br's buffer

	writer

	uri         URI
	cfg         Config
	logger      *slog.Logger
	frameMax    uint32
	channelMax  uint16
	heartbeat   time.Duration
	serverProps Table

	chMu     sync.Mutex
	channels []atomic.Pointer[Channel] // indexed by channel id
	nextID   uint16

	blockMu   sync.Mutex
	blocked   atomic.Bool
	unblocked chan struct{} // closed when the block is lifted

	// update-secret requests are numbered, replies arrive in order
	secretMu   sync.Mutex
	secretSent uint64
	secretOk   uint64
	secretCh   chan struct{} // closed when secretOk increases
	closing    atomic.Bool
	cause      atomic.Pointer[error] // why the connection is being closed

	closeTimeout time.Duration // how long Close waits for close-ok, set in tests

	done chan struct{}
	err  error // set before done is closed
}

// Dial connects to the broker at uri, e.g. "amqp://guest:guest@localhost/vhost".
// cfg may be nil. The context bounds the TCP connect, the TLS handshake and
// the AMQP handshake.
func Dial(ctx context.Context, uri string, cfg *Config) (*Connection, error) {
	u, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	return DialURI(ctx, u, cfg)
}

// DialURI is like [Dial] but takes a parsed URI.
func DialURI(ctx context.Context, u URI, cfg *Config) (*Connection, error) {
	var c Config
	if cfg != nil {
		c = *cfg
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = u.Heartbeat
	}
	if c.FrameMax == 0 {
		c.FrameMax = u.FrameMax
	}
	if c.ChannelMax == 0 {
		c.ChannelMax = u.ChannelMax
	}
	if c.ConnectionName == "" {
		c.ConnectionName = u.ConnectionName
	}
	if c.ConnectionName == "" {
		c.ConnectionName = filepath.Base(os.Args[0])
	}
	if c.FrameMax == 0 {
		c.FrameMax = defaultFrameMax
	}
	c.FrameMax = max(c.FrameMax, minFrameMax)
	if c.ChannelMax == 0 {
		c.ChannelMax = defaultChannelMax
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = u.ConnectTimeout
	}
	if c.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.ConnectTimeout)
		defer cancel()
	}

	dial := c.Dial
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	nc, err := dial(ctx, "tcp", u.Addr())
	if err != nil {
		return nil, fmt.Errorf("amqp: dial: %w", err)
	}
	if u.TLS {
		tc := c.TLSConfig.Clone()
		if tc == nil {
			tc = &tls.Config{}
		}
		if tc.ServerName == "" {
			tc.ServerName = u.Host
			if u.ServerName != "" {
				tc.ServerName = u.ServerName
			}
		}
		if u.InsecureTLS {
			tc.InsecureSkipVerify = true
		}
		tlsConn := tls.Client(nc, tc)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, fmt.Errorf("amqp: TLS handshake: %w", err)
		}
		nc = tlsConn
	}
	conn, err := open(ctx, nc, u, c)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return conn, nil
}

type deadlineReader struct {
	conn    net.Conn
	timeout time.Duration
}

func (r *deadlineReader) Read(p []byte) (int, error) {
	if r.timeout > 0 {
		if err := r.conn.SetReadDeadline(time.Now().Add(r.timeout)); err != nil {
			return 0, err
		}
	}
	return r.conn.Read(p)
}

func open(ctx context.Context, nc net.Conn, u URI, cfg Config) (*Connection, error) {
	rd := &deadlineReader{conn: nc}
	c := &Connection{
		conn:         nc,
		rd:           rd,
		br:           bufio.NewReaderSize(rd, int(cfg.FrameMax)+frameOverhead),
		uri:          u,
		cfg:          cfg,
		logger:       cfg.Logger,
		secretCh:     make(chan struct{}),
		closeTimeout: closeTimeout,
		done:         make(chan struct{}),
	}

	// Abort the handshake when the context is done.
	if deadline, ok := ctx.Deadline(); ok {
		nc.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { nc.SetDeadline(time.Unix(1, 0)) })
	err := c.handshake()
	if !stop() {
		return nil, fmt.Errorf("amqp: handshake: %w", context.Cause(ctx))
	}
	if err != nil {
		return nil, err
	}
	nc.SetDeadline(time.Time{})
	c.initWriter()

	if c.heartbeat > 0 {
		rd.timeout = 2 * c.heartbeat
		go c.heartbeatLoop()
	}
	go c.flushLoop()
	go c.readLoop()
	return c, nil
}

func (c *Connection) handshake() error {
	if _, err := c.conn.Write(protocolHeader); err != nil {
		return fmt.Errorf("amqp: handshake: %w", err)
	}

	cm, d, err := c.readConnectionMethod()
	if err != nil {
		return err
	}
	if cm != connectionStart {
		return unexpectedMethod(cm)
	}
	d.u8() // version major
	d.u8() // version minor
	c.serverProps = d.table()
	mechanisms := d.longStr()
	if d.err != nil {
		return d.err
	}

	mechanism := "PLAIN"
	if c.uri.AuthMechanism != "" {
		mechanism = c.uri.AuthMechanism
	}
	if !containsWord(mechanisms, mechanism) {
		return fmt.Errorf("amqp: broker doesn't support the %s auth mechanism, only %q", mechanism, mechanisms)
	}
	var response string
	switch mechanism {
	case "PLAIN":
		response = "\x00" + c.uri.Username + "\x00" + c.uri.Password
	case "EXTERNAL":
	default:
		return fmt.Errorf("amqp: unsupported auth mechanism %s", mechanism)
	}

	props := Table{
		"product":          "amqp-client.go",
		"version":          Version,
		"platform":         "Go",
		"platform_version": runtime.Version(),
		"information":      "https://github.com/cloudamqp/amqp-client.go",
		"connection_name":  c.cfg.ConnectionName,
		"capabilities": Table{
			"publisher_confirms":           true,
			"exchange_exchange_bindings":   true,
			"basic.nack":                   true,
			"per_consumer_qos":             true,
			"authentication_failure_close": true,
			"consumer_cancel_notify":       true,
			"connection.blocked":           true,
			"update-secret":                true,
		},
	}
	for k, v := range c.cfg.ClientProperties {
		props[k] = v
	}
	err = c.writeHandshake(connectionStartOk, func(b []byte) ([]byte, error) {
		b, err := appendTable(b, props)
		if err != nil {
			return b, err
		}
		b = appendShortStr(b, mechanism)
		b = appendLongStr(b, response)
		return appendShortStr(b, "en_US"), nil
	})
	if err != nil {
		return err
	}

	if cm, d, err = c.readConnectionMethod(); err != nil {
		return err
	}
	switch cm {
	case connectionTune:
	case connectionSecure:
		return errors.New("amqp: connection.secure isn't supported")
	default:
		return unexpectedMethod(cm)
	}
	channelMax := d.u16()
	frameMax := d.u32()
	heartbeat := time.Duration(d.u16()) * time.Second
	if d.err != nil {
		return d.err
	}
	if channelMax == 0 || channelMax > c.cfg.ChannelMax {
		channelMax = c.cfg.ChannelMax
	}
	if frameMax == 0 || frameMax > c.cfg.FrameMax {
		frameMax = c.cfg.FrameMax
	}
	switch {
	case c.cfg.Heartbeat < 0:
		heartbeat = 0
	case c.cfg.Heartbeat > 0:
		heartbeat = c.cfg.Heartbeat.Truncate(time.Second)
		heartbeat = min(max(heartbeat, time.Second), 65535*time.Second)
	}
	c.channelMax, c.frameMax, c.heartbeat = channelMax, frameMax, heartbeat
	c.channels = make([]atomic.Pointer[Channel], int(channelMax)+1)
	c.nextID = 1

	err = c.writeHandshake(connectionTuneOk, func(b []byte) ([]byte, error) {
		b = be.AppendUint16(b, channelMax)
		b = be.AppendUint32(b, frameMax)
		return be.AppendUint16(b, uint16(heartbeat/time.Second)), nil
	})
	if err != nil {
		return err
	}
	if err := checkShortStr("vhost", c.uri.Vhost); err != nil {
		return err
	}
	err = c.writeHandshake(connectionOpen, func(b []byte) ([]byte, error) {
		b = appendShortStr(b, c.uri.Vhost)
		return append(b, 0, 0), nil // reserved shortstr and bit
	})
	if err != nil {
		return err
	}
	if cm, _, err = c.readConnectionMethod(); err != nil {
		return err
	}
	if cm != connectionOpenOk {
		return unexpectedMethod(cm)
	}
	return nil
}

func containsWord(list, word string) bool {
	for len(list) > 0 {
		i := 0
		for i < len(list) && list[i] != ' ' {
			i++
		}
		if list[:i] == word {
			return true
		}
		if i == len(list) {
			break
		}
		list = list[i+1:]
	}
	return false
}

// readConnectionMethod reads the next method frame during the handshake.
// A connection.close from the broker (e.g. bad credentials) becomes an
// *Error.
func (c *Connection) readConnectionMethod() (uint32, decoder, error) {
	for {
		typ, _, payload, err := c.readFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, decoder{}, fmt.Errorf("amqp: handshake: connection closed by the broker: %w", err)
			}
			return 0, decoder{}, fmt.Errorf("amqp: handshake: %w", err)
		}
		if typ == frameHeartbeat {
			continue
		}
		if typ != frameMethod || len(payload) < 4 {
			return 0, decoder{}, errMalformed
		}
		cm := be.Uint32(payload)
		d := decoder{b: payload[4:]}
		if cm == connectionClose {
			e := decodeClose(&d, true)
			c.writeHandshake(connectionCloseOk, nil)
			return 0, d, e
		}
		return cm, d, nil
	}
}

func (c *Connection) writeHandshake(cm uint32, fn func([]byte) ([]byte, error)) error {
	b, start := beginMethod(nil, 0, cm)
	var err error
	if fn != nil {
		if b, err = fn(b); err != nil {
			return err
		}
	}
	b = endFrame(b, start)
	if _, err = c.conn.Write(b); err != nil {
		return fmt.Errorf("amqp: handshake: %w", err)
	}
	return nil
}

func decodeClose(d *decoder, conn bool) *Error {
	e := &Error{Connection: conn, Server: true}
	e.Code = d.u16()
	e.Reason = d.shortStr()
	e.ClassID = d.u16()
	e.MethodID = d.u16()
	return e
}

// readFrame returns the next frame. The payload is only valid until the
// next call.
func (c *Connection) readFrame() (typ byte, channel uint16, payload []byte, err error) {
	if c.pendingDiscard > 0 {
		c.br.Discard(c.pendingDiscard)
		c.pendingDiscard = 0
	}
	hdr, err := c.br.Peek(7)
	if err != nil {
		return 0, 0, nil, err
	}
	typ = hdr[0]
	channel = be.Uint16(hdr[1:3])
	size := be.Uint32(hdr[3:7])
	frameMax := c.frameMax
	if frameMax == 0 { // still in the handshake
		switch typ {
		case frameMethod, frameHeartbeat:
		case 'A':
			// The broker replies with its protocol header if it doesn't
			// support the client's version
			if h, err := c.br.Peek(8); err == nil && string(h[:4]) == "AMQP" {
				return 0, 0, nil, fmt.Errorf("amqp: the broker doesn't support AMQP 0-9-1, it supports %d-%d-%d", h[5], h[6], h[7])
			}
			fallthrough
		default:
			return 0, 0, nil, fmt.Errorf("amqp: not an AMQP 0-9-1 broker, it sent %q", hdr)
		}
		frameMax = max(c.cfg.FrameMax, minFrameMax)
	}
	if size > frameMax {
		return 0, 0, nil, &Error{Code: FrameError, Connection: true,
			Reason: fmt.Sprintf("frame size %d exceeds frame max %d", size, frameMax)}
	}
	n := 7 + int(size) + 1
	var frame []byte
	if n <= c.br.Size() {
		if frame, err = c.br.Peek(n); err != nil {
			return 0, 0, nil, err
		}
		c.pendingDiscard = n
	} else {
		if cap(c.readBuf) < n {
			c.readBuf = make([]byte, n)
		}
		frame = c.readBuf[:n]
		if _, err = io.ReadFull(c.br, frame); err != nil {
			return 0, 0, nil, err
		}
	}
	if frame[n-1] != frameEnd {
		return 0, 0, nil, &Error{Code: FrameError, Reason: "missing frame end octet", Connection: true}
	}
	return typ, channel, frame[7 : n-1], nil
}

func (c *Connection) readLoop() {
	var err error
	for {
		var typ byte
		var channel uint16
		var payload []byte
		if typ, channel, payload, err = c.readFrame(); err != nil {
			break
		}
		if channel == 0 {
			if typ == frameHeartbeat {
				continue
			}
			if typ != frameMethod || len(payload) < 4 {
				err = &Error{Code: CommandInvalid, Reason: "unexpected frame on channel 0", Connection: true}
				break
			}
			var stop bool
			if stop, err = c.handleConnectionMethod(be.Uint32(payload), decoder{b: payload[4:]}); stop {
				break
			}
			continue
		}
		ch := c.channel(channel)
		if ch == nil {
			c.logger.Debug("amqp: frame for unknown channel", "channel", channel, "type", typ)
			continue
		}
		if err = ch.handleFrame(typ, payload); err != nil {
			break
		}
	}
	var e *Error
	if c.cause.Load() == nil && errors.As(err, &e) {
		if !e.Server {
			// A protocol error detected by the client, tell the broker
			c.writeMethod(context.Background(), 0, connectionClose, true, func(b []byte) ([]byte, error) {
				b = be.AppendUint16(b, e.Code)
				b = appendShortStr(b, truncate(e.Reason, 255))
				b = be.AppendUint16(b, e.ClassID)
				return be.AppendUint16(b, e.MethodID), nil
			})
		}
		// Send connection.close or close-ok before closing the socket
		c.waitFlushed(time.Second)
	}
	if cause := c.cause.Load(); cause != nil {
		err = *cause
	} else if _, ok := err.(*Error); !ok && err != nil {
		err = &closedError{err}
	}
	c.teardown(err)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// handleConnectionMethod processes a method on channel 0. stop is true when
// the read loop should exit, err is then the reason.
func (c *Connection) handleConnectionMethod(cm uint32, d decoder) (stop bool, err error) {
	switch cm {
	case connectionClose:
		e := decodeClose(&d, true)
		c.writeMethod(context.Background(), 0, connectionCloseOk, true, nil)
		return true, e
	case connectionCloseOk:
		if c.closing.Load() {
			return true, ErrClosed
		}
		return true, unexpectedMethod(cm)
	case connectionBlocked:
		reason := d.shortStr()
		c.blockMu.Lock()
		if !c.blocked.Load() {
			c.unblocked = make(chan struct{})
			c.blocked.Store(true)
		}
		c.blockMu.Unlock()
		if c.cfg.OnBlocked != nil {
			c.cfg.OnBlocked(reason)
		}
	case connectionUnblocked:
		c.blockMu.Lock()
		if c.blocked.Load() {
			c.blocked.Store(false)
			close(c.unblocked)
		}
		c.blockMu.Unlock()
		if c.cfg.OnUnblocked != nil {
			c.cfg.OnUnblocked()
		}
	case connectionUpdateSecretOk:
		c.secretMu.Lock()
		c.secretOk++
		close(c.secretCh)
		c.secretCh = make(chan struct{})
		c.secretMu.Unlock()
	default:
		return true, unexpectedMethod(cm)
	}
	return false, nil
}

func (c *Connection) teardown(err error) {
	// Close the socket before taking wmu, it unblocks a writer stuck in a
	// write to a peer that stopped reading
	c.conn.Close()
	c.wmu.Lock()
	if c.werr == nil {
		c.werr = err
	}
	c.wmu.Unlock()
	c.chMu.Lock()
	c.err = err
	close(c.done)
	c.chMu.Unlock()
	for i := range c.channels {
		if ch := c.channels[i].Swap(nil); ch != nil {
			ch.shutdown(err)
		}
	}
}

// abort closes the socket, the read loop then tears down the connection
// with err as the reason.
func (c *Connection) abort(err error) {
	c.cause.CompareAndSwap(nil, &err)
	c.conn.Close()
}

func (c *Connection) channel(id uint16) *Channel {
	if int(id) >= len(c.channels) {
		return nil
	}
	return c.channels[id].Load()
}

func (c *Connection) removeChannel(ch *Channel) {
	if int(ch.id) < len(c.channels) {
		c.channels[ch.id].CompareAndSwap(ch, nil)
	}
}

// Channel opens a new channel.
func (c *Connection) Channel(ctx context.Context) (*Channel, error) {
	ch, err := c.allocChannel()
	if err != nil {
		return nil, err
	}
	if err := ch.open(ctx); err != nil {
		return nil, err
	}
	return ch, nil
}

func (c *Connection) allocChannel() (*Channel, error) {
	c.chMu.Lock()
	defer c.chMu.Unlock()
	select {
	case <-c.done:
		return nil, c.err
	default:
	}
	for range c.channelMax {
		id := c.nextID
		if c.nextID++; c.nextID > c.channelMax || c.nextID == 0 {
			c.nextID = 1
		}
		if c.channels[id].Load() == nil {
			ch := newChannel(c, id)
			c.channels[id].Store(ch)
			return ch, nil
		}
	}
	return nil, ErrChannelMax
}

// Close closes the connection gracefully, waiting for the broker to
// confirm. All channels and consumers are closed.
func (c *Connection) Close() error {
	return c.CloseReason("")
}

// CloseReason is like Close, the reason might be logged by the broker.
func (c *Connection) CloseReason(reason string) error {
	if !c.closing.CompareAndSwap(false, true) {
		<-c.done
		return nil
	}
	// Don't block forever on a peer that doesn't read
	c.conn.SetWriteDeadline(time.Now().Add(c.closeTimeout))
	err := c.writeMethod(context.Background(), 0, connectionClose, true, func(b []byte) ([]byte, error) {
		b = be.AppendUint16(b, ReplySuccess)
		b = appendShortStr(b, truncate(reason, 255))
		return append(b, 0, 0, 0, 0), nil
	})
	if err != nil {
		c.abort(ErrClosed)
		<-c.done
		return nil
	}
	select {
	case <-c.done:
	case <-time.After(c.closeTimeout):
		c.abort(ErrClosed)
		<-c.done
	}
	if errors.Is(c.err, ErrClosed) {
		return nil
	}
	return c.err
}

// Done returns a channel that's closed when the connection is closed, for
// any reason. Err then returns the reason.
func (c *Connection) Done() <-chan struct{} { return c.done }

// Err returns why the connection was closed: ErrClosed if closed by the
// client, an *Error if closed by the broker, or a network error (that
// matches ErrClosed). It returns nil while the connection is open.
func (c *Connection) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}

// IsClosed reports whether the connection is closed.
func (c *Connection) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// Blocked reports whether the broker has blocked publishing on the connection.
func (c *Connection) Blocked() bool { return c.blocked.Load() }

// waitUnblocked waits until the broker unblocks the connection.
func (c *Connection) waitUnblocked(ctx context.Context) error {
	if !c.blocked.Load() {
		return nil
	}
	c.blockMu.Lock()
	unblocked := c.unblocked
	blocked := c.blocked.Load()
	c.blockMu.Unlock()
	if !blocked {
		return nil
	}
	select {
	case <-unblocked:
		return nil
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UpdateSecret updates the credentials of the connection, e.g. a renewed
// OAuth 2 token, without reconnecting.
func (c *Connection) UpdateSecret(ctx context.Context, secret, reason string) error {
	if err := checkShortStr("reason", reason); err != nil {
		return err
	}
	if err := c.waitSpace(ctx); err != nil {
		return err
	}
	// Number the request while writing it, the broker replies in order
	c.secretMu.Lock()
	err := c.writeMethod(ctx, 0, connectionUpdateSecret, true, func(b []byte) ([]byte, error) {
		b = appendLongStr(b, secret)
		return appendShortStr(b, reason), nil
	})
	if err != nil {
		c.secretMu.Unlock()
		return err
	}
	c.secretSent++
	seq := c.secretSent
	c.secretMu.Unlock()
	for {
		c.secretMu.Lock()
		ok, changed := c.secretOk, c.secretCh
		c.secretMu.Unlock()
		if ok >= seq {
			return nil
		}
		select {
		case <-changed:
		case <-c.done:
			return c.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ServerProperties returns the properties the broker sent in the handshake,
// such as "product", "version" and "capabilities".
func (c *Connection) ServerProperties() Table { return c.serverProps }

// FrameMax returns the negotiated maximum frame size.
func (c *Connection) FrameMax() uint32 { return c.frameMax }

// ChannelMax returns the negotiated maximum channel id.
func (c *Connection) ChannelMax() uint16 { return c.channelMax }

// Heartbeat returns the negotiated heartbeat interval, 0 if disabled.
func (c *Connection) Heartbeat() time.Duration { return c.heartbeat }

// LocalAddr returns the local network address.
func (c *Connection) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// RemoteAddr returns the broker's network address.
func (c *Connection) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }
