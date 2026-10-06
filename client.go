package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ClientOptions are the options for [NewClient].
type ClientOptions struct {
	// Config is used for every connection the client makes.
	Config

	// ReconnectInterval is the delay before the second reconnect attempt
	// (the first is immediate), doubled for each failed attempt up to
	// MaxReconnectInterval. Defaults to 1s and 30s.
	ReconnectInterval    time.Duration
	MaxReconnectInterval time.Duration
	// MaxRetries is the number of consecutive failed reconnect attempts
	// before the client gives up and closes. Zero retries forever.
	MaxRetries int

	// OnConnect is called after each successful connection, including the
	// first one, once topology and consumers are recovered. The client's
	// methods can be used in it, e.g. to declare topology that isn't
	// recovered automatically. An error from the first call makes NewClient
	// fail, later errors are logged.
	OnConnect func(ctx context.Context, c *Client) error
	// OnDisconnect is called when the connection is lost.
	OnDisconnect func(err error)
	// OnFailed is called when the client gives up reconnecting, see
	// MaxRetries.
	OnFailed func(err error)
	// OnReturn is called with messages published with Mandatory that
	// couldn't be routed. It must not block.
	OnReturn func(*Return)

	// DefaultContentType and DefaultContentEncoding are used when a
	// published message doesn't set them.
	DefaultContentType     string
	DefaultContentEncoding string
}

// Client is the high-level API. It keeps a connection to the broker,
// reconnecting when it's lost, and recovers the exchanges, queues,
// bindings and subscriptions declared through it. Messages are published
// as persistent and confirmed by the broker by default.
//
// Operations wait (bounded by their context) while the client is
// reconnecting. A Client is safe for concurrent use.
type Client struct {
	uri  URI
	opts ClientOptions
	log  *slog.Logger

	ctx    context.Context // cancelled when the client is closed
	cancel context.CancelFunc

	mu      sync.Mutex
	conn    *Connection   // nil while disconnected
	changed chan struct{} // closed and replaced when conn or err changes
	err     error         // set when the client is closed or gave up

	ops *lazyChannel // declarations, bindings, gets
	pub *lazyChannel // publishing, in confirm mode

	topoMu     sync.Mutex
	exchanges  map[string]*Exchange
	queues     []*Queue
	qbindings  []queueBinding
	xbindings  []exchangeBinding
	subs       map[*Subscription]struct{}
	rpcClients map[*RPCClient]struct{}

	rpcMu     sync.Mutex
	rpcClient *RPCClient // shared by RPCCall

	closeOnce sync.Once
}

type queueBinding struct {
	q          *Queue
	exchange   string
	routingKey string
	args       Table
}

type exchangeBinding struct {
	destination, source string
	routingKey          string
	args                Table
}

// NewClient connects to the broker at url and returns a client that keeps
// the connection alive. It fails if the first connection attempt fails.
// opts may be nil.
func NewClient(ctx context.Context, url string, opts *ClientOptions) (*Client, error) {
	u, err := ParseURI(url)
	if err != nil {
		return nil, err
	}
	var o ClientOptions
	if opts != nil {
		o = *opts
	}
	if o.ReconnectInterval <= 0 {
		o.ReconnectInterval = time.Second
	}
	if o.MaxReconnectInterval <= 0 {
		o.MaxReconnectInterval = 30 * time.Second
	}
	o.MaxReconnectInterval = max(o.MaxReconnectInterval, o.ReconnectInterval)
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	c := &Client{
		uri:        u,
		opts:       o,
		log:        o.Logger,
		changed:    make(chan struct{}),
		exchanges:  map[string]*Exchange{},
		subs:       map[*Subscription]struct{}{},
		rpcClients: map[*RPCClient]struct{}{},
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.ops = &lazyChannel{c: c}
	c.pub = &lazyChannel{c: c, confirm: true}

	conn, err := DialURI(ctx, u, &o.Config)
	if err != nil {
		c.cancel()
		return nil, err
	}
	c.setConn(conn) // the client can't be closed yet
	if o.OnConnect != nil {
		if err := o.OnConnect(ctx, c); err != nil {
			c.Close()
			return nil, err
		}
	}
	go c.supervise(conn)
	return c, nil
}

// setConn publishes a new connection, it returns false (and closes the
// connection) if the client was closed in the meantime.
func (c *Client) setConn(conn *Connection) bool {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		conn.Close()
		return false
	}
	c.conn = conn
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
	return true
}

// connection returns the current connection, waiting for one while the
// client is reconnecting.
func (c *Client) connection(ctx context.Context) (*Connection, error) {
	for {
		c.mu.Lock()
		conn, changed, err := c.conn, c.changed, c.err
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Connection returns the current connection, waiting for the client to
// reconnect if necessary. Channels opened on it aren't recovered.
func (c *Client) Connection(ctx context.Context) (*Connection, error) {
	return c.connection(ctx)
}

func (c *Client) supervise(conn *Connection) {
	for {
		select {
		case <-conn.Done():
		case <-c.ctx.Done():
			return
		}
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
			close(c.changed)
			c.changed = make(chan struct{})
		}
		c.mu.Unlock()
		if c.ctx.Err() != nil {
			return
		}
		err := conn.Err()
		c.log.Warn("amqp: connection lost", "error", err)
		if c.opts.OnDisconnect != nil {
			c.opts.OnDisconnect(err)
		}
		if conn = c.reconnect(); conn == nil {
			return
		}
	}
}

func (c *Client) reconnect() *Connection {
	interval := c.opts.ReconnectInterval
	for attempt := 1; ; attempt++ {
		conn, err := DialURI(c.ctx, c.uri, &c.opts.Config)
		if err == nil {
			if err = c.recoverTopology(conn); err != nil {
				conn.Close()
			}
		}
		if err == nil {
			if !c.setConn(conn) {
				return nil
			}
			c.log.Info("amqp: reconnected", "attempts", attempt)
			if c.opts.OnConnect != nil {
				if err := c.opts.OnConnect(c.ctx, c); err != nil {
					c.log.Error("amqp: OnConnect failed", "error", err)
				}
			}
			return conn
		}
		if c.ctx.Err() != nil {
			return nil
		}
		if c.opts.MaxRetries > 0 && attempt >= c.opts.MaxRetries {
			c.log.Error("amqp: gave up reconnecting", "attempts", attempt, "error", err)
			c.shutdown(fmt.Errorf("amqp: gave up reconnecting after %d attempts: %w", attempt, err))
			if c.opts.OnFailed != nil {
				c.opts.OnFailed(err)
			}
			return nil
		}
		c.log.Warn("amqp: reconnect failed", "attempt", attempt, "retry_in", interval, "error", err)
		select {
		case <-time.After(interval):
		case <-c.ctx.Done():
			return nil
		}
		interval = min(interval*2, c.opts.MaxReconnectInterval)
	}
}

// recoverTopology redeclares exchanges, queues and bindings on a new
// connection. Subscriptions recover themselves once the connection is
// published. Declarations the broker rejects are logged and skipped.
func (c *Client) recoverTopology(conn *Connection) error {
	ctx, cancel := context.WithTimeout(c.ctx, time.Minute)
	defer cancel()
	ch, err := conn.Channel(ctx)
	if err != nil {
		return err
	}
	defer func() { ch.Close() }()
	// run a declaration, reopening the channel if the broker closes it
	run := func(what string, fn func(ch *Channel) error) error {
		err := fn(ch)
		if err == nil {
			return nil
		}
		var e *Error
		if !errors.As(err, &e) || e.Connection {
			return err
		}
		c.log.Error("amqp: failed to recover "+what, "error", err)
		next, err := conn.Channel(ctx)
		if err != nil {
			return err
		}
		ch = next
		return nil
	}

	c.topoMu.Lock()
	exchanges := make([]*Exchange, 0, len(c.exchanges))
	for _, x := range c.exchanges {
		exchanges = append(exchanges, x)
	}
	queues := append([]*Queue(nil), c.queues...)
	qbindings := append([]queueBinding(nil), c.qbindings...)
	xbindings := append([]exchangeBinding(nil), c.xbindings...)
	c.topoMu.Unlock()

	for _, x := range exchanges {
		if err := run("exchange "+x.name, func(ch *Channel) error { return x.declare(ctx, ch) }); err != nil {
			return err
		}
	}
	for _, q := range queues {
		if err := run("queue "+q.Name(), func(ch *Channel) error { return q.declare(ctx, ch) }); err != nil {
			return err
		}
	}
	for _, b := range xbindings {
		err := run("exchange binding", func(ch *Channel) error {
			return ch.ExchangeBind(ctx, b.destination, b.source, b.routingKey, b.args)
		})
		if err != nil {
			return err
		}
	}
	for _, b := range qbindings {
		err := run("queue binding", func(ch *Channel) error {
			return ch.QueueBind(ctx, b.q.Name(), b.exchange, b.routingKey, b.args)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Close stops the client: subscriptions are stopped (waiting for running
// handlers to return, their context is cancelled), and the connection is
// closed. Unacknowledged messages are requeued by the broker. As Close
// waits for handlers it must not be called from one, do that in a new
// goroutine.
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	return nil
}

func (c *Client) shutdown(reason error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.err = reason
		conn := c.conn
		close(c.changed)
		c.changed = make(chan struct{})
		c.mu.Unlock()
		c.cancel()

		c.topoMu.Lock()
		subs := make([]*Subscription, 0, len(c.subs))
		for s := range c.subs {
			subs = append(subs, s)
		}
		rpcs := make([]*RPCClient, 0, len(c.rpcClients))
		for r := range c.rpcClients {
			rpcs = append(rpcs, r)
		}
		c.topoMu.Unlock()
		for _, s := range subs {
			s.stop(reason, false)
		}
		for _, r := range rpcs {
			r.Close()
		}
		if conn != nil {
			conn.Close()
		}
	})
}

// Done returns a channel that's closed when the client is closed or gave
// up reconnecting.
func (c *Client) Done() <-chan struct{} { return c.ctx.Done() }

// Err returns why the client stopped, nil while it's running.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// lazyChannel is a channel that's (re)opened on demand.
type lazyChannel struct {
	c       *Client
	confirm bool
	sem     chan struct{}
	once    sync.Once
	ch      atomic.Pointer[Channel]
}

func (l *lazyChannel) get(ctx context.Context) (*Channel, error) {
	if ch := l.ch.Load(); ch != nil && !ch.IsClosed() {
		return ch, nil
	}
	l.once.Do(func() { l.sem = make(chan struct{}, 1) })
	select {
	case l.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-l.sem }()
	if ch := l.ch.Load(); ch != nil && !ch.IsClosed() {
		return ch, nil
	}
	for {
		conn, err := l.c.connection(ctx)
		if err != nil {
			return nil, err
		}
		ch, err := conn.Channel(ctx)
		if err == nil && l.confirm {
			ch.OnReturn(l.c.onReturn)
			if err = ch.ConfirmSelect(ctx); err != nil {
				ch.Close()
			}
		}
		if err == nil {
			l.ch.Store(ch)
			return ch, nil
		}
		if !isConnectionLost(err) || ctx.Err() != nil {
			return nil, err
		}
	}
}

func (c *Client) onReturn(r *Return) {
	if c.opts.OnReturn != nil {
		c.opts.OnReturn(r)
		return
	}
	c.log.Warn("amqp: message returned", "exchange", r.Exchange, "routing_key", r.RoutingKey,
		"reply_code", r.ReplyCode, "reply_text", r.ReplyText)
}

// isConnectionLost reports whether err is caused by a lost connection, or
// a channel closed by another operation, as opposed to an exception from
// the broker caused by the operation itself. The operation can then be
// retried.
func isConnectionLost(err error) bool {
	var ce *closedError
	var de *discardedError
	return errors.As(err, &ce) || errors.As(err, &de)
}

// withChannel runs fn on the operations channel. If the connection is lost
// it's retried on the next connection, until ctx is done.
func (c *Client) withChannel(ctx context.Context, fn func(ch *Channel) error) error {
	for {
		ch, err := c.ops.get(ctx)
		if err != nil {
			return err
		}
		if err = fn(ch); err == nil || !isConnectionLost(err) || ctx.Err() != nil {
			return err
		}
	}
}

// Message is a message published with the high-level API.
type Message struct {
	Properties
	// Body is a []byte or string, which is sent as is, or a value that's
	// serialized according to ContentType by the client's codecs.
	// Bodies are also encoded according to ContentEncoding (e.g. gzip) if
	// there are codecs.
	Body any
	// Mandatory makes the broker return the message, see
	// [ClientOptions.OnReturn], if it can't be routed to a queue.
	Mandatory bool
}

func (c *Client) encode(msg *Message) (Publishing, error) {
	p := Publishing{Properties: msg.Properties, Mandatory: msg.Mandatory}
	if p.DeliveryMode == 0 {
		p.DeliveryMode = Persistent
	}
	if p.ContentType == "" {
		p.ContentType = c.opts.DefaultContentType
	}
	if p.ContentEncoding == "" {
		p.ContentEncoding = c.opts.DefaultContentEncoding
	}
	if c.opts.Codecs != nil {
		body, err := c.opts.Codecs.Marshal(msg.Body, &p.Properties)
		p.Body = body
		return p, err
	}
	switch b := msg.Body.(type) {
	case nil:
	case []byte:
		p.Body = b
	case string:
		p.Body = []byte(b)
	default:
		return p, fmt.Errorf("amqp: can't publish a %T body without ClientOptions.Codecs", msg.Body)
	}
	return p, nil
}

// Publish publishes a message to an exchange and waits for the broker to
// confirm it. Messages are persistent unless DeliveryMode is set to
// Transient. Publish from multiple goroutines for higher throughput, the
// confirms are pipelined.
//
// If the connection is lost before the message is confirmed an error is
// returned, the message may or may not have reached the broker.
func (c *Client) Publish(ctx context.Context, exchange, routingKey string, msg Message) error {
	p, err := c.encode(&msg)
	if err != nil {
		return err
	}
	for {
		ch, err := c.pub.get(ctx)
		if err != nil {
			return err
		}
		conf, err := ch.BasicPublishConfirm(ctx, exchange, routingKey, p)
		if err != nil {
			if errors.Is(err, ErrClosed) && ctx.Err() == nil {
				continue // the message wasn't sent, retry on a new channel
			}
			return err
		}
		return conf.Wait(ctx)
	}
}

// Queue declares a queue and returns a handle to it. The queue (and its
// bindings) are redeclared when the client reconnects.
//
// With nil options a named queue is declared durable, and a queue with an
// empty name is declared exclusive and auto-delete with a name generated by
// the broker (which changes on reconnect).
func (c *Client) Queue(ctx context.Context, name string, opts *QueueOptions) (*Queue, error) {
	if err := checkShortStr("queue name", name); err != nil {
		return nil, err
	}
	var o QueueOptions
	switch {
	case opts != nil:
		o = *opts
	case name == "":
		o = QueueOptions{Exclusive: true, AutoDelete: true}
	default:
		o = QueueOptions{Durable: true}
	}
	q := &Queue{c: c, opts: o, serverNamed: name == ""}
	q.name.Store(&name)
	err := c.withChannel(ctx, func(ch *Channel) error { return q.declare(ctx, ch) })
	if err != nil {
		return nil, err
	}
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	if !q.serverNamed {
		for _, old := range c.queues {
			if !old.serverNamed && old.Name() == name {
				return old, nil // keep handles stable for bindings
			}
		}
	}
	if !o.Passive {
		c.queues = append(c.queues, q)
	}
	return q, nil
}

// Exchange declares an exchange of the given kind ([Direct], [Fanout],
// [Topic], [Headers] or a plugin type) and returns a handle to it. With nil
// options the exchange is durable. Exchanges named "amq.*" are built in,
// they're only checked for existence. The exchange is redeclared when the
// client reconnects.
func (c *Client) Exchange(ctx context.Context, name, kind string, opts *ExchangeOptions) (*Exchange, error) {
	if name == "" {
		return c.DefaultExchange(), nil
	}
	o := ExchangeOptions{Durable: true}
	if opts != nil {
		o = *opts
	}
	if isBuiltinExchange(name) {
		o.Passive = true
	}
	x := &Exchange{c: c, name: name, kind: kind, opts: o}
	if err := c.withChannel(ctx, func(ch *Channel) error { return x.declare(ctx, ch) }); err != nil {
		return nil, err
	}
	if !o.Passive {
		c.topoMu.Lock()
		c.exchanges[name] = x
		c.topoMu.Unlock()
	}
	return x, nil
}

func isBuiltinExchange(name string) bool {
	return len(name) >= 4 && name[:4] == "amq."
}

// DirectExchange declares a durable direct exchange, use "amq.direct" for
// the built-in one.
func (c *Client) DirectExchange(ctx context.Context, name string) (*Exchange, error) {
	return c.Exchange(ctx, name, Direct, nil)
}

// FanoutExchange declares a durable fanout exchange, use "amq.fanout" for
// the built-in one.
func (c *Client) FanoutExchange(ctx context.Context, name string) (*Exchange, error) {
	return c.Exchange(ctx, name, Fanout, nil)
}

// TopicExchange declares a durable topic exchange, use "amq.topic" for the
// built-in one.
func (c *Client) TopicExchange(ctx context.Context, name string) (*Exchange, error) {
	return c.Exchange(ctx, name, Topic, nil)
}

// HeadersExchange declares a durable headers exchange, use "amq.headers"
// for the built-in one.
func (c *Client) HeadersExchange(ctx context.Context, name string) (*Exchange, error) {
	return c.Exchange(ctx, name, Headers, nil)
}

// DefaultExchange returns the default exchange, which routes messages to
// the queue named by the routing key.
func (c *Client) DefaultExchange() *Exchange {
	return &Exchange{c: c, kind: Direct}
}

func (c *Client) addQueueBinding(b queueBinding) {
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	for _, old := range c.qbindings {
		if old.q == b.q && old.exchange == b.exchange && old.routingKey == b.routingKey && sameArgs(old.args, b.args) {
			return
		}
	}
	c.qbindings = append(c.qbindings, b)
}

func (c *Client) removeQueueBinding(b queueBinding) {
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	bs := c.qbindings[:0]
	for _, old := range c.qbindings {
		if !(old.q == b.q && old.exchange == b.exchange && old.routingKey == b.routingKey && sameArgs(old.args, b.args)) {
			bs = append(bs, old)
		}
	}
	clear(c.qbindings[len(bs):])
	c.qbindings = bs
}

func (c *Client) addExchangeBinding(b exchangeBinding) {
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	for _, old := range c.xbindings {
		if old.destination == b.destination && old.source == b.source && old.routingKey == b.routingKey && sameArgs(old.args, b.args) {
			return
		}
	}
	c.xbindings = append(c.xbindings, b)
}

func (c *Client) removeExchangeBinding(b exchangeBinding) {
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	bs := c.xbindings[:0]
	for _, old := range c.xbindings {
		if !(old.destination == b.destination && old.source == b.source && old.routingKey == b.routingKey && sameArgs(old.args, b.args)) {
			bs = append(bs, old)
		}
	}
	clear(c.xbindings[len(bs):])
	c.xbindings = bs
}

func (c *Client) removeQueue(q *Queue) {
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	qs := c.queues[:0]
	for _, old := range c.queues {
		if old != q {
			qs = append(qs, old)
		}
	}
	clear(c.queues[len(qs):])
	c.queues = qs
	bs := c.qbindings[:0]
	for _, b := range c.qbindings {
		if b.q != q {
			bs = append(bs, b)
		}
	}
	clear(c.qbindings[len(bs):])
	c.qbindings = bs
}

func (c *Client) removeExchange(name string) {
	c.topoMu.Lock()
	defer c.topoMu.Unlock()
	delete(c.exchanges, name)
	bs := c.qbindings[:0]
	for _, b := range c.qbindings {
		if b.exchange != name {
			bs = append(bs, b)
		}
	}
	clear(c.qbindings[len(bs):])
	c.qbindings = bs
	xbs := c.xbindings[:0]
	for _, b := range c.xbindings {
		if b.source != name && b.destination != name {
			xbs = append(xbs, b)
		}
	}
	clear(c.xbindings[len(xbs):])
	c.xbindings = xbs
}

func sameArgs(a, b Table) bool {
	if len(a) != len(b) {
		return false
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
