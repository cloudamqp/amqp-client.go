package amqp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// Channel is an AMQP channel, a lightweight virtual connection multiplexed
// over a [Connection]. It's safe for concurrent use: synchronous methods
// (declarations, bindings etc.) from multiple goroutines are pipelined, the
// broker answers them in order.
//
// A channel is closed by the broker if an operation fails, e.g. declaring a
// queue passively that doesn't exist. Its methods then return an [*Error],
// and a new channel has to be opened.
type Channel struct {
	conn *Connection
	id   uint16

	rpcMu   sync.Mutex
	waiters []*rpcWaiter // in the order requests were sent
	closing atomic.Bool  // channel.close sent, waiting for close-ok
	// wclosed is set when channel.close or close-ok is written, no frames
	// may be sent on the channel after that. Guarded by conn.wmu.
	wclosed bool
	done    chan struct{}
	err     error // set before done is closed
	once    sync.Once

	consMu    sync.Mutex
	consumers map[string]*Consumer

	// Owned by the connection's read loop
	pend           pending
	lastConsumer   *Consumer
	lastExchange   string
	lastRoutingKey string
	propCache      propCache

	confirmMode atomic.Bool
	publishSeq  atomic.Uint64 // incremented with conn.wmu held
	confMu      sync.Mutex
	unconfirmed []unconfirmed // ordered by tag, starting at uhead
	uhead       int
	confWaiters []confirmWaiter
	nacked      bool
	confErr     error // set if the channel closed with unconfirmed messages

	onReturn atomic.Pointer[func(*Return)]
	// settledUpTo is the highest delivery tag acked or nacked with
	// multiple, the deliveries up to it are settled.
	settledUpTo atomic.Uint64
}

type rpcWaiter struct {
	req      uint32 // the request method
	expect   uint32
	reply    chan rpcReply
	state    atomic.Int32 // waiting, replied or abandoned
	consumer *Consumer    // for basic.consume
	noAck    bool         // for basic.get
}

const (
	waiting int32 = iota
	replied
	abandoned
)

type rpcReply struct {
	cm       uint32
	name     string // queue name or consumer tag
	n1, n2   uint32 // message and consumer counts
	delivery *Delivery
	err      error
}

// deliver hands the reply to the waiter, unless it gave up waiting.
func (w *rpcWaiter) deliver(r rpcReply) bool {
	if w.state.CompareAndSwap(waiting, replied) {
		w.reply <- r
		return true
	}
	return false
}

// maxBodyPrealloc is the largest body buffer allocated up front.
const maxBodyPrealloc = 16 << 20

type pendingKind uint8

const (
	pendNone pendingKind = iota
	pendDeliver
	pendGet
	pendReturn
)

// pending is a message whose content frames are being received.
type pending struct {
	kind     pendingKind
	header   bool
	size     uint64
	body     []byte
	delivery *Delivery
	ret      *Return
	consumer *Consumer
}

type unconfirmed struct {
	tag  uint64
	conf *Confirmation
}

type confirmWaiter struct {
	target uint64
	done   chan struct{}
}

func newChannel(c *Connection, id uint16) *Channel {
	return &Channel{
		conn:      c,
		id:        id,
		done:      make(chan struct{}),
		consumers: make(map[string]*Consumer),
	}
}

// ID returns the channel id.
func (ch *Channel) ID() uint16 { return ch.id }

// Connection returns the connection the channel belongs to.
func (ch *Channel) Connection() *Connection { return ch.conn }

// Done returns a channel that's closed when the channel is closed.
func (ch *Channel) Done() <-chan struct{} { return ch.done }

// Err returns why the channel was closed, or nil if it's open. See
// [Connection.Err].
func (ch *Channel) Err() error {
	select {
	case <-ch.done:
		return ch.err
	default:
		return nil
	}
}

// IsClosed reports whether the channel is closed.
func (ch *Channel) IsClosed() bool {
	select {
	case <-ch.done:
		return true
	default:
		return false
	}
}

// OnReturn sets a function that's called with messages published with
// Mandatory that the broker couldn't route. It's called from the
// connection's read loop, so it must not block or call synchronous methods
// on the connection. The broker sends the return before the publish
// confirm, so when a confirmation arrives the return has been processed.
func (ch *Channel) OnReturn(fn func(*Return)) {
	if fn == nil {
		ch.onReturn.Store(nil)
		return
	}
	ch.onReturn.Store(&fn)
}

type callOpts struct {
	consumer *Consumer
	noAck    bool
	onWrite  func() // called with conn.wmu held, when the frame is appended
}

// call sends a synchronous method and waits for the reply.
func (ch *Channel) call(ctx context.Context, cm, expect uint32, o callOpts, fn func([]byte) ([]byte, error)) (rpcReply, error) {
	// Wait for buffer space first, the frame is then written without
	// waiting, as the read loop needs rpcMu
	if err := ch.conn.waitSpace(ctx); err != nil {
		return rpcReply{}, err
	}
	w := &rpcWaiter{req: cm, expect: expect, reply: make(chan rpcReply, 1), consumer: o.consumer, noAck: o.noAck}
	ch.rpcMu.Lock()
	select {
	case <-ch.done:
		ch.rpcMu.Unlock()
		return rpcReply{}, ch.closedErr()
	default:
	}
	if ch.closing.Load() && cm != channelClose {
		ch.rpcMu.Unlock()
		return rpcReply{}, ErrClosed
	}
	ch.waiters = append(ch.waiters, w)
	if err := ch.write(cm, o.onWrite, fn); err != nil {
		ch.waiters = ch.waiters[:len(ch.waiters)-1]
		ch.rpcMu.Unlock()
		return rpcReply{}, err
	}
	ch.rpcMu.Unlock()

	select {
	case r := <-w.reply:
		return r, r.err
	case <-ctx.Done():
		if w.state.CompareAndSwap(waiting, abandoned) {
			return rpcReply{}, ctx.Err()
		}
		r := <-w.reply
		return r, r.err
	}
}

// write writes a method frame without waiting for buffer space.
func (ch *Channel) write(cm uint32, onWrite func(), fn func([]byte) ([]byte, error)) error {
	c := ch.conn
	b, err := c.beginWrite(context.Background(), 0, true)
	if err != nil {
		return err
	}
	if ch.wclosed && cm != channelCloseOk {
		c.cancelWrite()
		return ErrClosed
	}
	if b, err = c.appendMethod(b, ch.id, cm, fn); err != nil {
		c.cancelWrite()
		return err
	}
	if onWrite != nil {
		onWrite()
	}
	c.endWrite(b)
	return nil
}

// send writes an asynchronous method (one without a reply).
func (ch *Channel) send(cm uint32, fn func([]byte) ([]byte, error)) error {
	select {
	case <-ch.done:
		return ch.err
	default:
	}
	return ch.write(cm, nil, fn)
}

func (ch *Channel) open(ctx context.Context) error {
	_, err := ch.call(ctx, channelOpen, channelOpenOk, callOpts{}, func(b []byte) ([]byte, error) {
		return append(b, 0), nil // reserved shortstr
	})
	if err != nil {
		select {
		case <-ch.done:
		default:
			// Gave up waiting for open-ok, close it in the background
			go ch.Close()
		}
	}
	return err
}

// Close closes the channel gracefully. Unacknowledged messages are
// requeued by the broker. It returns nil if the channel is already closed.
func (ch *Channel) Close() error {
	return ch.CloseReason(ReplySuccess, "")
}

// CloseReason closes the channel with a reply code and text, which the
// broker might log.
func (ch *Channel) CloseReason(code uint16, reason string) error {
	ch.rpcMu.Lock()
	if ch.IsClosed() || ch.closing.Load() {
		ch.rpcMu.Unlock()
		<-ch.done
		return nil
	}
	ch.closing.Store(true)
	ch.rpcMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	_, err := ch.call(ctx, channelClose, channelCloseOk, callOpts{
		onWrite: func() { ch.wclosed = true },
	}, func(b []byte) ([]byte, error) {
		b = be.AppendUint16(b, code)
		b = appendShortStr(b, truncate(reason, 255))
		return append(b, 0, 0, 0, 0), nil
	})
	if err == context.DeadlineExceeded {
		// The id stays reserved until the broker's close-ok arrives, as the
		// broker still has the channel open
		ch.shutdown(ErrClosed)
	}
	<-ch.done
	return nil
}

// shutdown marks the channel as closed and fails everything waiting on it.
// It doesn't free the channel id, see removeChannel.
func (ch *Channel) shutdown(err error) {
	ch.once.Do(func() {
		ch.rpcMu.Lock()
		ch.err = err
		close(ch.done)
		waiters := ch.waiters
		ch.waiters = nil
		ch.rpcMu.Unlock()
		var e *Error
		isChannelError := errors.As(err, &e) && !e.Connection
		for i, w := range waiters {
			if isChannelError && !(i == 0 && w.req == uint32(e.ClassID)<<16|uint32(e.MethodID)) {
				// The broker discarded the request because an earlier
				// operation closed the channel
				w.deliver(rpcReply{err: &discardedError{err}})
				continue
			}
			w.deliver(rpcReply{err: err})
		}

		ch.consMu.Lock()
		for tag, c := range ch.consumers {
			// Deliveries to no-ack consumers are already settled, keep them
			c.close(err, !c.noAck)
			delete(ch.consumers, tag)
		}
		ch.consMu.Unlock()

		ch.confMu.Lock()
		// The fate of unconfirmed messages is unknown. A broker exception
		// may have been caused by any message on the channel, so it's not
		// returned as is, it would look like each message's own failure.
		unconfErr := &unconfirmedError{err}
		if ch.uhead < len(ch.unconfirmed) {
			ch.confErr = unconfErr
		}
		for _, u := range ch.unconfirmed[ch.uhead:] {
			if u.conf != nil {
				u.conf.resolve(false, unconfErr)
			}
		}
		ch.unconfirmed, ch.uhead = nil, 0
		ch.confMu.Unlock()

	})
}

// closedErr is the error for requests on a closed channel. A broker
// exception is wrapped, as it was caused by another request.
func (ch *Channel) closedErr() error {
	var e *Error
	if errors.As(ch.err, &e) && !e.Connection {
		return &discardedError{ch.err}
	}
	return ch.err
}

func (ch *Channel) popWaiter() *rpcWaiter {
	ch.rpcMu.Lock()
	defer ch.rpcMu.Unlock()
	if len(ch.waiters) == 0 {
		return nil
	}
	w := ch.waiters[0]
	ch.waiters[0] = nil
	ch.waiters = ch.waiters[1:]
	return w
}

func (ch *Channel) frameError(reason string) error {
	return &Error{Code: UnexpectedFrame, Reason: fmt.Sprintf("%s on channel %d", reason, ch.id), Connection: true}
}

// handleFrame processes a frame for the channel, called from the
// connection's read loop. A returned error closes the connection.
func (ch *Channel) handleFrame(typ byte, payload []byte) error {
	// Frames are processed as usual while closing: the broker answers
	// requests and confirms publishes sent before channel.close, only
	// deliveries are dropped (see contentComplete).
	switch typ {
	case frameMethod:
		if len(payload) < 4 {
			return &Error{Code: FrameError, Reason: "method frame too short", Connection: true}
		}
		if ch.pend.kind != pendNone {
			return ch.frameError("method frame while expecting content")
		}
		d := decoder{b: payload[4:]}
		return ch.handleMethod(be.Uint32(payload), &d)
	case frameHeader:
		return ch.handleHeader(payload)
	case frameBody:
		return ch.handleBody(payload)
	}
	return &Error{Code: FrameError, Reason: fmt.Sprintf("unknown frame type %d", typ), Connection: true}
}

func (ch *Channel) lookupConsumer(tag []byte) *Consumer {
	if c := ch.lastConsumer; c != nil && c.tag == string(tag) {
		return c
	}
	ch.consMu.Lock()
	c := ch.consumers[string(tag)]
	ch.consMu.Unlock()
	if c != nil {
		ch.lastConsumer = c
	}
	return c
}

func (ch *Channel) removeConsumer(tag string) *Consumer {
	ch.consMu.Lock()
	c := ch.consumers[tag]
	delete(ch.consumers, tag)
	ch.consMu.Unlock()
	if ch.lastConsumer == c {
		ch.lastConsumer = nil
	}
	return c
}

func (ch *Channel) handleMethod(cm uint32, d *decoder) error {
	switch cm {
	case basicDeliver:
		tag := d.shortStrBytes()
		m := &Delivery{channel: ch}
		m.DeliveryTag = d.u64()
		m.Redelivered = d.u8()&1 != 0
		m.Exchange = cached(d.shortStrBytes(), &ch.lastExchange)
		m.RoutingKey = cached(d.shortStrBytes(), &ch.lastRoutingKey)
		if d.err != nil {
			return d.err
		}
		c := ch.lookupConsumer(tag)
		if c != nil {
			m.ConsumerTag = c.tag
			if c.noAck {
				m.acked.Store(true)
			}
		} else {
			m.ConsumerTag = string(tag)
		}
		ch.pend = pending{kind: pendDeliver, delivery: m, consumer: c}
		return nil
	case basicGetOk:
		m := &Delivery{channel: ch}
		m.DeliveryTag = d.u64()
		m.Redelivered = d.u8()&1 != 0
		m.Exchange = cached(d.shortStrBytes(), &ch.lastExchange)
		m.RoutingKey = cached(d.shortStrBytes(), &ch.lastRoutingKey)
		m.MessageCount = d.u32()
		if d.err != nil {
			return d.err
		}
		ch.pend = pending{kind: pendGet, delivery: m}
		return nil
	case basicReturn:
		r := &Return{}
		r.ReplyCode = d.u16()
		r.ReplyText = d.shortStr()
		r.Exchange = d.shortStr()
		r.RoutingKey = d.shortStr()
		if d.err != nil {
			return d.err
		}
		ch.pend = pending{kind: pendReturn, ret: r}
		return nil
	case basicAck:
		tag := d.u64()
		multiple := d.u8()&1 != 0
		ch.confirmed(tag, multiple, true)
		return d.err
	case basicNack:
		tag := d.u64()
		multiple := d.u8()&1 != 0
		ch.confirmed(tag, multiple, false)
		return d.err
	case basicCancel:
		tag := d.shortStr()
		noWait := d.u8()&1 != 0
		ch.consMu.Lock()
		c := ch.consumers[tag]
		ch.consMu.Unlock()
		if c != nil && !c.abandoned { // an abandoned one waits for its cancel-ok
			ch.removeConsumer(tag)
			c.close(ErrConsumerCancelledByServer, false)
		}
		if !noWait {
			return ignoreClosed(ch.send(basicCancelOk, func(b []byte) ([]byte, error) {
				return appendShortStr(b, tag), nil
			}))
		}
		return d.err
	case channelClose:
		e := decodeClose(d, false)
		ch.closing.Store(true)
		ch.write(channelCloseOk, func() { ch.wclosed = true }, nil)
		ch.shutdown(e)
		ch.conn.removeChannel(ch)
		return nil
	case channelCloseOk:
		// Fail requests the broker discarded while closing
		for w := ch.popWaiter(); w != nil; w = ch.popWaiter() {
			if w.expect == channelCloseOk {
				w.deliver(rpcReply{cm: cm})
				break
			}
			w.deliver(rpcReply{err: ErrClosed})
		}
		ch.shutdown(ErrClosed)
		ch.conn.removeChannel(ch)
		return nil
	case channelFlow:
		active := d.u8()&1 != 0
		return ignoreClosed(ch.send(channelFlowOk, func(b []byte) ([]byte, error) {
			return appendBits(b, active), nil
		}))
	}

	// The remaining methods are replies to synchronous requests
	r := rpcReply{cm: cm}
	switch cm {
	case queueDeclareOk:
		r.name = d.shortStr()
		r.n1 = d.u32()
		r.n2 = d.u32()
	case queuePurgeOk, queueDeleteOk:
		r.n1 = d.u32()
	case basicConsumeOk, basicCancelOk:
		r.name = d.shortStr()
	case channelFlowOk:
		if d.u8()&1 != 0 {
			r.n1 = 1
		}
	}
	if d.err != nil {
		return d.err
	}
	if cm == basicCancelOk {
		ch.consMu.Lock()
		c := ch.consumers[r.name]
		ch.consMu.Unlock()
		if c != nil && c.abandoned {
			// Reply to the cancel of an abandoned consumer, nobody waits for it
			ch.removeConsumer(r.name)
			return nil
		}
	}
	w := ch.popWaiter()
	if w == nil {
		if ch.IsClosed() {
			return nil // a late reply to a request that failed when the channel closed
		}
		return ch.frameError("unexpected " + methodName(cm))
	}
	if w.expect != cm && !(w.expect == basicGetOk && cm == basicGetEmpty) {
		w.deliver(rpcReply{err: ch.frameError("unexpected " + methodName(cm))})
		return ch.frameError(fmt.Sprintf("expected %s but got %s", methodName(w.expect), methodName(cm)))
	}
	switch cm {
	case basicConsumeOk:
		c := w.consumer
		c.tag = r.name
		ch.consMu.Lock()
		ch.consumers[c.tag] = c
		ch.consMu.Unlock()
		if !w.deliver(r) {
			// The caller gave up waiting, cancel the consumer. It stays
			// registered until cancel-ok, so that messages delivered in the
			// meantime are requeued.
			c.abandoned = true
			c.close(ErrConsumerCancelled, true)
			return ignoreClosed(ch.send(basicCancel, func(b []byte) ([]byte, error) {
				return append(appendShortStr(b, c.tag), 0), nil
			}))
		}
		return nil
	case basicCancelOk:
		if c := ch.removeConsumer(r.name); c != nil {
			c.close(ErrConsumerCancelled, false)
		}
	}
	w.deliver(r)
	return nil
}

func (ch *Channel) handleHeader(payload []byte) error {
	p := &ch.pend
	if p.kind == pendNone || p.header {
		return ch.frameError("unexpected content header")
	}
	d := decoder{b: payload}
	d.u16() // class id
	d.u16() // weight
	p.size = d.u64()
	var headersErr error
	if p.kind == pendReturn {
		headersErr = d.properties(&p.ret.Properties, &ch.propCache)
	} else {
		headersErr = d.properties(&p.delivery.Properties, &ch.propCache)
	}
	if d.err != nil {
		return &Error{Code: FrameError, Reason: "malformed content header", Connection: true}
	}
	if headersErr != nil {
		// Don't fail the connection (and every redelivery) because of
		// headers this client can't decode, deliver the message without them
		ch.conn.logger.Warn("amqp: dropped message headers that couldn't be decoded",
			"channel", ch.id, "error", headersErr)
	}
	if p.size > uint64(math.MaxInt) {
		return &Error{Code: FrameError, Reason: "body too large", Connection: true}
	}
	p.header = true
	if p.size == 0 {
		return ch.contentComplete()
	}
	// The body grows as frames arrive, a large size from the broker isn't
	// trusted until the data is there
	p.body = make([]byte, 0, min(p.size, maxBodyPrealloc))
	return nil
}

func (ch *Channel) handleBody(payload []byte) error {
	p := &ch.pend
	if !p.header {
		return ch.frameError("unexpected content body")
	}
	if uint64(len(p.body))+uint64(len(payload)) > p.size {
		return ch.frameError("content body exceeds the size in the header")
	}
	p.body = append(p.body, payload...)
	if uint64(len(p.body)) == p.size {
		return ch.contentComplete()
	}
	return nil
}

func (ch *Channel) contentComplete() error {
	p := ch.pend
	ch.pend = pending{}
	switch p.kind {
	case pendDeliver:
		p.delivery.Body = p.body
		if ch.closing.Load() && (p.consumer == nil || !p.consumer.noAck) {
			return nil // can't be acknowledged, the broker requeues it
		}
		if c := p.consumer; c != nil && c.abandoned {
			if !c.noAck {
				return ignoreClosed(ch.BasicNack(p.delivery.DeliveryTag, false, true))
			}
			return nil
		}
		if p.consumer == nil || !p.consumer.push(p.delivery) {
			ch.conn.logger.Debug("amqp: delivery for unknown or cancelled consumer",
				"channel", ch.id, "consumer_tag", p.delivery.ConsumerTag)
		}
	case pendGet:
		p.delivery.Body = p.body
		w := ch.popWaiter()
		if w == nil && ch.IsClosed() {
			return nil // the broker requeues it when the channel closes
		}
		if w == nil || w.expect != basicGetOk {
			return ch.frameError("unexpected basic.get-ok")
		}
		if w.noAck {
			p.delivery.acked.Store(true)
		}
		if !w.deliver(rpcReply{cm: basicGetOk, delivery: p.delivery}) && !w.noAck {
			// The caller gave up waiting, put the message back
			return ignoreClosed(ch.BasicNack(p.delivery.DeliveryTag, false, true))
		}
	case pendReturn:
		p.ret.Body = p.body
		if fn := ch.onReturn.Load(); fn != nil {
			(*fn)(p.ret)
		} else {
			ch.conn.logger.Warn("amqp: message returned but no return handler set",
				"channel", ch.id, "exchange", p.ret.Exchange, "routing_key", p.ret.RoutingKey,
				"reply_code", p.ret.ReplyCode, "reply_text", p.ret.ReplyText)
		}
	}
	return nil
}

// ignoreClosed drops errors from writes on closed channels in the read loop.
func ignoreClosed(err error) error {
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}

// confirmed processes a basic.ack or basic.nack from the broker.
func (ch *Channel) confirmed(tag uint64, multiple, ack bool) {
	ch.confMu.Lock()
	defer ch.confMu.Unlock()
	if !ack {
		ch.nacked = true
	}
	u := ch.unconfirmed[ch.uhead:]
	switch {
	case multiple:
		i := 0
		for i < len(u) && u[i].tag <= tag {
			if u[i].conf != nil {
				u[i].conf.resolve(ack, nil)
			}
			u[i] = unconfirmed{}
			i++
		}
		ch.uhead += i
	case len(u) > 0 && u[0].tag == tag:
		if u[0].conf != nil {
			u[0].conf.resolve(ack, nil)
		}
		u[0] = unconfirmed{}
		ch.uhead++
	default:
		i := sort.Search(len(u), func(i int) bool { return u[i].tag >= tag })
		if i < len(u) && u[i].tag == tag {
			if u[i].conf != nil {
				u[i].conf.resolve(ack, nil)
			}
			copy(u[i:], u[i+1:])
			u[len(u)-1] = unconfirmed{}
			ch.unconfirmed = ch.unconfirmed[:len(ch.unconfirmed)-1]
		}
	}
	// Reuse the slice's backing array
	if ch.uhead == len(ch.unconfirmed) {
		ch.unconfirmed, ch.uhead = ch.unconfirmed[:0], 0
	} else if ch.uhead > 1024 && ch.uhead > len(ch.unconfirmed)/2 {
		n := copy(ch.unconfirmed, ch.unconfirmed[ch.uhead:])
		clear(ch.unconfirmed[n:])
		ch.unconfirmed, ch.uhead = ch.unconfirmed[:n], 0
	}
	if len(ch.confWaiters) > 0 {
		ws := ch.confWaiters[:0]
		for _, w := range ch.confWaiters {
			if ch.allConfirmedLocked(w.target) {
				close(w.done)
			} else {
				ws = append(ws, w)
			}
		}
		clear(ch.confWaiters[len(ws):])
		ch.confWaiters = ws
	}
}

func (ch *Channel) allConfirmedLocked(target uint64) bool {
	return ch.uhead == len(ch.unconfirmed) || ch.unconfirmed[ch.uhead].tag > target
}

// Confirmation is the broker's confirmation of a published message, see
// [Channel.BasicPublishConfirm].
type Confirmation struct {
	tag  uint64
	done chan struct{}
	ack  bool
	err  error
}

func (c *Confirmation) resolve(ack bool, err error) {
	c.ack, c.err = ack, err
	close(c.done)
}

// DeliveryTag returns the publish sequence number of the message.
func (c *Confirmation) DeliveryTag() uint64 { return c.tag }

// Done returns a channel that's closed when the broker has acked or nacked
// the message, or the channel was closed before that.
func (c *Confirmation) Done() <-chan struct{} { return c.done }

// Acked reports whether the broker acked the message, valid after Done is
// closed.
func (c *Confirmation) Acked() bool {
	select {
	case <-c.done:
		return c.ack
	default:
		return false
	}
}

// Wait waits for the confirmation. It returns nil if the broker acked the
// message, ErrPublishNacked if it was nacked, or an error matching
// ErrUnconfirmed (and ErrClosed) if the channel closed before the message
// was confirmed. The message may or may not have been routed then, and
// [Channel.Err] tells why the channel closed.
func (c *Confirmation) Wait(ctx context.Context) error {
	select {
	case <-c.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if c.err != nil {
		return c.err
	}
	if !c.ack {
		return ErrPublishNacked
	}
	return nil
}

// Publishing is a message to publish.
type Publishing struct {
	Properties
	Body []byte
	// Mandatory makes the broker return the message, see
	// [Channel.OnReturn], if it can't be routed to any queue.
	Mandatory bool
}

// BasicPublish publishes a message to an exchange. It returns when the
// message is written to the connection's buffer, it's then sent to the
// broker in the background. If the broker has blocked the connection it
// waits until it's unblocked or ctx is done.
//
// Use [Channel.BasicPublishConfirm] to know when the broker has taken
// responsibility for the message.
func (ch *Channel) BasicPublish(ctx context.Context, exchange, routingKey string, msg Publishing) error {
	_, err := ch.publish(ctx, exchange, routingKey, &msg, false)
	return err
}

// BasicPublishConfirm publishes a message and returns a confirmation to
// wait on. It puts the channel in confirm mode if it isn't already.
// Waiting for each confirmation in turn limits the throughput to one
// message per round trip, so publish many messages concurrently (or
// pipeline them) and wait for the confirmations afterwards.
func (ch *Channel) BasicPublishConfirm(ctx context.Context, exchange, routingKey string, msg Publishing) (*Confirmation, error) {
	if !ch.confirmMode.Load() {
		if err := ch.ConfirmSelect(ctx); err != nil {
			return nil, err
		}
	}
	return ch.publish(ctx, exchange, routingKey, &msg, true)
}

func (ch *Channel) publish(ctx context.Context, exchange, routingKey string, msg *Publishing, wantConfirm bool) (*Confirmation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkShortStr("exchange name", exchange); err != nil {
		return nil, err
	}
	if err := checkShortStr("routing key", routingKey); err != nil {
		return nil, err
	}
	if err := msg.Properties.validate(); err != nil {
		return nil, err
	}
	c := ch.conn
	if err := c.waitUnblocked(ctx); err != nil {
		return nil, err
	}
	select {
	case <-ch.done:
		return nil, ch.err
	default:
	}

	body := msg.Body
	maxBody := int(c.frameMax) - frameOverhead
	need := 512 + len(body) + (len(body)/maxBody+1)*frameOverhead
	b, err := c.beginWrite(ctx, need, false)
	if err != nil {
		return nil, err
	}
	if ch.wclosed {
		c.cancelWrite()
		if err := ch.Err(); err != nil {
			return nil, err
		}
		return nil, ErrClosed
	}
	b, start := beginMethod(b, ch.id, basicPublish)
	b = append(b, 0, 0) // reserved
	b = appendShortStr(b, exchange)
	b = appendShortStr(b, routingKey)
	if msg.Mandatory {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = endFrame(b, start)
	b, start = beginFrame(b, frameHeader, ch.id)
	b = be.AppendUint16(b, classBasic)
	b = append(b, 0, 0) // weight
	b = be.AppendUint64(b, uint64(len(body)))
	if b, err = appendProperties(b, &msg.Properties); err != nil {
		c.cancelWrite()
		return nil, err
	}
	if size := len(b) - start; size > maxBody {
		c.cancelWrite()
		return nil, fmt.Errorf("amqp: content header of %d bytes exceeds frame max %d", size, c.frameMax)
	}
	b = endFrame(b, start)
	for len(body) > 0 {
		n := min(len(body), maxBody)
		b, start = beginFrame(b, frameBody, ch.id)
		b = append(b, body[:n]...)
		b = endFrame(b, start)
		body = body[n:]
	}

	var conf *Confirmation
	if ch.confirmMode.Load() {
		tag := ch.publishSeq.Add(1)
		if wantConfirm {
			conf = &Confirmation{tag: tag, done: make(chan struct{})}
		}
		ch.confMu.Lock()
		ch.unconfirmed = append(ch.unconfirmed, unconfirmed{tag, conf})
		ch.confMu.Unlock()
	}
	c.endWrite(b)
	return conf, nil
}

// ConfirmSelect puts the channel in confirm mode, the broker then acks or
// nacks every published message.
func (ch *Channel) ConfirmSelect(ctx context.Context) error {
	if ch.confirmMode.Load() {
		return nil
	}
	_, err := ch.call(ctx, confirmSelect, confirmSelectOk, callOpts{
		// Publishes are numbered from the first one after confirm.select
		onWrite: func() { ch.confirmMode.Store(true) },
	}, func(b []byte) ([]byte, error) {
		return append(b, 0), nil // no-wait
	})
	return err
}

// WaitForConfirms waits until all messages published on the channel before
// the call are confirmed. It returns ErrPublishNacked if any message was
// nacked since the previous call, and an error matching ErrUnconfirmed if
// the channel closed with messages unconfirmed.
func (ch *Channel) WaitForConfirms(ctx context.Context) error {
	if !ch.confirmMode.Load() {
		return ErrNoConfirmMode
	}
	target := ch.publishSeq.Load()
	ch.confMu.Lock()
	if !ch.allConfirmedLocked(target) {
		w := confirmWaiter{target, make(chan struct{})}
		ch.confWaiters = append(ch.confWaiters, w)
		ch.confMu.Unlock()
		select {
		case <-w.done:
		case <-ch.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		ch.confMu.Lock()
	}
	nacked, confErr := ch.nacked, ch.confErr
	ch.nacked = false
	ch.confMu.Unlock()
	if confErr != nil {
		return confErr
	}
	if nacked {
		return ErrPublishNacked
	}
	return nil
}

// ConsumeOptions are the options for [Channel.BasicConsume].
type ConsumeOptions struct {
	// Tag identifies the consumer, the broker generates one if empty.
	Tag string
	// NoAck makes the broker consider messages acknowledged as soon as
	// they're delivered.
	NoAck bool
	// Exclusive requests to be the only consumer of the queue.
	Exclusive bool
	// NoLocal isn't supported by RabbitMQ or LavinMQ.
	NoLocal bool
	// Args are optional arguments, e.g. "x-stream-offset" or "x-priority".
	Args Table
}

// BasicConsume starts a consumer on a queue. Read deliveries with
// [Consumer.Next] or [Consumer.Deliveries]. Set a prefetch limit with
// [Channel.BasicQos] first, as deliveries are buffered in memory until read.
func (ch *Channel) BasicConsume(ctx context.Context, queue string, opts ConsumeOptions) (*Consumer, error) {
	if err := checkShortStr("queue name", queue); err != nil {
		return nil, err
	}
	if err := checkShortStr("consumer tag", opts.Tag); err != nil {
		return nil, err
	}
	c := newConsumer(ch, queue, opts.NoAck)
	_, err := ch.call(ctx, basicConsume, basicConsumeOk, callOpts{consumer: c}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, queue)
		b = appendShortStr(b, opts.Tag)
		b = appendBits(b, opts.NoLocal, opts.NoAck, opts.Exclusive, false)
		return appendTable(b, opts.Args)
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// BasicCancel cancels a consumer. Deliveries already received can still be
// read from the consumer.
func (ch *Channel) BasicCancel(ctx context.Context, consumerTag string) error {
	if err := checkShortStr("consumer tag", consumerTag); err != nil {
		return err
	}
	_, err := ch.call(ctx, basicCancel, basicCancelOk, callOpts{}, func(b []byte) ([]byte, error) {
		return append(appendShortStr(b, consumerTag), 0), nil
	})
	return err
}

// BasicGet polls a queue for a message. ok is false if the queue is empty.
// Unless noAck the message has to be acknowledged.
func (ch *Channel) BasicGet(ctx context.Context, queue string, noAck bool) (msg *Delivery, ok bool, err error) {
	if err := checkShortStr("queue name", queue); err != nil {
		return nil, false, err
	}
	r, err := ch.call(ctx, basicGet, basicGetOk, callOpts{noAck: noAck}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, queue)
		return appendBits(b, noAck), nil
	})
	if err != nil || r.cm == basicGetEmpty {
		return nil, false, err
	}
	return r.delivery, true, nil
}

// BasicAck acknowledges a delivery, or with multiple all deliveries up to
// and including deliveryTag.
func (ch *Channel) BasicAck(deliveryTag uint64, multiple bool) error {
	if multiple {
		ch.settled(deliveryTag)
	}
	return ch.send(basicAck, func(b []byte) ([]byte, error) {
		return appendBits(be.AppendUint64(b, deliveryTag), multiple), nil
	})
}

// BasicNack rejects a delivery, or with multiple all deliveries up to and
// including deliveryTag. With requeue the messages are put back in the
// queue, otherwise they're dropped or dead-lettered.
func (ch *Channel) BasicNack(deliveryTag uint64, multiple, requeue bool) error {
	if multiple {
		ch.settled(deliveryTag)
	}
	return ch.send(basicNack, func(b []byte) ([]byte, error) {
		return appendBits(be.AppendUint64(b, deliveryTag), multiple, requeue), nil
	})
}

// settled records that all deliveries up to tag are acknowledged.
func (ch *Channel) settled(tag uint64) {
	for {
		old := ch.settledUpTo.Load()
		if tag <= old || ch.settledUpTo.CompareAndSwap(old, tag) {
			return
		}
	}
}

// BasicReject rejects a single delivery, see [Channel.BasicNack].
func (ch *Channel) BasicReject(deliveryTag uint64, requeue bool) error {
	return ch.send(basicReject, func(b []byte) ([]byte, error) {
		return appendBits(be.AppendUint64(b, deliveryTag), requeue), nil
	})
}

// BasicQos limits the number of unacknowledged messages delivered to each
// consumer on the channel (or, with global, shared by all consumers on the
// channel in RabbitMQ). Zero means unlimited.
func (ch *Channel) BasicQos(ctx context.Context, prefetchCount uint16, global bool) error {
	_, err := ch.call(ctx, basicQos, basicQosOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = be.AppendUint32(b, 0) // prefetch size
		b = be.AppendUint16(b, prefetchCount)
		return appendBits(b, global), nil
	})
	return err
}

// BasicRecover asks the broker to redeliver all unacknowledged messages on
// the channel. Not supported by all brokers with requeue=false.
func (ch *Channel) BasicRecover(ctx context.Context, requeue bool) error {
	_, err := ch.call(ctx, basicRecover, basicRecoverOk, callOpts{}, func(b []byte) ([]byte, error) {
		return appendBits(b, requeue), nil
	})
	return err
}

// Flow asks the broker to pause (active=false) or resume deliveries.
// Not supported by RabbitMQ.
func (ch *Channel) Flow(ctx context.Context, active bool) error {
	_, err := ch.call(ctx, channelFlow, channelFlowOk, callOpts{}, func(b []byte) ([]byte, error) {
		return appendBits(b, active), nil
	})
	return err
}

// QueueDeclareOptions are the options for [Channel.QueueDeclare].
type QueueDeclareOptions struct {
	// Passive only checks if the queue exists, the channel is closed with
	// a NotFound error otherwise.
	Passive bool
	// Durable queues survive broker restarts. Note that messages also have
	// to be published as persistent to survive.
	Durable bool
	// Exclusive queues can only be used by this connection and are deleted
	// when it closes.
	Exclusive bool
	// AutoDelete queues are deleted when their last consumer is cancelled.
	AutoDelete bool
	// Args are optional arguments, e.g. "x-queue-type", "x-max-length" or
	// "x-dead-letter-exchange".
	Args Table
}

// QueueInfo is the broker's reply to a queue declaration.
type QueueInfo struct {
	Name          string // the queue name, generated by the broker if declared with ""
	MessageCount  uint32 // number of messages ready in the queue
	ConsumerCount uint32 // number of consumers
}

// QueueDeclare declares a queue, creating it if it doesn't exist. Pass an
// empty name to let the broker generate a unique name.
func (ch *Channel) QueueDeclare(ctx context.Context, name string, opts QueueDeclareOptions) (QueueInfo, error) {
	if err := checkShortStr("queue name", name); err != nil {
		return QueueInfo{}, err
	}
	r, err := ch.call(ctx, queueDeclare, queueDeclareOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, name)
		b = appendBits(b, opts.Passive, opts.Durable, opts.Exclusive, opts.AutoDelete, false)
		return appendTable(b, opts.Args)
	})
	return QueueInfo{Name: r.name, MessageCount: r.n1, ConsumerCount: r.n2}, err
}

// QueueBind binds a queue to an exchange. Args are matched against message
// headers by headers exchanges.
func (ch *Channel) QueueBind(ctx context.Context, queue, exchange, routingKey string, args Table) error {
	if err := checkNames(queue, exchange, routingKey); err != nil {
		return err
	}
	_, err := ch.call(ctx, queueBind, queueBindOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, queue)
		b = appendShortStr(b, exchange)
		b = appendShortStr(b, routingKey)
		b = append(b, 0) // no-wait
		return appendTable(b, args)
	})
	return err
}

// QueueUnbind removes a binding between a queue and an exchange.
func (ch *Channel) QueueUnbind(ctx context.Context, queue, exchange, routingKey string, args Table) error {
	if err := checkNames(queue, exchange, routingKey); err != nil {
		return err
	}
	_, err := ch.call(ctx, queueUnbind, queueUnbindOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, queue)
		b = appendShortStr(b, exchange)
		b = appendShortStr(b, routingKey)
		return appendTable(b, args)
	})
	return err
}

// QueuePurge removes all ready messages from a queue and returns how many
// were removed.
func (ch *Channel) QueuePurge(ctx context.Context, queue string) (uint32, error) {
	if err := checkShortStr("queue name", queue); err != nil {
		return 0, err
	}
	r, err := ch.call(ctx, queuePurge, queuePurgeOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, queue)
		return append(b, 0), nil // no-wait
	})
	return r.n1, err
}

// QueueDeleteOptions are the options for [Channel.QueueDelete].
type QueueDeleteOptions struct {
	IfUnused bool // only delete if the queue has no consumers
	IfEmpty  bool // only delete if the queue has no messages
}

// QueueDelete deletes a queue and returns the number of messages it had.
func (ch *Channel) QueueDelete(ctx context.Context, queue string, opts QueueDeleteOptions) (uint32, error) {
	if err := checkShortStr("queue name", queue); err != nil {
		return 0, err
	}
	r, err := ch.call(ctx, queueDelete, queueDeleteOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, queue)
		return appendBits(b, opts.IfUnused, opts.IfEmpty, false), nil
	})
	return r.n1, err
}

// Exchange types
const (
	Direct  = "direct"
	Fanout  = "fanout"
	Topic   = "topic"
	Headers = "headers"
)

// ExchangeDeclareOptions are the options for [Channel.ExchangeDeclare].
type ExchangeDeclareOptions struct {
	// Passive only checks if the exchange exists, the channel is closed
	// with a NotFound error otherwise.
	Passive bool
	// Durable exchanges survive broker restarts.
	Durable bool
	// AutoDelete exchanges are deleted when their last binding is removed.
	AutoDelete bool
	// Internal exchanges can't be published to directly, only via
	// exchange-to-exchange bindings.
	Internal bool
	// Args are optional arguments, e.g. "alternate-exchange".
	Args Table
}

// ExchangeDeclare declares an exchange of the given kind, e.g. [Direct],
// [Fanout], [Topic], [Headers] or a plugin type such as
// "x-consistent-hash".
func (ch *Channel) ExchangeDeclare(ctx context.Context, name, kind string, opts ExchangeDeclareOptions) error {
	if err := checkShortStr("exchange name", name); err != nil {
		return err
	}
	if err := checkShortStr("exchange type", kind); err != nil {
		return err
	}
	_, err := ch.call(ctx, exchangeDeclare, exchangeDeclareOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, name)
		b = appendShortStr(b, kind)
		b = appendBits(b, opts.Passive, opts.Durable, opts.AutoDelete, opts.Internal, false)
		return appendTable(b, opts.Args)
	})
	return err
}

// ExchangeDelete deletes an exchange. With ifUnused it's only deleted if it
// has no bindings.
func (ch *Channel) ExchangeDelete(ctx context.Context, name string, ifUnused bool) error {
	if err := checkShortStr("exchange name", name); err != nil {
		return err
	}
	_, err := ch.call(ctx, exchangeDelete, exchangeDeleteOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, name)
		return appendBits(b, ifUnused, false), nil
	})
	return err
}

// ExchangeBind binds the destination exchange to the source exchange, so
// that messages routed by the source with a matching routing key are
// routed by the destination too.
func (ch *Channel) ExchangeBind(ctx context.Context, destination, source, routingKey string, args Table) error {
	if err := checkNames(destination, source, routingKey); err != nil {
		return err
	}
	_, err := ch.call(ctx, exchangeBind, exchangeBindOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, destination)
		b = appendShortStr(b, source)
		b = appendShortStr(b, routingKey)
		b = append(b, 0) // no-wait
		return appendTable(b, args)
	})
	return err
}

// ExchangeUnbind removes an exchange-to-exchange binding.
func (ch *Channel) ExchangeUnbind(ctx context.Context, destination, source, routingKey string, args Table) error {
	if err := checkNames(destination, source, routingKey); err != nil {
		return err
	}
	_, err := ch.call(ctx, exchangeUnbind, exchangeUnbindOk, callOpts{}, func(b []byte) ([]byte, error) {
		b = append(b, 0, 0) // reserved
		b = appendShortStr(b, destination)
		b = appendShortStr(b, source)
		b = appendShortStr(b, routingKey)
		b = append(b, 0) // no-wait
		return appendTable(b, args)
	})
	return err
}

func checkNames(a, b, routingKey string) error {
	if err := checkShortStr("name", a); err != nil {
		return err
	}
	if err := checkShortStr("name", b); err != nil {
		return err
	}
	return checkShortStr("routing key", routingKey)
}

// TxSelect puts the channel in transaction mode.
func (ch *Channel) TxSelect(ctx context.Context) error {
	_, err := ch.call(ctx, txSelect, txSelectOk, callOpts{}, nil)
	return err
}

// TxCommit commits the current transaction.
func (ch *Channel) TxCommit(ctx context.Context) error {
	_, err := ch.call(ctx, txCommit, txCommitOk, callOpts{}, nil)
	return err
}

// TxRollback abandons the current transaction.
func (ch *Channel) TxRollback(ctx context.Context) error {
	_, err := ch.call(ctx, txRollback, txRollbackOk, callOpts{}, nil)
	return err
}
