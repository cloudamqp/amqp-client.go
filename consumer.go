package amqp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrAlreadyAcknowledged is returned when acknowledging or rejecting a
// delivery a second time, or one delivered with no-ack, which would
// otherwise make the broker close the channel.
var ErrAlreadyAcknowledged = errors.New("amqp: delivery already acknowledged or rejected")

// Delivery is a message delivered to a consumer or fetched with
// [Channel.BasicGet].
type Delivery struct {
	Properties
	Body         []byte
	ConsumerTag  string // empty for basic.get
	DeliveryTag  uint64 // identifies the delivery on its channel
	Redelivered  bool   // the message has been delivered before
	Exchange     string // the exchange the message was published to
	RoutingKey   string // the routing key the message was published with
	MessageCount uint32 // messages left in the queue, only for basic.get

	channel *Channel
	acked   atomic.Bool
}

// Channel returns the channel the message was delivered on.
func (d *Delivery) Channel() *Channel { return d.channel }

// settle marks the delivery as acknowledged, it returns false if it
// already was: by Ack, Nack or Reject, by the channel's BasicAck or BasicNack
// with multiple, or because it was delivered with no-ack. Acknowledging it
// again would make the broker close the channel.
func (d *Delivery) settle() bool {
	if d.channel != nil && d.DeliveryTag <= d.channel.settledUpTo.Load() {
		d.acked.Store(true)
		return false
	}
	return d.acked.CompareAndSwap(false, true)
}

// Ack acknowledges the delivery, the broker then removes the message. It
// returns ErrAlreadyAcknowledged if the delivery is already acknowledged
// or rejected, or was delivered with no-ack.
func (d *Delivery) Ack() error {
	if !d.settle() {
		return ErrAlreadyAcknowledged
	}
	return d.channel.BasicAck(d.DeliveryTag, false)
}

// Nack rejects the delivery. With requeue the message is put back in the
// queue, otherwise it's dropped or dead-lettered.
func (d *Delivery) Nack(requeue bool) error {
	if !d.settle() {
		return ErrAlreadyAcknowledged
	}
	return d.channel.BasicNack(d.DeliveryTag, false, requeue)
}

// Reject is like Nack, using basic.reject.
func (d *Delivery) Reject(requeue bool) error {
	if !d.settle() {
		return ErrAlreadyAcknowledged
	}
	return d.channel.BasicReject(d.DeliveryTag, requeue)
}

// Acknowledged reports whether the delivery is acknowledged or rejected,
// with its own methods or the channel's with multiple, or was delivered
// with no-ack.
func (d *Delivery) Acknowledged() bool {
	return d.acked.Load() || d.channel != nil && d.DeliveryTag <= d.channel.settledUpTo.Load()
}

// Decode decodes the body into v according to the message's content
// encoding and content type, using the connection's [Config.Codecs] or
// [DefaultCodecs]. v can also be a *[]byte or *string to get the body
// with only the content encoding decoded.
func (d *Delivery) Decode(v any) error {
	return d.codecs().Unmarshal(d.Body, &d.Properties, v)
}

func (d *Delivery) codecs() *Codecs {
	if d.channel != nil && d.channel.conn.cfg.Codecs != nil {
		return d.channel.conn.cfg.Codecs
	}
	return defaultCodecs
}

// Return is a message the broker couldn't route, published with the
// Mandatory flag.
type Return struct {
	Properties
	Body       []byte
	ReplyCode  uint16 // e.g. NoRoute
	ReplyText  string
	Exchange   string
	RoutingKey string
}

// Consumer receives deliveries from a queue, see [Channel.BasicConsume].
// Deliveries are buffered in memory until read, so the connection never
// waits for a slow consumer. Limit the buffer with [Channel.BasicQos].
type Consumer struct {
	ch    *Channel
	tag   string
	queue string
	noAck bool
	// abandoned is set by the read loop when the caller of BasicConsume
	// gave up waiting for consume-ok.
	abandoned bool

	mu     sync.Mutex
	buf    []*Delivery
	head   int
	closed bool
	err    error
	signal chan struct{}
	done   chan struct{}

	pumpOnce   sync.Once
	deliveries chan *Delivery
}

func newConsumer(ch *Channel, queue string, noAck bool) *Consumer {
	return &Consumer{
		ch:     ch,
		queue:  queue,
		noAck:  noAck,
		signal: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Tag returns the consumer tag.
func (c *Consumer) Tag() string { return c.tag }

// Queue returns the name of the queue consumed from.
func (c *Consumer) Queue() string { return c.queue }

// Channel returns the consumer's channel.
func (c *Consumer) Channel() *Channel { return c.ch }

// push buffers a delivery, called from the read loop.
func (c *Consumer) push(d *Delivery) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.buf = append(c.buf, d)
	c.mu.Unlock()
	select {
	case c.signal <- struct{}{}:
	default:
	}
	return true
}

// close stops the consumer. With discard the buffered deliveries are
// dropped, as they can't be acknowledged when the channel is closed.
func (c *Consumer) close(err error, discard bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.err = err
	if discard {
		clear(c.buf)
		c.buf, c.head = nil, 0
	}
	close(c.done)
}

// Next returns the next delivery, waiting for one if none is buffered.
// When the consumer is cancelled it returns the buffered deliveries first
// and then the reason: ErrConsumerCancelled, ErrConsumerCancelledByServer
// or the channel's error. Next can be called from multiple goroutines.
func (c *Consumer) Next(ctx context.Context) (*Delivery, error) {
	for {
		c.mu.Lock()
		if c.head < len(c.buf) {
			d := c.buf[c.head]
			c.buf[c.head] = nil
			c.head++
			more := c.head < len(c.buf)
			if !more {
				c.buf, c.head = c.buf[:0], 0
			} else if c.head >= 1024 && c.head > len(c.buf)/2 {
				n := copy(c.buf, c.buf[c.head:])
				clear(c.buf[n:])
				c.buf, c.head = c.buf[:n], 0
			}
			c.mu.Unlock()
			if more {
				// Wake up another goroutine waiting in Next
				select {
				case c.signal <- struct{}{}:
				default:
				}
			}
			return d, nil
		}
		if c.closed {
			err := c.err
			c.mu.Unlock()
			return nil, err
		}
		c.mu.Unlock()
		select {
		case <-c.signal:
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Deliveries returns a Go channel of deliveries, which is closed when the
// consumer is cancelled or its channel closed, Err then tells why. The
// returned channel must be read until it's closed. [Consumer.Next] is
// slightly faster.
func (c *Consumer) Deliveries() <-chan *Delivery {
	c.pumpOnce.Do(func() {
		c.deliveries = make(chan *Delivery, 64)
		go func() {
			defer close(c.deliveries)
			for {
				d, err := c.Next(context.Background())
				if err != nil {
					return
				}
				c.deliveries <- d
			}
		}()
	})
	return c.deliveries
}

// Cancel cancels the consumer. Deliveries already buffered can still be
// read, after that Next returns ErrConsumerCancelled.
func (c *Consumer) Cancel(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	default:
	}
	err := c.ch.BasicCancel(ctx, c.tag)
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}

// Done returns a channel that's closed when the consumer stops receiving
// deliveries, there might still be buffered deliveries to read.
func (c *Consumer) Done() <-chan struct{} { return c.done }

// Err returns why the consumer stopped, or nil while it's active.
func (c *Consumer) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
