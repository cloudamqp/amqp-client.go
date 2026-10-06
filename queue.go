package amqp

import (
	"context"
	"sync/atomic"
)

// QueueOptions are the options for [Client.Queue], see
// [QueueDeclareOptions] for their meaning.
type QueueOptions struct {
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Passive    bool
	Args       Table
}

// Queue is a handle to a queue declared with [Client.Queue].
type Queue struct {
	c           *Client
	name        atomic.Pointer[string]
	opts        QueueOptions
	serverNamed bool
}

// Name returns the queue's name. The name of a queue declared with an
// empty name changes when the client reconnects.
func (q *Queue) Name() string { return *q.name.Load() }

func (q *Queue) declare(ctx context.Context, ch *Channel) error {
	name := ""
	if !q.serverNamed {
		name = q.Name()
	}
	info, err := ch.QueueDeclare(ctx, name, QueueDeclareOptions{
		Passive:    q.opts.Passive,
		Durable:    q.opts.Durable,
		Exclusive:  q.opts.Exclusive,
		AutoDelete: q.opts.AutoDelete,
		Args:       q.opts.Args,
	})
	if err != nil {
		return err
	}
	q.name.Store(&info.Name)
	return nil
}

// Publish publishes a message directly to the queue, via the default
// exchange, and waits for the broker to confirm it. See [Client.Publish].
func (q *Queue) Publish(ctx context.Context, msg Message) error {
	return q.c.Publish(ctx, "", q.Name(), msg)
}

// Bind binds the queue to an exchange. The binding is recovered when the
// client reconnects.
func (q *Queue) Bind(ctx context.Context, exchange, bindingKey string, args Table) error {
	err := q.c.withChannel(ctx, func(ch *Channel) error {
		return ch.QueueBind(ctx, q.Name(), exchange, bindingKey, args)
	})
	if err == nil {
		q.c.addQueueBinding(queueBinding{q, exchange, bindingKey, args})
	}
	return err
}

// Unbind removes a binding to an exchange.
func (q *Queue) Unbind(ctx context.Context, exchange, bindingKey string, args Table) error {
	err := q.c.withChannel(ctx, func(ch *Channel) error {
		return ch.QueueUnbind(ctx, q.Name(), exchange, bindingKey, args)
	})
	if err == nil {
		q.c.removeQueueBinding(queueBinding{q, exchange, bindingKey, args})
	}
	return err
}

// Get fetches a message from the queue, ok is false if it's empty. Unless
// noAck the message has to be acknowledged.
func (q *Queue) Get(ctx context.Context, noAck bool) (msg *Delivery, ok bool, err error) {
	err = q.c.withChannel(ctx, func(ch *Channel) error {
		msg, ok, err = ch.BasicGet(ctx, q.Name(), noAck)
		return err
	})
	return msg, ok, err
}

// Purge removes all ready messages from the queue and returns how many
// were removed.
func (q *Queue) Purge(ctx context.Context) (n uint32, err error) {
	err = q.c.withChannel(ctx, func(ch *Channel) error {
		n, err = ch.QueuePurge(ctx, q.Name())
		return err
	})
	return n, err
}

// Delete deletes the queue and returns the number of messages it had. It
// is no longer recovered on reconnect.
func (q *Queue) Delete(ctx context.Context, opts QueueDeleteOptions) (n uint32, err error) {
	err = q.c.withChannel(ctx, func(ch *Channel) error {
		n, err = ch.QueueDelete(ctx, q.Name(), opts)
		return err
	})
	if err == nil {
		q.c.removeQueue(q)
	}
	return n, err
}

// Subscribe consumes messages from the queue, calling handler for each,
// see [Subscription].
func (q *Queue) Subscribe(ctx context.Context, handler Handler, opts *SubscribeOptions) (*Subscription, error) {
	return q.c.subscribe(ctx, q, handler, opts)
}

// ExchangeOptions are the options for [Client.Exchange], see
// [ExchangeDeclareOptions] for their meaning.
type ExchangeOptions struct {
	Durable    bool
	AutoDelete bool
	Internal   bool
	Passive    bool
	Args       Table
}

// Exchange is a handle to an exchange declared with [Client.Exchange].
type Exchange struct {
	c    *Client
	name string
	kind string
	opts ExchangeOptions
}

// Name returns the exchange's name.
func (x *Exchange) Name() string { return x.name }

// Kind returns the exchange type.
func (x *Exchange) Kind() string { return x.kind }

func (x *Exchange) declare(ctx context.Context, ch *Channel) error {
	return ch.ExchangeDeclare(ctx, x.name, x.kind, ExchangeDeclareOptions{
		Passive:    x.opts.Passive,
		Durable:    x.opts.Durable,
		AutoDelete: x.opts.AutoDelete,
		Internal:   x.opts.Internal,
		Args:       x.opts.Args,
	})
}

// Publish publishes a message to the exchange and waits for the broker to
// confirm it. See [Client.Publish].
func (x *Exchange) Publish(ctx context.Context, routingKey string, msg Message) error {
	return x.c.Publish(ctx, x.name, routingKey, msg)
}

// Bind binds the exchange (as destination) to the source exchange, so
// that messages matching the routing key are routed to this exchange too.
// The binding is recovered when the client reconnects.
func (x *Exchange) Bind(ctx context.Context, source, routingKey string, args Table) error {
	err := x.c.withChannel(ctx, func(ch *Channel) error {
		return ch.ExchangeBind(ctx, x.name, source, routingKey, args)
	})
	if err == nil {
		x.c.addExchangeBinding(exchangeBinding{x.name, source, routingKey, args})
	}
	return err
}

// Unbind removes a binding to the source exchange.
func (x *Exchange) Unbind(ctx context.Context, source, routingKey string, args Table) error {
	err := x.c.withChannel(ctx, func(ch *Channel) error {
		return ch.ExchangeUnbind(ctx, x.name, source, routingKey, args)
	})
	if err == nil {
		x.c.removeExchangeBinding(exchangeBinding{x.name, source, routingKey, args})
	}
	return err
}

// Delete deletes the exchange. With ifUnused it's only deleted if it has
// no bindings. It's no longer recovered on reconnect.
func (x *Exchange) Delete(ctx context.Context, ifUnused bool) error {
	err := x.c.withChannel(ctx, func(ch *Channel) error {
		return ch.ExchangeDelete(ctx, x.name, ifUnused)
	})
	if err == nil {
		x.c.removeExchange(x.name)
	}
	return err
}
