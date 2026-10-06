// Package amqp is a fast AMQP 0-9-1 client without dependencies, for
// brokers such as LavinMQ and RabbitMQ.
//
// It has two APIs.
//
// # High-level API
//
// [NewClient] returns a [Client] that keeps a connection to the broker. It
// reconnects when the connection is lost and then recovers the exchanges,
// queues, bindings and subscriptions declared through it. Messages are
// published as persistent and confirmed by the broker by default, and
// subscriptions acknowledge messages when the handler returns:
//
//	c, err := amqp.NewClient(ctx, "amqp://guest:guest@localhost", nil)
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	q, err := c.Queue(ctx, "jobs", nil) // durable
//	if err != nil {
//		return err
//	}
//	sub, err := q.Subscribe(ctx, func(ctx context.Context, d *amqp.Delivery) error {
//		return process(d.Body) // ack on nil, requeue on error
//	}, &amqp.SubscribeOptions{Prefetch: 50, Workers: 4})
//	if err != nil {
//		return err
//	}
//	defer sub.Cancel(ctx)
//
//	err = q.Publish(ctx, amqp.Message{Body: "hello"}) // waits for the confirm
//
// With [Codecs] message bodies are serialized and encoded according to
// their content type and content encoding, see [ClientOptions] and
// [Delivery.Decode]. [Client.RPCServer] and [Client.RPCCall] implement
// request/reply over direct reply-to.
//
// # Low-level API
//
// [Dial] returns a [Connection] and [Connection.Channel] opens a [Channel]
// with methods that map one-to-one to the protocol, e.g.
// [Channel.QueueDeclare], [Channel.BasicPublish] and
// [Channel.BasicConsume]. Nothing is recovered if the connection is lost:
//
//	conn, err := amqp.Dial(ctx, "amqp://guest:guest@localhost", nil)
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//	ch, err := conn.Channel(ctx)
//	if err != nil {
//		return err
//	}
//	q, err := ch.QueueDeclare(ctx, "", amqp.QueueDeclareOptions{Exclusive: true})
//	if err != nil {
//		return err
//	}
//	err = ch.BasicPublish(ctx, "", q.Name, amqp.Publishing{Body: []byte("hi")})
//
// # Concurrency and performance
//
// Connections, channels and clients are safe for concurrent use.
// Synchronous methods called concurrently on one channel are pipelined.
// Frames are written to a buffer and flushed by a background goroutine, so
// that concurrent or rapid publishes and acks are sent in batches with few
// syscalls. Received messages are buffered per consumer, so a slow consumer
// never stalls the connection; limit the buffer with a prefetch count.
//
// # Errors
//
// Exceptions from the broker are returned as [*Error], with a reply code
// such as [NotFound] or [AccessRefused], check them with [IsCode]. A
// channel exception closes the channel, a connection exception the
// connection. All errors caused by a closed channel or connection match
// [ErrClosed] with errors.Is.
package amqp
