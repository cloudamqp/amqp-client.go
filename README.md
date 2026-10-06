# amqp-client.go

A fast AMQP 0-9-1 client for Go, without dependencies. It works with any AMQP 0-9-1 broker, such as [LavinMQ](https://lavinmq.com) and RabbitMQ, self-hosted or managed by [CloudAMQP](https://www.cloudamqp.com).

It's a sibling of [amqp-client.rb](https://github.com/cloudamqp/amqp-client.rb), [amqp-client.cr](https://github.com/cloudamqp/amqp-client.cr) and [amqp-client.js](https://github.com/cloudamqp/amqp-client.js), with the same two APIs:

- **High-level** (`Client`): reconnects automatically and recovers exchanges, queues, bindings and consumers. Safe by default: messages are persistent and confirmed by the broker, and consumers acknowledge a message after its handler has processed it.
- **Low-level** (`Connection` and `Channel`): methods that map one-to-one to the protocol, for full control.

Both are idiomatic Go: every blocking call takes a `context.Context`, errors work with `errors.Is`/`errors.As`, and everything is safe for concurrent use.

[API reference](https://pkg.go.dev/github.com/cloudamqp/amqp-client.go)

## Install

```sh
go get github.com/cloudamqp/amqp-client.go
```

Go 1.24 or later is required.

## High-level API

```go
import amqp "github.com/cloudamqp/amqp-client.go"

ctx := context.Background()

// Connects, and reconnects if the connection is lost
c, err := amqp.NewClient(ctx, "amqp://guest:guest@localhost", nil)
if err != nil {
	log.Fatal(err)
}
defer c.Close()

// A durable queue (a queue named "" is server-named, exclusive and auto-delete)
q, err := c.Queue(ctx, "jobs", nil)

// A durable topic exchange, and a binding that is recovered after reconnects
events, err := c.TopicExchange(ctx, "events")
err = q.Bind(ctx, events.Name(), "jobs.#", nil)

// Calls the handler for each message. The message is acked when the handler
// returns nil, and rejected and requeued when it returns an error or panics.
// The handler can also ack/reject the message itself.
sub, err := q.Subscribe(ctx, func(ctx context.Context, d *amqp.Delivery) error {
	log.Printf("received %s", d.Body)
	return nil
}, &amqp.SubscribeOptions{Prefetch: 50, Workers: 4})
defer sub.Cancel(ctx)

// Publishes a persistent message and waits until the broker confirms it
err = q.Publish(ctx, amqp.Message{Body: "hello"})
err = events.Publish(ctx, "jobs.created", amqp.Message{
	Body:       `{"id": 1}`,
	Properties: amqp.Properties{ContentType: "application/json", Headers: amqp.Table{"retries": 0}},
})
```

`Publish` returns once the broker has confirmed the message. Publish from several goroutines to get a higher throughput, because the confirms are pipelined on a shared channel. The first publish to an exchange the client hasn't declared checks that it exists, so that publishing to a missing exchange fails on its own instead of closing the shared channel for every publish in flight.

While the client is reconnecting, operations wait until the connection is back or their context is done. If the channel or connection is lost after a message is sent but before it's confirmed, `Publish` returns an error that matches `amqp.ErrUnconfirmed`. The message may or may not have reached the broker.

### Reconnection

```go
c, err := amqp.NewClient(ctx, url, &amqp.ClientOptions{
	ReconnectInterval:    time.Second,      // doubled after each failed attempt...
	MaxReconnectInterval: 30 * time.Second, // ...up to this
	MaxRetries:           0,                // give up after N failed attempts, 0 = never
	OnConnect: func(ctx context.Context, c *amqp.Client) error {
		return nil // runs after every (re)connection, after recovery
	},
	OnDisconnect: func(err error) { log.Println("disconnected:", err) },
	OnFailed:     func(err error) { log.Println("gave up:", err) },
})
```

Topology that `OnConnect` declares with its context isn't recovered, because `OnConnect` declares it again on every connection. Subscriptions it starts end with their connection, and `OnConnect` starts them again.

Each reconnect attempt times out after `Config.ConnectTimeout` (30s by default). A connection that the broker closes soon after it's established, for example because of a connection limit, counts as a failed attempt, so the backoff and `MaxRetries` apply.

When the client reconnects, it declares exchanges, queues and bindings again. A server-named queue gets a new name, and `Queue.Name()` returns the new one. Subscriptions consume again, also after their channel is closed while the connection stays up, for example after a consumer timeout. If the broker has deleted the queue in the meantime (an auto-delete queue is deleted when its last consumer goes away), it's declared again with its bindings. Messages that were in process when the connection was lost are redelivered, so make handlers idempotent.

### Codecs

Opt in to codecs to have message bodies serialized and compressed according to their `content-type` and `content-encoding`:

```go
c, err := amqp.NewClient(ctx, url, &amqp.ClientOptions{
	Config:                 amqp.Config{Codecs: amqp.DefaultCodecs()}, // JSON, text, gzip, deflate
	DefaultContentType:     "application/json",
	DefaultContentEncoding: "gzip",
})

// Serialized as JSON and gzip compressed
err = q.Publish(ctx, amqp.Message{Body: Order{ID: 1}})

_, err = q.Subscribe(ctx, func(ctx context.Context, d *amqp.Delivery) error {
	var o Order
	if err := d.Decode(&o); err != nil { // gunzip and unmarshal
		d.Reject(false) // can't be processed, dead-letter it instead of requeuing
		return err
	}
	return handle(o)
}, nil)
```

Register your own codecs with `Codecs.RegisterSerializer` (by content type) and `Codecs.RegisterEncoder` (by content encoding). The built-in gzip and deflate encoders refuse to decode more than 128 MiB, to protect against compression bombs; use `amqp.NewGzip(limit)` or `amqp.NewDeflate(limit)` for another limit.

### RPC

Request/reply over [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to):

```go
srv, err := c.RPCServer(ctx, "upcase", func(ctx context.Context, req *amqp.Delivery) (amqp.Message, error) {
	return amqp.Message{Body: strings.ToUpper(string(req.Body))}, nil
}, &amqp.SubscribeOptions{Workers: 8})

ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
reply, err := c.RPCCall(ctx, "upcase", amqp.Message{Body: "hi"})
```

`RPCCall` returns `amqp.ErrUnroutable` right away if no queue exists for the request. If you make many calls, use your own `RPCClient` from `c.RPCClient(ctx)`. Both the servers and the clients recover after reconnects.

## Low-level API

The low-level API doesn't recover anything. Methods map one-to-one to the protocol:

```go
conn, err := amqp.Dial(ctx, "amqp://guest:guest@localhost", &amqp.Config{
	ConnectionName: "my-app",
	Heartbeat:      30 * time.Second,
})
defer conn.Close()

ch, err := conn.Channel(ctx)
q, err := ch.QueueDeclare(ctx, "", amqp.QueueDeclareOptions{Exclusive: true, AutoDelete: true})
err = ch.QueueBind(ctx, q.Name, "amq.topic", "a.#", nil)
err = ch.ExchangeDeclare(ctx, "logs", amqp.Fanout, amqp.ExchangeDeclareOptions{Durable: true})

// Consume. Deliveries are buffered per consumer, so always set a prefetch limit
err = ch.BasicQos(ctx, 100, false)
consumer, err := ch.BasicConsume(ctx, q.Name, amqp.ConsumeOptions{})
go func() {
	for {
		d, err := consumer.Next(ctx) // or range over consumer.Deliveries()
		if err != nil {
			return // amqp.ErrConsumerCancelled, a channel or connection error
		}
		d.Ack()
	}
}()

// Fire and forget: returns when the message is buffered for sending
err = ch.BasicPublish(ctx, "amq.topic", "a.b", amqp.Publishing{Body: []byte("hi")})

// Publisher confirms: publish many messages, then wait for their confirms
conf, err := ch.BasicPublishConfirm(ctx, "", q.Name, amqp.Publishing{
	Properties: amqp.Properties{DeliveryMode: amqp.Persistent},
	Body:       []byte("hi"),
})
err = conf.Wait(ctx)         // or wait for everything published so far:
err = ch.WaitForConfirms(ctx)

// Polling
msg, ok, err := ch.BasicGet(ctx, q.Name, false)

// Mandatory messages that can't be routed are returned
ch.OnReturn(func(r *amqp.Return) { log.Printf("returned: %s", r.ReplyText) })
```

Other methods are `QueuePurge`, `QueueDelete`, `QueueUnbind`, `ExchangeDelete`, `ExchangeBind`/`Unbind`, `BasicNack`, `BasicReject`, `BasicRecover`, `BasicCancel`, `TxSelect`/`Commit`/`Rollback`, `Flow` and `Connection.UpdateSecret`, which renews an OAuth 2 token without reconnecting.

Lifecycles follow the `context` pattern. `Done()` returns a channel that's closed when the connection, channel, consumer or subscription stops, and `Err()` returns the reason.

### Errors

```go
_, err := ch.QueueDeclare(ctx, "q", amqp.QueueDeclareOptions{Passive: true})
switch {
case amqp.IsCode(err, amqp.NotFound): // an *amqp.Error from the broker, the channel is now closed
case errors.Is(err, amqp.ErrClosed):  // the channel or connection is closed
}
```

Broker exceptions are returned as `*amqp.Error`, with `Code`, `Reason`, `ClassID`/`MethodID` and whether the whole `Connection` was closed. All errors that come from a closed channel or connection match `amqp.ErrClosed`, including network errors.

## Performance

The library is designed to avoid allocations and syscalls:

- Frames are encoded straight into the write buffer. Encoding a publish takes about 90 ns and allocates nothing.
- A background goroutine flushes the write buffer, so concurrent or rapid publishes and acks are sent in batches with few syscalls. Publishes are still bounded by TCP backpressure.
- Received frames are parsed in place in the read buffer. Receiving a message allocates only the `Delivery` and its body; repeated strings such as the exchange, routing key and content type are reused.
- Each consumer has its own buffer, so a slow consumer never stalls the connection, and synchronous calls on a channel are pipelined.

The table below compares 1-byte messages with no properties, with one goroutine publishing or consuming, against LavinMQ 2.2.0 on the same machine (Intel i9-13900H). Consuming includes acknowledging each message, with a prefetch of 1000.

| Client                                                         | Publish rate   | Consume rate   |
| -------------------------------------------------------------- | -------------- | -------------- |
| amqp-client.go                                                 | 1 550 000 msgs/s | 1 200 000 msgs/s |
| [amqp091-go](https://github.com/rabbitmq/amqp091-go) v1.15.0   | 200 000 msgs/s | 45 000 msgs/s  |

Run the benchmarks against a broker with `go test -run=^$ -bench=. -benchmem`.

## Development

Most tests need a broker, by default at `amqp://guest:guest@localhost:5672`. Set `AMQP_URL` to use another one. If no broker is reachable, those tests are skipped.

```sh
go test -race ./...
go test -short ./...  # skips the slow heartbeat tests
```

## License

The library is available as open source under the terms of the [MIT License](LICENSE).
