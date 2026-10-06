package amqp_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/cloudamqp/amqp-client.go"
)

func Example() {
	ctx := context.Background()
	c, err := amqp.NewClient(ctx, "amqp://guest:guest@localhost", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// A server-named, exclusive, auto-delete queue
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		log.Fatal(err)
	}
	received := make(chan string)
	sub, err := q.Subscribe(ctx, func(ctx context.Context, d *amqp.Delivery) error {
		received <- string(d.Body)
		return nil // acknowledges the message
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Cancel(ctx)

	// Waits until the broker has confirmed the message
	if err := q.Publish(ctx, amqp.Message{Body: "Hello world"}); err != nil {
		log.Fatal(err)
	}
	fmt.Println(<-received)
}

func ExampleClient_TopicExchange() {
	ctx := context.Background()
	c, err := amqp.NewClient(ctx, "amqp://localhost", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	events, err := c.TopicExchange(ctx, "events")
	if err != nil {
		log.Fatal(err)
	}
	q, err := c.Queue(ctx, "user-events", nil)
	if err != nil {
		log.Fatal(err)
	}
	// Bindings are recovered after reconnects too
	if err := q.Bind(ctx, events.Name(), "user.*", nil); err != nil {
		log.Fatal(err)
	}
	err = events.Publish(ctx, "user.created", amqp.Message{
		Body:       `{"id": 1}`,
		Properties: amqp.Properties{ContentType: "application/json"},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_codecs() {
	ctx := context.Background()
	c, err := amqp.NewClient(ctx, "amqp://localhost", &amqp.ClientOptions{
		Config:                 amqp.Config{Codecs: amqp.DefaultCodecs()},
		DefaultContentType:     "application/json",
		DefaultContentEncoding: "gzip",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	type Order struct {
		ID    int      `json:"id"`
		Items []string `json:"items"`
	}
	q, err := c.Queue(ctx, "orders", nil)
	if err != nil {
		log.Fatal(err)
	}
	// Serialized as JSON and gzip compressed
	if err := q.Publish(ctx, amqp.Message{Body: Order{ID: 1, Items: []string{"book"}}}); err != nil {
		log.Fatal(err)
	}
	_, err = q.Subscribe(ctx, func(ctx context.Context, d *amqp.Delivery) error {
		var o Order
		if err := d.Decode(&o); err != nil {
			d.Reject(false) // can't be processed, don't requeue
			return err
		}
		fmt.Println(o.ID, o.Items)
		return nil
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_RPCCall() {
	ctx := context.Background()
	c, err := amqp.NewClient(ctx, "amqp://localhost", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	_, err = c.RPCServer(ctx, "upcase", func(ctx context.Context, req *amqp.Delivery) (amqp.Message, error) {
		return amqp.Message{Body: fmt.Sprintf("%X", req.Body)}, nil
	}, nil)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reply, err := c.RPCCall(ctx, "upcase", amqp.Message{Body: "hi"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply.Body))
}

func ExampleDial() {
	ctx := context.Background()
	conn, err := amqp.Dial(ctx, "amqp://guest:guest@localhost", &amqp.Config{
		ConnectionName: "example",
		Heartbeat:      10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel(ctx)
	if err != nil {
		log.Fatal(err)
	}
	q, err := ch.QueueDeclare(ctx, "", amqp.QueueDeclareOptions{Exclusive: true, AutoDelete: true})
	if err != nil {
		log.Fatal(err)
	}
	if err := ch.BasicQos(ctx, 100, false); err != nil {
		log.Fatal(err)
	}
	consumer, err := ch.BasicConsume(ctx, q.Name, amqp.ConsumeOptions{})
	if err != nil {
		log.Fatal(err)
	}

	err = ch.BasicPublish(ctx, "", q.Name, amqp.Publishing{
		Properties: amqp.Properties{ContentType: "text/plain"},
		Body:       []byte("Hello world"),
	})
	if err != nil {
		log.Fatal(err)
	}

	d, err := consumer.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(d.Body))
	d.Ack()
}

func ExampleChannel_BasicPublishConfirm() {
	ctx := context.Background()
	conn, err := amqp.Dial(ctx, "amqp://localhost", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Publish a batch, then wait for all confirms; waiting for each one in
	// turn would limit the throughput to one message per round trip.
	confirms := make([]*amqp.Confirmation, 0, 1000)
	for i := range 1000 {
		c, err := ch.BasicPublishConfirm(ctx, "amq.topic", "numbers", amqp.Publishing{Body: fmt.Append(nil, i)})
		if err != nil {
			log.Fatal(err)
		}
		confirms = append(confirms, c)
	}
	for _, c := range confirms {
		if err := c.Wait(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

func ExampleConsumer_Deliveries() {
	ctx := context.Background()
	conn, err := amqp.Dial(ctx, "amqp://localhost", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel(ctx)
	if err != nil {
		log.Fatal(err)
	}
	ch.BasicQos(ctx, 10, false)
	consumer, err := ch.BasicConsume(ctx, "jobs", amqp.ConsumeOptions{})
	if err != nil {
		log.Fatal(err)
	}
	shutdown := make(chan struct{})
	for {
		select {
		case d, ok := <-consumer.Deliveries():
			if !ok {
				log.Println("consumer stopped:", consumer.Err())
				return
			}
			fmt.Println(string(d.Body))
			d.Ack()
		case <-shutdown:
			consumer.Cancel(ctx)
		}
	}
}

func ExampleIsCode() {
	ctx := context.Background()
	conn, err := amqp.Dial(ctx, "amqp://localhost", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel(ctx)
	if err != nil {
		log.Fatal(err)
	}
	_, err = ch.QueueDeclare(ctx, "might-not-exist", amqp.QueueDeclareOptions{Passive: true})
	switch {
	case amqp.IsCode(err, amqp.NotFound):
		fmt.Println("no such queue") // the channel is closed now
	case errors.Is(err, amqp.ErrClosed):
		fmt.Println("connection lost")
	case err != nil:
		log.Fatal(err)
	}
}
