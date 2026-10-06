package amqp

import (
	"bufio"
	"context"
	"io"
	"testing"
)

// Benchmarks against a broker, e.g.:
//
//	go test -run=^$ -bench=. -benchmem

func BenchmarkPublish(b *testing.B) {
	conn := dialTest(b, nil)
	ch := openChannel(b, conn)
	ctx := context.Background()
	q := tempQueue(b, ch)
	ch2 := openChannel(b, conn)
	cons, err := ch2.BasicConsume(ctx, q, ConsumeOptions{NoAck: true})
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		for {
			if _, err := cons.Next(ctx); err != nil {
				return
			}
		}
	}()
	msg := Publishing{Body: []byte("x")}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := ch.BasicPublish(ctx, "", q, msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishConfirm(b *testing.B) {
	conn := dialTest(b, nil)
	ch := openChannel(b, conn)
	ctx := context.Background()
	info, err := ch.QueueDeclare(ctx, "", QueueDeclareOptions{Exclusive: true, AutoDelete: true,
		Args: Table{"x-max-length": 1000}})
	if err != nil {
		b.Fatal(err)
	}
	q := info.Name
	msg := Publishing{Body: []byte("x")}
	confs := make(chan *Confirmation, 1000)
	done := make(chan error)
	go func() {
		for c := range confs {
			if err := c.Wait(ctx); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, err := ch.BasicPublishConfirm(ctx, "", q, msg)
		if err != nil {
			b.Fatal(err)
		}
		confs <- c
	}
	close(confs)
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkConsume(b *testing.B) {
	conn := dialTest(b, nil)
	ctx := context.Background()
	ch := openChannel(b, conn)
	q := tempQueue(b, ch)
	pub := openChannel(b, conn)
	go func() {
		msg := Publishing{Body: []byte("x")}
		for range b.N {
			if err := pub.BasicPublish(ctx, "", q, msg); err != nil {
				return
			}
		}
	}()
	if err := ch.BasicQos(ctx, 1000, false); err != nil {
		b.Fatal(err)
	}
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := cons.Next(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := d.Ack(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodePublish measures the client side cost of publishing,
// without a broker.
func BenchmarkEncodePublish(b *testing.B) {
	c := &Connection{frameMax: defaultFrameMax, flushCh: make(chan struct{}, 1)}
	c.bw = newDiscardWriter()
	ch := newChannel(c, 1)
	ctx := context.Background()
	msg := Publishing{Body: make([]byte, 100), Properties: Properties{
		ContentType: "application/json", DeliveryMode: Persistent, Headers: Table{"a": "b"},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ch.publish(ctx, "amq.topic", "a.b.c", &msg, false); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeDeliver measures the client side cost of receiving a
// message, without a broker.
func BenchmarkDecodeDeliver(b *testing.B) {
	c := &Connection{frameMax: defaultFrameMax, flushCh: make(chan struct{}, 1), done: make(chan struct{})}
	ch := newChannel(c, 1)
	cons := newConsumer(ch, "q", true)
	cons.tag = "ctag"
	ch.consumers["ctag"] = cons

	deliver, _ := beginMethod(nil, 1, basicDeliver)
	deliver = deliver[7:]
	deliver = appendShortStr(deliver, "ctag")
	deliver = be.AppendUint64(deliver, 1)
	deliver = append(deliver, 0)
	deliver = appendShortStr(deliver, "amq.topic")
	deliver = appendShortStr(deliver, "a.b.c")
	header := be.AppendUint16(nil, classBasic)
	header = append(header, 0, 0)
	header = be.AppendUint64(header, 100)
	header, _ = appendProperties(header, &Properties{ContentType: "application/json", DeliveryMode: Persistent})
	body := make([]byte, 100)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := ch.handleFrame(frameMethod, deliver); err != nil {
			b.Fatal(err)
		}
		if err := ch.handleFrame(frameHeader, header); err != nil {
			b.Fatal(err)
		}
		if err := ch.handleFrame(frameBody, body); err != nil {
			b.Fatal(err)
		}
		if _, err := cons.Next(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func newDiscardWriter() *bufioWriter { return newBufioWriter(io.Discard) }

type bufioWriter = bufio.Writer

func newBufioWriter(w io.Writer) *bufio.Writer { return bufio.NewWriterSize(w, defaultFrameMax) }
