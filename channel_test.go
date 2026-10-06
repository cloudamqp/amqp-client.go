package amqp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDial(t *testing.T) {
	conn := dialTest(t, &Config{ConnectionName: "test-dial"})
	if conn.ServerProperties()["product"] == nil {
		t.Error("expected server properties")
	}
	if conn.FrameMax() == 0 || conn.ChannelMax() == 0 {
		t.Errorf("frame max %d channel max %d", conn.FrameMax(), conn.ChannelMax())
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if !conn.IsClosed() || !errors.Is(conn.Err(), ErrClosed) {
		t.Fatalf("expected closed, got %v", conn.Err())
	}
	if _, err := conn.Channel(testContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestDialBadCredentials(t *testing.T) {
	requireBroker(t)
	u, _ := ParseURI(testURL())
	u.Password = "wrong"
	_, err := DialURI(testContext(t), u, nil)
	if !IsCode(err, AccessRefused) {
		t.Fatalf("expected access refused, got %v", err)
	}
}

func TestDialBadVhost(t *testing.T) {
	requireBroker(t)
	u, _ := ParseURI(testURL())
	u.Vhost = "does-not-exist"
	_, err := DialURI(testContext(t), u, nil)
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *Error, got %v", err)
	}
}

func TestDialContextCancelled(t *testing.T) {
	requireBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Dial(ctx, testURL(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPublishGet(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)

	ts := time.Unix(1700000000, 0)
	props := Properties{
		ContentType:     "text/plain",
		ContentEncoding: "identity",
		Headers: Table{
			"string": "s", "int": 1, "int8": int8(-8), "uint8": uint8(8), "int16": int16(-16),
			"int32": int32(-32), "int64": int64(-64), "float32": float32(1.5), "float64": 2.5,
			"bool": true, "bytes": []byte{1, 2}, "time": ts, "nested": Table{"a": "b"},
			"array": []any{"x", int32(1)}, "decimal": Decimal{2, 314}, "void": nil,
		},
		DeliveryMode:  Persistent,
		Priority:      5,
		CorrelationID: "corr",
		ReplyTo:       "reply",
		Expiration:    "60000",
		MessageID:     "msgid",
		Timestamp:     ts,
		Type:          "type",
		AppID:         "app",
		UserID:        "guest",
	}
	if err := ch.BasicPublish(ctx, "", q, Publishing{Properties: props, Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	var msg *Delivery
	var ok bool
	var err error
	for range 50 {
		if msg, ok, err = ch.BasicGet(ctx, q, false); err != nil || ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if string(msg.Body) != "hello" || msg.RoutingKey != q || msg.Exchange != "" {
		t.Fatalf("unexpected message %+v", msg)
	}
	p := msg.Properties
	if p.ContentType != props.ContentType || p.ContentEncoding != props.ContentEncoding ||
		p.DeliveryMode != props.DeliveryMode || p.Priority != props.Priority ||
		p.CorrelationID != props.CorrelationID || p.ReplyTo != props.ReplyTo ||
		p.Expiration != props.Expiration || p.MessageID != props.MessageID ||
		!p.Timestamp.Equal(ts) || p.Type != props.Type || p.AppID != props.AppID || p.UserID != props.UserID {
		t.Fatalf("properties differ: %+v", p)
	}
	h := p.Headers
	if h["string"] != "s" || h["int"] != int64(1) || h["int8"] != int8(-8) || h["bool"] != true ||
		h["float64"] != 2.5 || !bytes.Equal(h["bytes"].([]byte), []byte{1, 2}) ||
		!h["time"].(time.Time).Equal(ts) || h["nested"].(Table)["a"] != "b" ||
		h["array"].([]any)[1] != int32(1) || h["decimal"] != (Decimal{2, 314}) {
		t.Fatalf("headers differ: %#v", h)
	}
	if msg.MessageCount != 0 {
		t.Errorf("message count %d", msg.MessageCount)
	}
	if err := msg.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := msg.Ack(); !errors.Is(err, ErrAlreadyAcknowledged) {
		t.Fatalf("expected ErrAlreadyAcknowledged, got %v", err)
	}
	if _, ok, err := ch.BasicGet(ctx, q, true); ok || err != nil {
		t.Fatalf("expected empty queue, ok=%v err=%v", ok, err)
	}
}

func TestEmptyAndLargeBodies(t *testing.T) {
	conn := dialTest(t, &Config{FrameMax: 8192}) // the minimum RabbitMQ 4 accepts
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	sizes := []int{0, 1, 8184, 8185, 100_000, 1 << 21}
	if err := ch.ConfirmSelect(ctx); err != nil {
		t.Fatal(err)
	}
	for _, size := range sizes {
		body := bytes.Repeat([]byte{byte(size)}, size)
		if err := ch.BasicPublish(ctx, "", q, Publishing{Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ch.WaitForConfirms(ctx); err != nil {
		t.Fatal(err)
	}
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{NoAck: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range sizes {
		d, err := cons.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Body) != size || (size > 0 && d.Body[size-1] != byte(size)) {
			t.Fatalf("expected body of %d bytes, got %d", size, len(d.Body))
		}
	}
}

func TestConsume(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	if err := ch.BasicQos(ctx, 10, false); err != nil {
		t.Fatal(err)
	}
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{Tag: "my-consumer"})
	if err != nil {
		t.Fatal(err)
	}
	if cons.Tag() != "my-consumer" || cons.Queue() != q {
		t.Fatalf("tag %q queue %q", cons.Tag(), cons.Queue())
	}
	const n = 100
	for i := range n {
		if err := ch.BasicPublish(ctx, "", q, Publishing{Body: fmt.Append(nil, i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		d, err := cons.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(d.Body) != fmt.Sprint(i) || d.ConsumerTag != "my-consumer" {
			t.Fatalf("unexpected delivery %q %q", d.Body, d.ConsumerTag)
		}
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cons.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := cons.Next(ctx); !errors.Is(err, ErrConsumerCancelled) {
		t.Fatalf("expected ErrConsumerCancelled, got %v", err)
	}
}

func TestConsumeDeliveriesChannel(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{NoAck: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		ch.BasicPublish(ctx, "", q, Publishing{Body: fmt.Append(nil, i)})
	}
	i := 0
	for d := range cons.Deliveries() {
		if string(d.Body) != fmt.Sprint(i) {
			t.Fatalf("unexpected body %q", d.Body)
		}
		if i++; i == 10 {
			cons.Cancel(ctx)
		}
	}
	if !errors.Is(cons.Err(), ErrConsumerCancelled) {
		t.Fatalf("expected ErrConsumerCancelled, got %v", cons.Err())
	}
}

func TestConsumerCancelledByServer(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	name := randomName("test-server-cancel")
	if _, err := ch.QueueDeclare(ctx, name, QueueDeclareOptions{Durable: true}); err != nil {
		t.Fatal(err)
	}
	cons, err := ch.BasicConsume(ctx, name, ConsumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ch2 := openChannel(t, conn)
	if _, err := ch2.QueueDelete(ctx, name, QueueDeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cons.Next(ctx); !errors.Is(err, ErrConsumerCancelledByServer) {
		t.Fatalf("expected ErrConsumerCancelledByServer, got %v", err)
	}
	if ch.IsClosed() {
		t.Fatal("channel should stay open")
	}
}

func TestChannelError(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	_, err := ch.QueueDeclare(ctx, randomName("does-not-exist"), QueueDeclareOptions{Passive: true})
	if !IsCode(err, NotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	var e *Error
	if !errors.As(err, &e) || e.Connection || !e.Server || e.ClassID != classQueue {
		t.Fatalf("unexpected error %#v", err)
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatal("expected error to match ErrClosed")
	}
	<-ch.Done()
	if !IsCode(ch.Err(), NotFound) {
		t.Fatalf("expected channel error, got %v", ch.Err())
	}
	// Later requests get an error that isn't mistaken for their own failure
	if _, err := ch.QueueDeclare(ctx, "", QueueDeclareOptions{}); !errors.Is(err, ErrClosed) || IsCode(err, NotFound) {
		t.Fatalf("expected closed channel error, got %v", err)
	}
	if err := ch.BasicPublish(ctx, "", "x", Publishing{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected closed channel error, got %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatalf("close of closed channel: %v", err)
	}
	// The connection and other channels are unaffected
	ch2 := openChannel(t, conn)
	tempQueue(t, ch2)
}

func TestPipelinedRPC(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := randomName(fmt.Sprintf("test-pipelined-%d", i))
			q, err := ch.QueueDeclare(ctx, name, QueueDeclareOptions{Exclusive: true})
			if err == nil && q.Name != name {
				err = fmt.Errorf("expected %s, got %s", name, q.Name)
			}
			if err == nil {
				err = ch.QueueBind(ctx, name, "amq.topic", name, nil)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRPCContextCancel(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ch.QueueDeclare(cctx, "", QueueDeclareOptions{Exclusive: true}); !errors.Is(err, context.Canceled) {
		// Already cancelled contexts aren't checked before sending, the
		// reply may win the race; both outcomes are valid.
		if err != nil {
			t.Fatal(err)
		}
	}
	// Abandoned replies are discarded, later calls get their own reply
	ctx := testContext(t)
	for range 10 {
		cctx, cancel := context.WithTimeout(ctx, time.Microsecond)
		ch.QueueDeclare(cctx, "", QueueDeclareOptions{Exclusive: true})
		cancel()
	}
	name := randomName("test-after-cancel")
	q, err := ch.QueueDeclare(ctx, name, QueueDeclareOptions{Exclusive: true})
	if err != nil || q.Name != name {
		t.Fatalf("expected %s, got %s %v", name, q.Name, err)
	}
	// An abandoned consume is cancelled
	cctx, cancel = context.WithTimeout(ctx, time.Microsecond)
	_, err = ch.BasicConsume(cctx, name, ConsumeOptions{Tag: "abandoned"})
	cancel()
	if err == nil {
		t.Skip("consume-ok arrived before the context timed out")
	}
	time.Sleep(50 * time.Millisecond)
	if q, err := ch.QueueDeclare(ctx, name, QueueDeclareOptions{Passive: true}); err != nil || q.ConsumerCount != 0 {
		t.Fatalf("expected no consumers, got %d %v", q.ConsumerCount, err)
	}
}

func TestPublishConfirm(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	confs := make([]*Confirmation, 0, 1000)
	for range 1000 {
		c, err := ch.BasicPublishConfirm(ctx, "", q, Publishing{Body: []byte("x")})
		if err != nil {
			t.Fatal(err)
		}
		confs = append(confs, c)
	}
	for i, c := range confs {
		if c.DeliveryTag() != uint64(i+1) {
			t.Fatalf("expected tag %d, got %d", i+1, c.DeliveryTag())
		}
		if err := c.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		if !c.Acked() {
			t.Fatal("expected acked")
		}
	}
	if err := ch.WaitForConfirms(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := ch.QueueDeclare(ctx, q, QueueDeclareOptions{Passive: true})
	if err != nil || info.MessageCount != 1000 {
		t.Fatalf("expected 1000 messages, got %d %v", info.MessageCount, err)
	}
}

func TestPublishConfirmConcurrent(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	var wg sync.WaitGroup
	var failed atomic.Int32
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				c, err := ch.BasicPublishConfirm(ctx, "", q, Publishing{Body: []byte("x")})
				if err == nil {
					err = c.Wait(ctx)
				}
				if err != nil {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d publishes failed", failed.Load())
	}
}

func TestWaitForConfirmsNotInConfirmMode(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	if err := ch.WaitForConfirms(testContext(t)); !errors.Is(err, ErrNoConfirmMode) {
		t.Fatalf("expected ErrNoConfirmMode, got %v", err)
	}
}

func TestConfirmsFailOnChannelClose(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	// Publishing to a non-existing exchange closes the channel
	c, err := ch.BasicPublishConfirm(ctx, randomName("no-such-exchange"), "", Publishing{Body: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	// The confirmation doesn't claim the error as its own, another message
	// could have caused it
	if err := c.Wait(ctx); !errors.Is(err, ErrUnconfirmed) || !errors.Is(err, ErrClosed) || IsCode(err, NotFound) {
		t.Fatalf("expected ErrUnconfirmed, got %v", err)
	}
	if !IsCode(ch.Err(), NotFound) {
		t.Fatalf("expected the channel to be closed with not found, got %v", ch.Err())
	}
}

func TestWaitForConfirmsChannelClosed(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	if err := ch.ConfirmSelect(ctx); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		ch.BasicPublish(ctx, randomName("no-such-exchange"), "", Publishing{Body: []byte("x")})
	}
	<-ch.Done()
	if err := ch.WaitForConfirms(ctx); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("expected ErrUnconfirmed, got %v", err)
	}
}

func TestMandatoryReturn(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	var returned atomic.Pointer[Return]
	ch.OnReturn(func(r *Return) { returned.Store(r) })
	c, err := ch.BasicPublishConfirm(ctx, "amq.direct", "unroutable", Publishing{
		Mandatory: true, Body: []byte("ret"), Properties: Properties{MessageID: "m1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	r := returned.Load()
	if r == nil {
		t.Fatal("expected the return to be processed before the confirm")
	}
	if r.ReplyCode != NoRoute || string(r.Body) != "ret" || r.MessageID != "m1" || r.RoutingKey != "unroutable" {
		t.Fatalf("unexpected return %+v", r)
	}
}

func TestExchanges(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	src, dst := randomName("test-src"), randomName("test-dst")
	if err := ch.ExchangeDeclare(ctx, src, Topic, ExchangeDeclareOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDeclare(ctx, dst, Fanout, ExchangeDeclareOptions{AutoDelete: true}); err != nil {
		t.Fatal(err)
	}
	defer ch.ExchangeDelete(ctx, src, false)
	defer ch.ExchangeDelete(ctx, dst, false)
	if err := ch.ExchangeBind(ctx, dst, src, "a.*", nil); err != nil {
		t.Fatal(err)
	}
	q := tempQueue(t, ch)
	if err := ch.QueueBind(ctx, q, dst, "", nil); err != nil {
		t.Fatal(err)
	}
	ch.ConfirmSelect(ctx)
	ch.BasicPublish(ctx, src, "a.b", Publishing{Body: []byte("1")})
	ch.BasicPublish(ctx, src, "b.b", Publishing{Body: []byte("2")})
	if err := ch.WaitForConfirms(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := ch.QueuePurge(ctx, q); err != nil || n != 1 {
		t.Fatalf("expected 1 message, got %d %v", n, err)
	}
	if err := ch.ExchangeUnbind(ctx, dst, src, "a.*", nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueUnbind(ctx, q, dst, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDeclare(ctx, src, Topic, ExchangeDeclareOptions{Passive: true}); err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDelete(ctx, src, false); err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDeclare(ctx, src, Topic, ExchangeDeclareOptions{Passive: true}); !IsCode(err, NotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestQueueDelete(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	name := randomName("test-delete")
	if _, err := ch.QueueDeclare(ctx, name, QueueDeclareOptions{Durable: true}); err != nil {
		t.Fatal(err)
	}
	ch.ConfirmSelect(ctx)
	for range 3 {
		ch.BasicPublish(ctx, "", name, Publishing{Body: []byte("x")})
	}
	ch.WaitForConfirms(ctx)
	if n, err := ch.QueueDelete(ctx, name, QueueDeleteOptions{}); err != nil || n != 3 {
		t.Fatalf("expected 3 messages, got %d %v", n, err)
	}
}

func TestTx(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	if err := ch.TxSelect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.BasicPublish(ctx, "", q, Publishing{Body: []byte("rolled back")})
	if err := ch.TxRollback(ctx); err != nil {
		t.Fatal(err)
	}
	ch.BasicPublish(ctx, "", q, Publishing{Body: []byte("committed")})
	if err := ch.TxCommit(ctx); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := ch.BasicGet(ctx, q, true)
	if err != nil || !ok || string(msg.Body) != "committed" {
		t.Fatalf("unexpected %v %v %v", msg, ok, err)
	}
	if _, ok, _ := ch.BasicGet(ctx, q, true); ok {
		t.Fatal("expected empty queue")
	}
}

func TestNackRequeue(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	ch.BasicPublish(ctx, "", q, Publishing{Body: []byte("x")})
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d, err := cons.Next(ctx)
	if err != nil || d.Redelivered {
		t.Fatal(err, d.Redelivered)
	}
	d.Nack(true)
	d, err = cons.Next(ctx)
	if err != nil || !d.Redelivered {
		t.Fatal(err, d.Redelivered)
	}
	d.Reject(false)
}

func TestChannelClose(t *testing.T) {
	conn := dialTest(t, nil)
	ctx := testContext(t)
	ch := openChannel(t, conn)
	q := tempQueue(t, ch)
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ch.Err(), ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", ch.Err())
	}
	if _, err := cons.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	// Channel ids are reused
	for range int(conn.ChannelMax()) + 10 {
		ch, err := conn.Channel(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ch.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConnectionLost(t *testing.T) {
	p := newProxy(t)
	conn, err := Dial(testContext(t), p.url(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := testContext(t)
	ch, err := conn.Channel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	q := tempQueue(t, ch)
	cons, err := ch.BasicConsume(ctx, q, ConsumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p.cut()
	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection not closed")
	}
	if !errors.Is(conn.Err(), ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", conn.Err())
	}
	if _, err := cons.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := ch.QueueDeclare(ctx, "", QueueDeclareOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	conn := dialTest(t, &Config{Heartbeat: time.Second})
	if conn.Heartbeat() != time.Second {
		t.Fatalf("expected 1s heartbeat, got %v", conn.Heartbeat())
	}
	time.Sleep(3500 * time.Millisecond)
	if conn.IsClosed() {
		t.Fatalf("connection closed: %v", conn.Err())
	}
}

func TestHeartbeatTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	// A proxy that stops forwarding simulates a dead broker
	requireBroker(t)
	u, _ := ParseURI(testURL())
	stall := make(chan struct{})
	cfg := &Config{Heartbeat: time.Second, Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, network, u.Addr())
		if err != nil {
			return nil, err
		}
		return &stallConn{Conn: c, stall: stall}, nil
	}}
	conn := dialTest(t, cfg)
	close(stall)
	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("expected heartbeat timeout")
	}
	var ne net.Error
	if !errors.As(conn.Err(), &ne) || !ne.Timeout() {
		t.Fatalf("expected timeout, got %v", conn.Err())
	}
}

// stallConn discards all data read once stall is closed.
type stallConn struct {
	net.Conn
	stall chan struct{}
}

func (c *stallConn) Read(p []byte) (int, error) {
	for {
		n, err := c.Conn.Read(p)
		select {
		case <-c.stall:
			if err != nil {
				return 0, err
			}
			continue // discard
		default:
			return n, err
		}
	}
}

func TestBlockedPublishWaitsForContext(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	// Simulate connection.blocked from the broker
	conn.handleConnectionMethod(connectionBlocked, decoder{b: []byte{4, 't', 'e', 's', 't'}})
	if !conn.Blocked() {
		t.Fatal("expected blocked")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := ch.BasicPublish(ctx, "", "x", Publishing{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	done := make(chan error)
	go func() { done <- ch.BasicPublish(testContext(t), "", "x", Publishing{}) }()
	time.Sleep(10 * time.Millisecond)
	conn.handleConnectionMethod(connectionUnblocked, decoder{})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Issue #2
func TestChannelCloseKeepsConfirms(t *testing.T) {
	conn := dialTest(t, nil)
	ch := openChannel(t, conn)
	ctx := testContext(t)
	q := tempQueue(t, ch)
	const n = 5000
	confs := make([]*Confirmation, 0, n)
	for range n {
		c, err := ch.BasicPublishConfirm(ctx, "", q, Publishing{Body: []byte("x")})
		if err != nil {
			t.Fatal(err)
		}
		confs = append(confs, c)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	acked := 0
	for _, c := range confs {
		err := c.Wait(ctx)
		switch {
		case err == nil:
			acked++
		case !errors.Is(err, ErrUnconfirmed):
			t.Fatalf("unexpected error %v", err)
		}
	}
	info, err := openChannel(t, conn).QueueDeclare(ctx, q, QueueDeclareOptions{Passive: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d acked, %d in the queue", acked, info.MessageCount)
	if acked == 0 || uint32(acked) > info.MessageCount {
		t.Fatalf("%d acked but %d in the queue", acked, info.MessageCount)
	}
}
