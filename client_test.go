package amqp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t testing.TB, url string, opts *ClientOptions) *Client {
	t.Helper()
	requireBroker(t)
	if opts == nil {
		opts = &ClientOptions{}
	}
	if opts.ReconnectInterval == 0 {
		opts.ReconnectInterval = 50 * time.Millisecond
	}
	c, err := NewClient(testContext(t), url, opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// collect returns a handler that sends message bodies to a channel.
func collect(n int) (Handler, chan string) {
	bodies := make(chan string, n)
	return func(ctx context.Context, d *Delivery) error {
		bodies <- string(d.Body)
		return nil
	}, bodies
}

func receive(t testing.TB, bodies chan string, n int) []string {
	t.Helper()
	var got []string
	for range n {
		select {
		case b := <-bodies:
			got = append(got, b)
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout after receiving %d of %d messages", len(got), n)
		}
	}
	return got
}

func TestClientPublishSubscribe(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	// Not auto-delete, so that the queue remains after the subscription
	q, err := c.Queue(ctx, "", &QueueOptions{Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(q.Name(), "amq.gen-") {
		t.Fatalf("expected a server-named queue, got %q", q.Name())
	}
	handler, bodies := collect(10)
	sub, err := q.Subscribe(ctx, handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := q.Publish(ctx, Message{Body: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	got := receive(t, bodies, 10)
	for i, b := range got {
		if b != fmt.Sprint(i) {
			t.Fatalf("expected %d, got %s", i, b)
		}
	}
	if err := sub.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	<-sub.Done()
	if !errors.Is(sub.Err(), ErrConsumerCancelled) {
		t.Fatalf("expected ErrConsumerCancelled, got %v", sub.Err())
	}
	// All messages were acked
	if _, ok, err := q.Get(ctx, true); ok || err != nil {
		t.Fatalf("expected empty queue, got ok=%v err=%v", ok, err)
	}
}

func TestClientDurableQueue(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	name := randomName("test-durable")
	q, err := c.Queue(ctx, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Delete(ctx, QueueDeleteOptions{})
	if err := q.Publish(ctx, Message{Body: "persistent"}); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := q.Get(ctx, false)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if msg.DeliveryMode != Persistent {
		t.Fatalf("expected persistent message, got %d", msg.DeliveryMode)
	}
	msg.Ack()
	// Redeclaring returns the same handle
	q2, err := c.Queue(ctx, name, nil)
	if err != nil || q2 != q {
		t.Fatalf("expected the same handle, got %v %v", q2, err)
	}
	// A passive declare of a missing queue fails, the client recovers
	if _, err := c.Queue(ctx, randomName("missing"), &QueueOptions{Passive: true}); !IsCode(err, NotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := q.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := q.Delete(ctx, QueueDeleteOptions{}); err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

func TestClientHandlerErrorRequeues(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	done := make(chan bool, 1)
	_, err = q.Subscribe(ctx, func(ctx context.Context, d *Delivery) error {
		switch attempts.Add(1) {
		case 1:
			return errors.New("failed")
		case 2:
			panic("panicked")
		default:
			done <- d.Redelivered
			return nil
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, Message{Body: "retry me"}); err != nil {
		t.Fatal(err)
	}
	select {
	case redelivered := <-done:
		if !redelivered {
			t.Fatal("expected a redelivery")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestClientManualAck(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	_, err = q.Subscribe(ctx, func(ctx context.Context, d *Delivery) error {
		calls.Add(1)
		d.Reject(false) // drop it, the error doesn't requeue it
		return errors.New("rejected")
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	q.Publish(ctx, Message{Body: "drop me"})
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}
}

func TestClientWorkers(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var running, maxRunning atomic.Int32
	handler, bodies := collect(20)
	_, err = q.Subscribe(ctx, func(ctx context.Context, d *Delivery) error {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		return handler(ctx, d)
	}, &SubscribeOptions{Workers: 5})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := q.Publish(ctx, Message{Body: fmt.Sprint(i)}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	receive(t, bodies, 20)
	if maxRunning.Load() < 2 {
		t.Fatalf("expected concurrent handlers, max was %d", maxRunning.Load())
	}
}

type order struct {
	ID    int      `json:"id"`
	Items []string `json:"items"`
}

func TestClientCodecs(t *testing.T) {
	c := newTestClient(t, testURL(), &ClientOptions{
		Config:                 Config{Codecs: DefaultCodecs()},
		DefaultContentType:     "application/json",
		DefaultContentEncoding: "gzip",
	})
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	in := order{ID: 42, Items: []string{"a", "b"}}
	if err := q.Publish(ctx, Message{Body: in}); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := q.Get(ctx, true)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if msg.ContentType != "application/json" || msg.ContentEncoding != "gzip" {
		t.Fatalf("unexpected properties %+v", msg.Properties)
	}
	var out order
	if err := msg.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.ID != 42 || len(out.Items) != 2 {
		t.Fatalf("unexpected %+v", out)
	}
	var raw string
	if err := msg.Decode(&raw); err != nil || raw != `{"id":42,"items":["a","b"]}` {
		t.Fatalf("unexpected %q %v", raw, err)
	}
	// Unknown encodings are rejected
	err = q.Publish(ctx, Message{Body: "x", Properties: Properties{ContentEncoding: "br"}})
	if !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("expected ErrUnsupportedContentEncoding, got %v", err)
	}
}

func TestClientWithoutCodecs(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, Message{Body: order{}}); err == nil {
		t.Fatal("expected an error publishing a struct without codecs")
	}
}

func TestClientExchanges(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	x, err := c.TopicExchange(ctx, randomName("test-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer x.Delete(ctx, false)
	src, err := c.Exchange(ctx, randomName("test-src"), Fanout, &ExchangeOptions{AutoDelete: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Bind(ctx, src.Name(), "", nil); err != nil {
		t.Fatal(err)
	}
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Bind(ctx, x.Name(), "events.*", nil); err != nil {
		t.Fatal(err)
	}
	handler, bodies := collect(2)
	if _, err := q.Subscribe(ctx, handler, nil); err != nil {
		t.Fatal(err)
	}
	if err := x.Publish(ctx, "events.created", Message{Body: "direct"}); err != nil {
		t.Fatal(err)
	}
	if err := src.Publish(ctx, "events.updated", Message{Body: "via source"}); err != nil {
		t.Fatal(err)
	}
	if err := x.Publish(ctx, "other", Message{Body: "unrouted"}); err != nil {
		t.Fatal(err)
	}
	got := receive(t, bodies, 2)
	if got[0] != "direct" || got[1] != "via source" {
		t.Fatalf("unexpected %v", got)
	}
	select {
	case b := <-bodies:
		t.Fatalf("unexpected message %q", b)
	case <-time.After(100 * time.Millisecond):
	}
	if err := q.Unbind(ctx, x.Name(), "events.*", nil); err != nil {
		t.Fatal(err)
	}
	if err := x.Unbind(ctx, src.Name(), "", nil); err != nil {
		t.Fatal(err)
	}
	// Built-in exchanges are only checked
	if _, err := c.TopicExchange(ctx, "amq.topic"); err != nil {
		t.Fatal(err)
	}
	if c.DefaultExchange().Name() != "" {
		t.Fatal("expected the default exchange")
	}
}

func TestClientPublishToMissingExchange(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	err := c.Publish(ctx, randomName("missing"), "", Message{Body: "x"})
	if !IsCode(err, NotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	// The publish channel is reopened
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, Message{Body: "x"}); err != nil {
		t.Fatal(err)
	}
}

func TestClientMandatoryReturn(t *testing.T) {
	returns := make(chan *Return, 1)
	c := newTestClient(t, testURL(), &ClientOptions{OnReturn: func(r *Return) { returns <- r }})
	ctx := testContext(t)
	if err := c.Publish(ctx, "amq.direct", "nowhere", Message{Body: "x", Mandatory: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-returns:
		if r.ReplyCode != NoRoute {
			t.Fatalf("unexpected return %+v", r)
		}
	default:
		t.Fatal("expected the return before the confirm")
	}
}

func TestClientReconnect(t *testing.T) {
	p := newProxy(t)
	var connects, disconnects atomic.Int32
	c := newTestClient(t, p.url(), &ClientOptions{
		OnConnect:    func(ctx context.Context, c *Client) error { connects.Add(1); return nil },
		OnDisconnect: func(err error) { disconnects.Add(1) },
	})
	ctx := testContext(t)
	x, err := c.Exchange(ctx, randomName("test-reconnect"), Topic, &ExchangeOptions{AutoDelete: true})
	if err != nil {
		t.Fatal(err)
	}
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Bind(ctx, x.Name(), "#", nil); err != nil {
		t.Fatal(err)
	}
	handler, bodies := collect(100)
	sub, err := q.Subscribe(ctx, handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Publish(ctx, "a", Message{Body: "before"}); err != nil {
		t.Fatal(err)
	}
	receive(t, bodies, 1)
	oldName := q.Name()

	p.cut()
	// Publishing waits for the reconnect, topology and the subscription
	// are recovered
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := x.Publish(ctx, "a", Message{Body: "after"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
	if got := receive(t, bodies, 1); got[0] != "after" {
		t.Fatalf("unexpected %v", got)
	}
	if q.Name() == oldName {
		t.Fatal("expected a new server-named queue")
	}
	if connects.Load() != 2 || disconnects.Load() != 1 {
		t.Fatalf("expected 2 connects and 1 disconnect, got %d and %d", connects.Load(), disconnects.Load())
	}
	if sub.Err() != nil {
		t.Fatal(sub.Err())
	}
}

func TestClientMaxRetries(t *testing.T) {
	p := newProxy(t)
	failed := make(chan error, 1)
	c := newTestClient(t, p.url(), &ClientOptions{
		MaxRetries: 2,
		OnFailed:   func(err error) { failed <- err },
	})
	p.ln.Close()
	p.cut()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the client to give up")
	}
	<-c.Done()
	if c.Err() == nil {
		t.Fatal("expected an error")
	}
	if _, err := c.Queue(testContext(t), "", nil); err == nil {
		t.Fatal("expected an error after giving up")
	}
}

func TestClientOperationsWaitForReconnect(t *testing.T) {
	p := newProxy(t)
	c := newTestClient(t, p.url(), nil)
	p.cut()
	if _, err := c.Queue(testContext(t), "", nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientClose(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	finished := make(chan struct{})
	sub, err := q.Subscribe(ctx, func(ctx context.Context, d *Delivery) error {
		close(started)
		<-ctx.Done() // the context is cancelled when the client closes
		close(finished)
		return ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	q.Publish(ctx, Message{Body: "x"})
	<-started
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("expected Close to wait for the handler")
	}
	<-sub.Done()
	if !errors.Is(c.Err(), ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", c.Err())
	}
	if err := c.Publish(ctx, "", "x", Message{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestSubscriptionStopsWhenQueueDeleted(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, randomName("test-deleted"), &QueueOptions{AutoDelete: false})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := q.Subscribe(ctx, func(ctx context.Context, d *Delivery) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Delete(ctx, QueueDeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("expected the subscription to stop")
	}
	if !IsCode(sub.Err(), NotFound) {
		t.Fatalf("expected not found, got %v", sub.Err())
	}
}

func TestSubscriptionRecoversFromChannelError(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", &QueueOptions{Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	handler, bodies := collect(10)
	var once sync.Once
	_, err = q.Subscribe(ctx, func(ctx context.Context, d *Delivery) error {
		once.Do(func() {
			// Acking an unknown delivery tag makes the broker close the
			// channel. Wait for that, as LavinMQ closes the connection if
			// it gets more frames on a channel it has closed.
			d.Channel().BasicAck(d.DeliveryTag+1000, false)
			<-d.Channel().Done()
		})
		return handler(ctx, d)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		q.Publish(ctx, Message{Body: fmt.Sprint(i)})
	}
	// The first message might be delivered twice, as its ack is lost
	seen := map[string]bool{}
	for len(seen) < 3 {
		seen[receive(t, bodies, 1)[0]] = true
	}
}

func TestClientRPC(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	queue := randomName("test-rpc")
	srv, err := c.RPCServer(ctx, queue, func(ctx context.Context, req *Delivery) (Message, error) {
		if string(req.Body) == "fail" {
			return Message{}, errors.New("failed")
		}
		return Message{Body: "re: " + string(req.Body)}, nil
	}, &SubscribeOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer c.withChannel(ctx, func(ch *Channel) error { _, err := ch.QueueDelete(ctx, queue, QueueDeleteOptions{}); return err })
	defer srv.Cancel(ctx)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := c.RPCCall(ctx, queue, Message{Body: fmt.Sprint(i), Properties: Properties{ContentType: "text/plain"}})
			if err != nil {
				t.Error(err)
				return
			}
			if string(reply.Body) != fmt.Sprintf("re: %d", i) || reply.ContentType != "text/plain" {
				t.Errorf("unexpected reply %q %q", reply.Body, reply.ContentType)
			}
		}()
	}
	wg.Wait()

	tctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := c.RPCCall(tctx, queue, Message{Body: "fail"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if _, err := c.RPCCall(ctx, randomName("no-server"), Message{Body: "x"}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("expected ErrUnroutable, got %v", err)
	}
}

func TestRPCClientReconnect(t *testing.T) {
	p := newProxy(t)
	c := newTestClient(t, p.url(), nil)
	ctx := testContext(t)
	queue := randomName("test-rpc-reconnect")
	_, err := c.RPCServer(ctx, queue, func(ctx context.Context, req *Delivery) (Message, error) {
		return Message{Body: req.Body}, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.withChannel(ctx, func(ch *Channel) error { _, err := ch.QueueDelete(ctx, queue, QueueDeleteOptions{}); return err })
	rpc, err := c.RPCClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	if _, err := rpc.Call(ctx, queue, Message{Body: "1"}); err != nil {
		t.Fatal(err)
	}
	p.cut()
	deadline := time.Now().Add(10 * time.Second)
	for {
		cctx, cancel := context.WithTimeout(ctx, time.Second)
		reply, err := rpc.Call(cctx, queue, Message{Body: "2"})
		cancel()
		if err == nil {
			if string(reply.Body) != "2" {
				t.Fatalf("unexpected reply %q", reply.Body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
}

func TestSubscriptionCancelFromHandler(t *testing.T) {
	c := newTestClient(t, testURL(), nil)
	ctx := testContext(t)
	q, err := c.Queue(ctx, "", &QueueOptions{Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	var sub *Subscription
	ready := make(chan struct{})
	var handled atomic.Int32
	sub, err = q.Subscribe(ctx, func(hctx context.Context, d *Delivery) error {
		<-ready
		if handled.Add(1) == 3 {
			sub.Cancel(hctx) // must not deadlock
		}
		return nil
	}, &SubscribeOptions{Prefetch: 1})
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	for i := range 5 {
		q.Publish(ctx, Message{Body: fmt.Sprint(i)})
	}
	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("expected the subscription to stop")
	}
	// The third message was acked, the rest are left in the queue
	time.Sleep(50 * time.Millisecond)
	n, err := q.Purge(ctx)
	if err != nil || n != 2 {
		t.Fatalf("expected 2 messages left, got %d %v", n, err)
	}
}
