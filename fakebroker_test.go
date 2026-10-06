package amqp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

// fakeBroker is a scripted broker for testing protocol edge cases that a
// real broker doesn't produce.
type fakeBroker struct {
	t    testing.TB
	conn net.Conn
	br   *bufio.Reader
	quit chan struct{} // closed when the test ends
}

type frame struct {
	typ     byte
	channel uint16
	payload []byte
}

func (f frame) method() uint32 { return be.Uint32(f.payload) }

// dialFake connects a client to a fake broker, script runs the broker side
// after the handshake.
func dialFake(t testing.TB, cfg *Config, tune func(b []byte) []byte, script func(f *fakeBroker)) (*Connection, error) {
	t.Helper()
	client, server := net.Pipe()
	f := &fakeBroker{t: t, conn: server, br: bufio.NewReader(server), quit: make(chan struct{})}
	done := make(chan struct{})
	t.Cleanup(func() { close(f.quit); server.Close(); <-done })
	go func() {
		defer close(done)
		if err := f.handshake(tune); err != nil {
			return
		}
		if script != nil {
			script(f)
		}
	}()
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) { return client, nil }
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return Dial(ctx, "amqp://localhost", cfg)
}

func (f *fakeBroker) read() (frame, error) {
	hdr := make([]byte, 7)
	if _, err := io.ReadFull(f.br, hdr); err != nil {
		return frame{}, err
	}
	payload := make([]byte, be.Uint32(hdr[3:])+1)
	if _, err := io.ReadFull(f.br, payload); err != nil {
		return frame{}, err
	}
	return frame{hdr[0], be.Uint16(hdr[1:]), payload[:len(payload)-1]}, nil
}

// expect reads frames until a method frame, which must be cm.
func (f *fakeBroker) expect(cm uint32) frame {
	for {
		fr, err := f.read()
		if err != nil {
			f.t.Errorf("fake broker: expected %s: %v", methodName(cm), err)
			return frame{}
		}
		if fr.typ == frameHeartbeat {
			continue
		}
		if fr.typ != frameMethod || fr.method() != cm {
			f.t.Errorf("fake broker: expected %s, got frame type %d %x", methodName(cm), fr.typ, fr.payload)
		}
		return fr
	}
}

func (f *fakeBroker) send(typ byte, channel uint16, payload []byte) {
	b, start := beginFrame(nil, typ, channel)
	b = append(b, payload...)
	f.conn.Write(endFrame(b, start))
}

func (f *fakeBroker) method(channel uint16, cm uint32, args []byte) {
	f.send(frameMethod, channel, append(be.AppendUint32(nil, cm), args...))
}

func (f *fakeBroker) handshake(tune func(b []byte) []byte) error {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(f.br, hdr); err != nil {
		return err
	}
	args := []byte{0, 9}
	args, _ = appendTable(args, Table{"product": "fake"})
	args = appendLongStr(args, "PLAIN AMQPLAIN")
	args = appendLongStr(args, "en_US")
	f.method(0, connectionStart, args)
	f.expect(connectionStartOk)
	if tune == nil {
		tune = func(b []byte) []byte {
			b = be.AppendUint16(b, 16)
			b = be.AppendUint32(b, 4096)
			return be.AppendUint16(b, 0)
		}
	}
	f.method(0, connectionTune, tune(nil))
	f.expect(connectionTuneOk)
	f.expect(connectionOpen)
	f.method(0, connectionOpenOk, []byte{0})
	return nil
}

// openChannel answers a channel.open.
func (f *fakeBroker) openChannel() uint16 {
	fr := f.expect(channelOpen)
	f.method(fr.channel, channelOpenOk, be.AppendUint32(nil, 0))
	return fr.channel
}

func waitDone(t testing.TB, conn *Connection) error {
	t.Helper()
	select {
	case <-conn.Done():
		return conn.Err()
	case <-time.After(5 * time.Second):
		t.Fatal("expected the connection to close")
		return nil
	}
}

func TestFakeTuneNegotiation(t *testing.T) {
	conn, err := dialFake(t, &Config{ChannelMax: 100, FrameMax: 8192, Heartbeat: -1}, func(b []byte) []byte {
		b = be.AppendUint16(b, 0)     // no channel limit
		b = be.AppendUint32(b, 1<<20) // larger than the client's
		return be.AppendUint16(b, 60) // heartbeat disabled by the client
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if conn.ChannelMax() != 100 || conn.FrameMax() != 8192 || conn.Heartbeat() != 0 {
		t.Fatalf("negotiated %d %d %v", conn.ChannelMax(), conn.FrameMax(), conn.Heartbeat())
	}
	if conn.ServerProperties()["product"] != "fake" {
		t.Fatal("expected server properties")
	}
}

func TestFakeHandshakeRefused(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	go func() {
		f := &fakeBroker{t: t, conn: server, br: bufio.NewReader(server)}
		io.ReadFull(f.br, make([]byte, 8))
		args := []byte{0, 9}
		args, _ = appendTable(args, Table{})
		args = appendLongStr(args, "PLAIN")
		args = appendLongStr(args, "en_US")
		f.method(0, connectionStart, args)
		f.expect(connectionStartOk)
		close := be.AppendUint16(nil, AccessRefused)
		close = appendShortStr(close, "ACCESS_REFUSED - Login was refused")
		close = append(close, 0, 0, 0, 0)
		f.method(0, connectionClose, close)
		f.expect(connectionCloseOk)
	}()
	_, err := Dial(context.Background(), "amqp://localhost", &Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) { return client, nil },
	})
	var e *Error
	if !errors.As(err, &e) || e.Code != AccessRefused || !e.Connection || !e.Server {
		t.Fatalf("expected access refused, got %#v", err)
	}
}

func TestFakeUnsupportedMechanism(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	go func() {
		f := &fakeBroker{t: t, conn: server, br: bufio.NewReader(server)}
		io.ReadFull(f.br, make([]byte, 8))
		args := []byte{0, 9}
		args, _ = appendTable(args, Table{})
		args = appendLongStr(args, "AMQPLAIN")
		args = appendLongStr(args, "en_US")
		f.method(0, connectionStart, args)
		io.Copy(io.Discard, server)
	}()
	_, err := Dial(context.Background(), "amqp://localhost", &Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) { return client, nil },
	})
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestFakeServerClosesConnection(t *testing.T) {
	gotCloseOk := make(chan bool, 1)
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		args := be.AppendUint16(nil, ConnectionForced)
		args = appendShortStr(args, "CONNECTION_FORCED - shutdown")
		args = append(args, 0, 0, 0, 0)
		f.method(0, connectionClose, args)
		fr, err := f.read()
		gotCloseOk <- err == nil && fr.method() == connectionCloseOk
	})
	if err != nil {
		t.Fatal(err)
	}
	err = waitDone(t, conn)
	if !IsCode(err, ConnectionForced) {
		t.Fatalf("expected connection forced, got %v", err)
	}
	if !<-gotCloseOk {
		t.Fatal("expected close-ok")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close after server close: %v", err)
	}
}

func TestFakeFrameTooLarge(t *testing.T) {
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		f.send(frameBody, 1, make([]byte, 5000))
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := waitDone(t, conn); !IsCode(err, FrameError) {
		t.Fatalf("expected frame error, got %v", err)
	}
}

func TestFakeMissingFrameEnd(t *testing.T) {
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		f.conn.Write([]byte{frameHeartbeat, 0, 0, 0, 0, 0, 0, 0})
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := waitDone(t, conn); !IsCode(err, FrameError) {
		t.Fatalf("expected frame error, got %v", err)
	}
}

func TestFakeUnexpectedReply(t *testing.T) {
	closeCode := make(chan uint16, 1)
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		f.method(ch, queueBindOk, nil) // nobody asked
		fr := f.expect(connectionClose)
		closeCode <- be.Uint16(fr.payload[4:])
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Channel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := waitDone(t, conn); !IsCode(err, UnexpectedFrame) {
		t.Fatalf("expected unexpected frame, got %v", err)
	}
	if code := <-closeCode; code != UnexpectedFrame {
		t.Fatalf("expected the client to close with 505, got %d", code)
	}
}

func TestFakeContentWithoutMethod(t *testing.T) {
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		f.send(frameBody, ch, []byte("orphan"))
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	conn.Channel(context.Background())
	if err := waitDone(t, conn); !IsCode(err, UnexpectedFrame) {
		t.Fatalf("expected unexpected frame, got %v", err)
	}
}

func TestFakeChannelFlowAndBlocked(t *testing.T) {
	blocked := make(chan string, 1)
	unblocked := make(chan struct{}, 1)
	flowOk := make(chan bool, 1)
	conn, err := dialFake(t, &Config{
		OnBlocked:   func(reason string) { blocked <- reason },
		OnUnblocked: func() { unblocked <- struct{}{} },
	}, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		f.method(ch, channelFlow, []byte{0})
		fr := f.expect(channelFlowOk)
		flowOk <- fr.payload[4] == 0
		f.method(0, connectionBlocked, appendShortStr(nil, "low memory"))
		f.method(0, connectionUnblocked, nil)
		f.expect(connectionUpdateSecret)
		f.method(0, connectionUpdateSecretOk, nil)
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Channel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !<-flowOk {
		t.Fatal("expected flow-ok with active=false")
	}
	if reason := <-blocked; reason != "low memory" {
		t.Fatalf("unexpected reason %q", reason)
	}
	<-unblocked
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.UpdateSecret(ctx, "new-token", "refresh"); err != nil {
		t.Fatal(err)
	}
}

func TestFakeDeliveryForUnknownConsumer(t *testing.T) {
	got := make(chan *Delivery, 1)
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		deliver := func(tag string, body string) {
			args := appendShortStr(nil, tag)
			args = be.AppendUint64(args, 1)
			args = append(args, 0)
			args = appendShortStr(args, "")
			args = appendShortStr(args, "q")
			f.method(ch, basicDeliver, args)
			hdr := be.AppendUint16(nil, classBasic)
			hdr = append(hdr, 0, 0)
			hdr = be.AppendUint64(hdr, uint64(len(body)))
			hdr = be.AppendUint16(hdr, 0)
			f.send(frameHeader, ch, hdr)
			f.send(frameBody, ch, []byte(body))
		}
		deliver("unknown", "dropped")
		f.expect(basicConsume)
		f.method(ch, basicConsumeOk, appendShortStr(nil, "known"))
		deliver("known", "kept")
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cons, err := ch.BasicConsume(context.Background(), "q", ConsumeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		d, _ := cons.Next(context.Background())
		got <- d
	}()
	select {
	case d := <-got:
		if string(d.Body) != "kept" {
			t.Fatalf("unexpected %q", d.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// A peer that stops reading must not prevent the connection from being
// torn down when heartbeats time out.
func TestFakeStalledPeer(t *testing.T) {
	conn, err := dialFake(t, nil, func(b []byte) []byte {
		b = be.AppendUint16(b, 16)
		b = be.AppendUint32(b, 131072)
		return be.AppendUint16(b, 1) // 1s heartbeat
	}, func(f *fakeBroker) {
		f.openChannel()
		<-f.quit // stop reading
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Fill the pipe, the flusher blocks in a write
	go func() {
		body := make([]byte, 100_000)
		for ch.BasicPublish(context.Background(), "", "q", Publishing{Body: body}) == nil {
		}
	}()
	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("expected the connection to time out")
	}
	closed := make(chan struct{})
	go func() { conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close blocked")
	}
}

func TestFakeAbandonedConsume(t *testing.T) {
	consumeSent := make(chan struct{})
	consumeOkSent := make(chan struct{})
	nacked := make(chan bool, 1)
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		f.expect(basicConsume)
		close(consumeSent)
		<-consumeOkSent // after the client gave up
		f.method(ch, basicConsumeOk, appendShortStr(nil, "t"))
		args := appendShortStr(nil, "t")
		args = be.AppendUint64(args, 7)
		args = append(args, 0)
		args = appendShortStr(args, "")
		args = appendShortStr(args, "q")
		f.method(ch, basicDeliver, args)
		hdr := be.AppendUint16(nil, classBasic)
		hdr = append(hdr, 0, 0)
		hdr = be.AppendUint64(hdr, 0)
		hdr = be.AppendUint16(hdr, 0)
		f.send(frameHeader, ch, hdr)
		var gotNack, gotCancel bool
		for !gotNack || !gotCancel {
			fr, err := f.read()
			if err != nil {
				return
			}
			switch fr.method() {
			case basicCancel:
				gotCancel = true
			case basicNack:
				gotNack = be.Uint64(fr.payload[4:]) == 7 && fr.payload[12]&2 != 0
			}
		}
		f.method(ch, basicCancelOk, appendShortStr(nil, "t"))
		nacked <- gotNack
		f.expect(basicQos)
		f.method(ch, basicQosOk, nil)
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-consumeSent; cancel(); time.Sleep(20 * time.Millisecond); close(consumeOkSent) }()
	if _, err := ch.BasicConsume(ctx, "q", ConsumeOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if !<-nacked {
		t.Fatal("expected the delivery to the abandoned consumer to be requeued")
	}
	// The cancel-ok didn't confuse the next request
	if err := ch.BasicQos(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
}

func TestFakeDiscardedRequests(t *testing.T) {
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		f.expect(queueDeclare)
		f.expect(queueDeclare)
		args := be.AppendUint16(nil, NotFound)
		args = appendShortStr(args, "NOT_FOUND - no queue 'a'")
		args = be.AppendUint16(args, classQueue)
		args = be.AppendUint16(args, 10)
		f.method(ch, channelClose, args)
		f.expect(channelCloseOk)
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error)
	go func() {
		_, err := ch.QueueDeclare(context.Background(), "a", QueueDeclareOptions{Passive: true})
		first <- err
	}()
	time.Sleep(20 * time.Millisecond) // make sure it's sent first
	_, err2 := ch.QueueDeclare(context.Background(), "b", QueueDeclareOptions{Passive: true})
	if err1 := <-first; !IsCode(err1, NotFound) {
		t.Fatalf("expected not found for the first request, got %v", err1)
	}
	if !errors.Is(err2, ErrClosed) || IsCode(err2, NotFound) || !isConnectionLost(err2) {
		t.Fatalf("expected a retryable error for the second request, got %v", err2)
	}
}

// An auto-delete exchange can be deleted by the broker's cleanup of the
// old connection after the client redeclared it, recovery then redeclares
// it before retrying the binding.
func TestFakeRecoverBindingAfterNotFound(t *testing.T) {
	var steps []string
	conn, err := dialFake(t, nil, nil, func(f *fakeBroker) {
		ch := f.openChannel()
		f.expect(exchangeDeclare)
		f.method(ch, exchangeDeclareOk, nil)
		f.expect(queueDeclare)
		f.method(ch, queueDeclareOk, append(appendShortStr(nil, "q"), 0, 0, 0, 0, 0, 0, 0, 0))
		f.expect(queueBind)
		args := be.AppendUint16(nil, NotFound)
		args = appendShortStr(args, "NOT_FOUND - no exchange 'x'")
		args = be.AppendUint16(args, classQueue)
		args = be.AppendUint16(args, 20)
		f.method(ch, channelClose, args)
		f.expect(channelCloseOk)
		ch = f.openChannel()
		for _, cm := range []uint32{exchangeDeclare, queueDeclare, queueBind} {
			f.expect(cm)
			steps = append(steps, methodName(cm))
			switch cm {
			case queueDeclare:
				f.method(ch, queueDeclareOk, append(appendShortStr(nil, "q"), 0, 0, 0, 0, 0, 0, 0, 0))
			default:
				f.method(ch, cm+1, nil) // the -ok reply
			}
		}
		f.expect(channelClose)
		f.method(ch, channelCloseOk, nil)
		io.Copy(io.Discard, f.conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{log: slog.New(slog.NewTextHandler(io.Discard, nil)), exchanges: map[string]*Exchange{}}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	defer c.cancel()
	x := &Exchange{c: c, name: "x", kind: Topic, opts: ExchangeOptions{AutoDelete: true}}
	q := &Queue{c: c, opts: QueueOptions{Exclusive: true}}
	name := "q"
	q.name.Store(&name)
	c.exchanges["x"] = x
	c.queues = []*Queue{q}
	c.qbindings = []queueBinding{{q: q, exchange: "x", routingKey: "#"}}
	if err := c.recoverTopology(conn); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(steps) != "[exchange.declare queue.declare queue.bind]" {
		t.Fatalf("unexpected recovery steps %v", steps)
	}
}
