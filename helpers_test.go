package amqp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func testURL() string {
	if u := os.Getenv("AMQP_URL"); u != "" {
		return u
	}
	return "amqp://guest:guest@localhost:5672"
}

var (
	brokerOnce sync.Once
	brokerErr  error
)

// requireBroker skips the test if no broker is reachable.
func requireBroker(t testing.TB) {
	t.Helper()
	brokerOnce.Do(func() {
		u, err := ParseURI(testURL())
		if err != nil {
			brokerErr = err
			return
		}
		c, err := net.DialTimeout("tcp", u.Addr(), time.Second)
		if err != nil {
			brokerErr = err
			return
		}
		c.Close()
	})
	if brokerErr != nil {
		if os.Getenv("AMQP_REQUIRE_BROKER") != "" {
			t.Fatalf("broker not available: %v", brokerErr)
		}
		t.Skipf("broker not available: %v", brokerErr)
	}
}

func testContext(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func dialTest(t testing.TB, cfg *Config) *Connection {
	t.Helper()
	requireBroker(t)
	conn, err := Dial(testContext(t), testURL(), cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func openChannel(t testing.TB, conn *Connection) *Channel {
	t.Helper()
	ch, err := conn.Channel(testContext(t))
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	return ch
}

func randomName(prefix string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// tempQueue declares an exclusive auto-delete queue with a random name.
func tempQueue(t testing.TB, ch *Channel) string {
	t.Helper()
	q, err := ch.QueueDeclare(testContext(t), "", QueueDeclareOptions{Exclusive: true, AutoDelete: true})
	if err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	return q.Name
}

// proxy forwards TCP traffic to the broker and can cut all connections, to
// simulate network failures.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newProxy(t testing.TB) *proxy {
	t.Helper()
	requireBroker(t)
	u, _ := ParseURI(testURL())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b, err := net.Dial("tcp", u.Addr())
			if err != nil {
				c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, b)
			p.mu.Unlock()
			go pipe(c, b)
			go pipe(b, c)
		}
	}()
	t.Cleanup(func() { ln.Close(); p.cut() })
	return p
}

func pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	dst.Close()
	src.Close()
}

// cut closes all proxied connections.
func (p *proxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

// url returns an AMQP URL that goes through the proxy.
func (p *proxy) url() string {
	u, _ := ParseURI(testURL())
	return "amqp://" + u.Username + ":" + u.Password + "@" + p.ln.Addr().String() + "/"
}
