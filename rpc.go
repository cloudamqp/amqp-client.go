package amqp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

// directReplyTo is the pseudo queue for RPC replies, supported by RabbitMQ
// and LavinMQ.
const directReplyTo = "amq.rabbitmq.reply-to"

// ErrUnroutable is returned by an RPC call when no queue received the
// request, i.e. there's no server for the method.
var ErrUnroutable = errors.New("amqp: message could not be routed to any queue")

// RPCHandler handles an RPC request and returns the reply. If it returns
// an error the request is rejected (dead-lettered if the queue has a
// dead-letter exchange) and no reply is sent.
type RPCHandler func(ctx context.Context, req *Delivery) (Message, error)

// RPCServer serves RPC requests from a queue (declared durable if it
// doesn't exist). Replies are sent to the request's ReplyTo with its
// CorrelationID, and with its content type and encoding unless the reply
// sets them. Cancel the returned subscription to stop serving.
func (c *Client) RPCServer(ctx context.Context, queue string, handler RPCHandler, opts *SubscribeOptions) (*Subscription, error) {
	q, err := c.Queue(ctx, queue, nil)
	if err != nil {
		return nil, err
	}
	return q.Subscribe(ctx, func(ctx context.Context, req *Delivery) error {
		reply, err := handler(ctx, req)
		if err != nil {
			req.Reject(false)
			return err
		}
		if req.ReplyTo == "" {
			return nil
		}
		reply.CorrelationID = req.CorrelationID
		if reply.ContentType == "" {
			reply.ContentType = req.ContentType
		}
		if reply.ContentEncoding == "" {
			reply.ContentEncoding = req.ContentEncoding
		}
		if reply.DeliveryMode == 0 {
			reply.DeliveryMode = Transient
		}
		p, err := c.encode(&reply)
		if err != nil {
			req.Reject(false)
			return err
		}
		return req.Channel().BasicPublish(ctx, "", req.ReplyTo, p)
	}, opts)
}

// RPCCall sends a request to an RPC server consuming from queue and waits
// for the reply, use a context with a deadline to not wait forever. It uses
// a shared [RPCClient].
func (c *Client) RPCCall(ctx context.Context, queue string, msg Message) (*Delivery, error) {
	c.rpcMu.Lock()
	if c.rpcClient == nil {
		rpc, err := c.RPCClient(ctx)
		if err != nil {
			c.rpcMu.Unlock()
			return nil, err
		}
		c.rpcClient = rpc
	}
	rpc := c.rpcClient
	c.rpcMu.Unlock()
	return rpc.Call(ctx, queue, msg)
}

// RPCClient makes RPC calls using direct reply-to. Calls can be made
// concurrently, and the client recovers after reconnects.
type RPCClient struct {
	c      *Client
	prefix string
	seq    atomic.Uint64

	sem     chan struct{} // guards setting up ch
	ch      atomic.Pointer[Channel]
	mu      sync.Mutex
	pending map[string]pendingCall
	closed  bool
}

type pendingCall struct {
	ch    *Channel
	reply chan rpcResult
}

type rpcResult struct {
	d   *Delivery
	err error
}

// RPCClient returns a new RPC client, close it when done.
func (c *Client) RPCClient(ctx context.Context) (*RPCClient, error) {
	b := make([]byte, 8)
	rand.Read(b)
	r := &RPCClient{
		c:       c,
		prefix:  hex.EncodeToString(b) + "-",
		sem:     make(chan struct{}, 1),
		pending: map[string]pendingCall{},
	}
	if _, err := r.channel(ctx); err != nil {
		return nil, err
	}
	c.topoMu.Lock()
	c.rpcClients[r] = struct{}{}
	c.topoMu.Unlock()
	return r, nil
}

// channel returns the channel consuming replies, setting it up if needed.
func (r *RPCClient) channel(ctx context.Context) (*Channel, error) {
	if ch := r.ch.Load(); ch != nil && !ch.IsClosed() {
		return ch, nil
	}
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-r.sem }()
	if ch := r.ch.Load(); ch != nil && !ch.IsClosed() {
		return ch, nil
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	for {
		conn, err := r.c.connection(ctx)
		if err != nil {
			return nil, err
		}
		ch, err := conn.Channel(ctx)
		if err == nil {
			ch.OnReturn(func(ret *Return) { r.fail(ret.CorrelationID, ErrUnroutable) })
			var cons *Consumer
			if cons, err = ch.BasicConsume(ctx, directReplyTo, ConsumeOptions{NoAck: true}); err == nil {
				r.ch.Store(ch)
				go r.dispatch(ch, cons)
				return ch, nil
			}
			ch.Close()
		}
		if !isConnectionLost(err) || ctx.Err() != nil {
			return nil, err
		}
	}
}

func (r *RPCClient) dispatch(ch *Channel, cons *Consumer) {
	for {
		d, err := cons.Next(context.Background())
		if err != nil {
			break
		}
		r.mu.Lock()
		call, ok := r.pending[d.CorrelationID]
		delete(r.pending, d.CorrelationID)
		r.mu.Unlock()
		if ok {
			call.reply <- rpcResult{d: d}
		}
	}
	// Fail the calls waiting for replies on this channel
	err := ch.Err()
	if err == nil {
		err = ErrClosed
	}
	r.mu.Lock()
	for id, call := range r.pending {
		if call.ch == ch {
			call.reply <- rpcResult{err: err}
			delete(r.pending, id)
		}
	}
	r.mu.Unlock()
}

func (r *RPCClient) fail(correlationID string, err error) {
	r.mu.Lock()
	call, ok := r.pending[correlationID]
	delete(r.pending, correlationID)
	r.mu.Unlock()
	if ok {
		call.reply <- rpcResult{err: err}
	}
}

// Call sends a request to the RPC server consuming from queue and waits
// for the reply, use a context with a deadline to not wait forever. If no
// queue receives the request ErrUnroutable is returned.
func (r *RPCClient) Call(ctx context.Context, queue string, msg Message) (*Delivery, error) {
	if msg.DeliveryMode == 0 {
		msg.DeliveryMode = Transient
	}
	p, err := r.c.encode(&msg)
	if err != nil {
		return nil, err
	}
	id := r.prefix + strconv.FormatUint(r.seq.Add(1), 36)
	p.CorrelationID = id
	p.ReplyTo = directReplyTo
	p.Mandatory = true
	for {
		ch, err := r.channel(ctx)
		if err != nil {
			return nil, err
		}
		reply := make(chan rpcResult, 1)
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrClosed
		}
		r.pending[id] = pendingCall{ch, reply}
		r.mu.Unlock()
		if err := ch.BasicPublish(ctx, "", queue, p); err != nil {
			r.mu.Lock()
			delete(r.pending, id)
			r.mu.Unlock()
			if errors.Is(err, ErrClosed) && ctx.Err() == nil {
				continue // not sent, retry on a new channel
			}
			return nil, err
		}
		select {
		case res := <-reply:
			if res.err != nil {
				return nil, fmt.Errorf("amqp: rpc call to %q: %w", queue, res.err)
			}
			return res.d, nil
		case <-ctx.Done():
			r.mu.Lock()
			delete(r.pending, id)
			r.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

// Close closes the RPC client's channel, failing calls in progress.
func (r *RPCClient) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.c.topoMu.Lock()
	delete(r.c.rpcClients, r)
	r.c.topoMu.Unlock()
	if ch := r.ch.Load(); ch != nil {
		return ch.Close()
	}
	return nil
}
