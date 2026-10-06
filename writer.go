package amqp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// writer is the connection's write path. Frames are appended to wbuf under
// wmu, and the flusher goroutine swaps the buffer out and writes it to the
// socket without holding the lock. A slow socket therefore never blocks the
// lock: writers that need to wait for buffer space wait with their context,
// and the read loop's own replies (and acks) never wait at all.
type writer struct {
	wmu      sync.Mutex
	wbuf     []byte // frames waiting to be written
	wspare   []byte // a drained buffer, reused
	wprev    int    // len(wbuf) when the current write began
	werr     error  // sticky write error
	wlimit   int    // writers wait while wbuf is larger than this
	wspace   chan struct{}
	wflushed chan struct{}
	wqueued  int64 // bytes appended in total
	wwritten int64 // bytes written to the socket in total
	flushCh  chan struct{}
	wrote    atomic.Bool // written since the last heartbeat tick
}

func (c *Connection) initWriter() {
	c.wlimit = max(1<<20, 4*int(c.frameMax))
	c.wspace = make(chan struct{})
	c.wflushed = make(chan struct{})
	c.flushCh = make(chan struct{}, 1)
}

// beginWrite locks the writer and returns the buffer to append frames to,
// pass the result to endWrite or call cancelWrite. Unless force, it first
// waits while the buffer is full, until ctx is done. need is the number of
// bytes about to be appended, if known.
func (c *Connection) beginWrite(ctx context.Context, need int, force bool) ([]byte, error) {
	c.wmu.Lock()
	for {
		if c.werr != nil {
			err := c.werr
			c.wmu.Unlock()
			return nil, err
		}
		if force || len(c.wbuf) == 0 || len(c.wbuf)+need <= c.wlimit {
			c.wprev = len(c.wbuf)
			return c.wbuf, nil
		}
		space := c.wspace
		c.wmu.Unlock()
		if err := c.waitFor(ctx, space); err != nil {
			return nil, err
		}
		c.wmu.Lock()
	}
}

// waitSpace waits until the buffer has room, without locking the writer.
func (c *Connection) waitSpace(ctx context.Context) error {
	c.wmu.Lock()
	for len(c.wbuf) > c.wlimit && c.werr == nil {
		space := c.wspace
		c.wmu.Unlock()
		if err := c.waitFor(ctx, space); err != nil {
			return err
		}
		c.wmu.Lock()
	}
	err := c.werr
	c.wmu.Unlock()
	return err
}

func (c *Connection) waitFor(ctx context.Context, ch chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// endWrite keeps the frames appended to b, unlocks the writer and wakes up
// the flusher.
func (c *Connection) endWrite(b []byte) {
	c.wqueued += int64(len(b) - c.wprev)
	c.wbuf = b
	c.wmu.Unlock()
	select {
	case c.flushCh <- struct{}{}:
	default:
	}
}

// cancelWrite unlocks the writer, dropping what was appended.
func (c *Connection) cancelWrite() {
	c.wmu.Unlock()
}

func (c *Connection) flushLoop() {
	for {
		select {
		case <-c.flushCh:
		case <-c.done:
			return
		}
		for {
			c.wmu.Lock()
			if len(c.wbuf) == 0 || c.werr != nil {
				c.wmu.Unlock()
				break
			}
			buf := c.wbuf
			c.wbuf, c.wspare = c.wspare[:0], nil
			close(c.wspace)
			c.wspace = make(chan struct{})
			c.wmu.Unlock()

			_, err := c.conn.Write(buf)

			c.wmu.Lock()
			if err != nil {
				c.writeFailed(err)
				c.wmu.Unlock()
				return
			}
			c.wwritten += int64(len(buf))
			close(c.wflushed)
			c.wflushed = make(chan struct{})
			if cap(buf) <= 4*c.wlimit {
				c.wspare = buf[:0]
			}
			c.wmu.Unlock()
			c.wrote.Store(true)
		}
	}
}

// waitFlushed waits until everything appended so far is written to the
// socket, at most for timeout.
func (c *Connection) waitFlushed(timeout time.Duration) {
	deadline := time.After(timeout)
	c.wmu.Lock()
	target := c.wqueued
	for c.wwritten < target && c.werr == nil {
		flushed := c.wflushed
		c.wmu.Unlock()
		select {
		case <-flushed:
		case <-deadline:
			return
		case <-c.done:
			return
		}
		c.wmu.Lock()
	}
	c.wmu.Unlock()
}

// writeFailed records a write error and closes the socket, c.wmu must be held.
func (c *Connection) writeFailed(err error) {
	if c.werr == nil {
		c.werr = &closedError{err}
		c.abort(c.werr)
	}
}

func (c *Connection) heartbeatLoop() {
	t := time.NewTicker(c.heartbeat / 2)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			if c.wrote.Swap(false) {
				continue
			}
			if b, err := c.beginWrite(context.Background(), 0, true); err == nil {
				c.endWrite(append(b, frameHeartbeat, 0, 0, 0, 0, 0, 0, frameEnd))
			}
		}
	}
}

// writeMethod writes a method frame, fn appends the method's arguments.
// Unless force it waits for buffer space, see beginWrite.
func (c *Connection) writeMethod(ctx context.Context, channel uint16, cm uint32, force bool, fn func([]byte) ([]byte, error)) error {
	b, err := c.beginWrite(ctx, 0, force)
	if err != nil {
		return err
	}
	b, err = c.appendMethod(b, channel, cm, fn)
	if err != nil {
		c.cancelWrite()
		return err
	}
	c.endWrite(b)
	return nil
}

func (c *Connection) appendMethod(b []byte, channel uint16, cm uint32, fn func([]byte) ([]byte, error)) ([]byte, error) {
	b, start := beginMethod(b, channel, cm)
	if fn != nil {
		var err error
		if b, err = fn(b); err != nil {
			return b, err
		}
	}
	if size := len(b) - start; size > int(c.frameMax)-frameOverhead {
		return b, fmt.Errorf("amqp: %s frame of %d bytes exceeds frame max %d", methodName(cm), size, c.frameMax)
	}
	return endFrame(b, start), nil
}
