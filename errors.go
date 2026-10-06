package amqp

import (
	"errors"
	"fmt"
)

var (
	// ErrClosed reports that the connection, channel or client is closed.
	// Every error caused by a closed connection or channel matches it with
	// errors.Is, including an [*Error] from the broker and network errors.
	ErrClosed = errors.New("amqp: closed")

	// ErrPublishNacked is returned when the broker negatively acknowledges
	// a published message, i.e. it couldn't take responsibility for it.
	ErrPublishNacked = errors.New("amqp: message nacked by the broker")

	// ErrConsumerCancelled is returned by a consumer after it has been
	// cancelled by the client and all buffered deliveries are read.
	ErrConsumerCancelled = errors.New("amqp: consumer cancelled")

	// ErrConsumerCancelledByServer is returned by a consumer the broker
	// cancelled, e.g. because its queue was deleted.
	ErrConsumerCancelledByServer = errors.New("amqp: consumer cancelled by the broker")

	// ErrNoConfirmMode is returned when waiting for confirms on a channel
	// that isn't in confirm mode.
	ErrNoConfirmMode = errors.New("amqp: channel is not in confirm mode")

	// ErrChannelMax is returned when all channel ids are in use.
	ErrChannelMax = errors.New("amqp: channel max reached")
)

// Error is an exception from the broker (or the client) that closed a
// channel or a connection. Code is one of the reply codes, such as
// [NotFound] or [PreconditionFailed].
type Error struct {
	Code     uint16
	Reason   string
	ClassID  uint16 // class of the method that caused the error, or 0
	MethodID uint16 // method that caused the error, or 0
	// Connection is true when the whole connection was closed, otherwise
	// only the channel was closed.
	Connection bool
	// Server is true when the broker sent the exception.
	Server bool
}

func (e *Error) Error() string {
	scope := "channel"
	if e.Connection {
		scope = "connection"
	}
	if e.ClassID != 0 {
		return fmt.Sprintf("amqp: %s closed: %d %s (%s)", scope, e.Code, e.Reason,
			methodName(uint32(e.ClassID)<<16|uint32(e.MethodID)))
	}
	return fmt.Sprintf("amqp: %s closed: %d %s", scope, e.Code, e.Reason)
}

// Is makes errors.Is(err, ErrClosed) true for broker exceptions.
func (e *Error) Is(target error) bool {
	return target == ErrClosed
}

// IsCode reports whether err is an [*Error] with the given reply code.
func IsCode(err error, code uint16) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// closedError wraps the cause of a lost connection so it matches ErrClosed.
type closedError struct{ cause error }

func (e *closedError) Error() string        { return "amqp: connection lost: " + e.cause.Error() }
func (e *closedError) Unwrap() error        { return e.cause }
func (e *closedError) Is(target error) bool { return target == ErrClosed }

// discardedError is returned for requests the broker discarded because an
// earlier operation on the channel caused it to close it. Such requests
// are safe to retry on another channel.
type discardedError struct{ cause error }

func (e *discardedError) Error() string {
	return "amqp: channel closed by an earlier operation: " + e.cause.Error()
}
func (e *discardedError) Is(target error) bool { return target == ErrClosed }

func unexpectedMethod(cm uint32) *Error {
	return &Error{Code: UnexpectedFrame, Reason: "unexpected " + methodName(cm),
		ClassID: uint16(cm >> 16), MethodID: uint16(cm), Connection: true}
}
