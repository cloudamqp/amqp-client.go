package amqp

// Protocol constants from the AMQP 0-9-1 specification.

const (
	frameMethod    = 1
	frameHeader    = 2
	frameBody      = 3
	frameHeartbeat = 8
	frameEnd       = 0xCE

	// frameOverhead is the frame header (type, channel, size) plus the frame-end octet.
	frameOverhead = 8

	// minFrameMax is the smallest frame size a peer has to accept.
	minFrameMax = 4096
)

var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

const (
	classConnection = 10
	classChannel    = 20
	classExchange   = 40
	classQueue      = 50
	classBasic      = 60
	classConfirm    = 85
	classTx         = 90
)

// Methods are identified by their class and method id packed into one uint32,
// which is exactly how they appear on the wire.
const (
	connectionStart          = classConnection<<16 | 10
	connectionStartOk        = classConnection<<16 | 11
	connectionSecure         = classConnection<<16 | 20
	connectionSecureOk       = classConnection<<16 | 21
	connectionTune           = classConnection<<16 | 30
	connectionTuneOk         = classConnection<<16 | 31
	connectionOpen           = classConnection<<16 | 40
	connectionOpenOk         = classConnection<<16 | 41
	connectionClose          = classConnection<<16 | 50
	connectionCloseOk        = classConnection<<16 | 51
	connectionBlocked        = classConnection<<16 | 60
	connectionUnblocked      = classConnection<<16 | 61
	connectionUpdateSecret   = classConnection<<16 | 70
	connectionUpdateSecretOk = classConnection<<16 | 71

	channelOpen    = classChannel<<16 | 10
	channelOpenOk  = classChannel<<16 | 11
	channelFlow    = classChannel<<16 | 20
	channelFlowOk  = classChannel<<16 | 21
	channelClose   = classChannel<<16 | 40
	channelCloseOk = classChannel<<16 | 41

	exchangeDeclare   = classExchange<<16 | 10
	exchangeDeclareOk = classExchange<<16 | 11
	exchangeDelete    = classExchange<<16 | 20
	exchangeDeleteOk  = classExchange<<16 | 21
	exchangeBind      = classExchange<<16 | 30
	exchangeBindOk    = classExchange<<16 | 31
	exchangeUnbind    = classExchange<<16 | 40
	exchangeUnbindOk  = classExchange<<16 | 51

	queueDeclare   = classQueue<<16 | 10
	queueDeclareOk = classQueue<<16 | 11
	queueBind      = classQueue<<16 | 20
	queueBindOk    = classQueue<<16 | 21
	queuePurge     = classQueue<<16 | 30
	queuePurgeOk   = classQueue<<16 | 31
	queueDelete    = classQueue<<16 | 40
	queueDeleteOk  = classQueue<<16 | 41
	queueUnbind    = classQueue<<16 | 50
	queueUnbindOk  = classQueue<<16 | 51

	basicQos       = classBasic<<16 | 10
	basicQosOk     = classBasic<<16 | 11
	basicConsume   = classBasic<<16 | 20
	basicConsumeOk = classBasic<<16 | 21
	basicCancel    = classBasic<<16 | 30
	basicCancelOk  = classBasic<<16 | 31
	basicPublish   = classBasic<<16 | 40
	basicReturn    = classBasic<<16 | 50
	basicDeliver   = classBasic<<16 | 60
	basicGet       = classBasic<<16 | 70
	basicGetOk     = classBasic<<16 | 71
	basicGetEmpty  = classBasic<<16 | 72
	basicAck       = classBasic<<16 | 80
	basicReject    = classBasic<<16 | 90
	basicRecover   = classBasic<<16 | 110
	basicRecoverOk = classBasic<<16 | 111
	basicNack      = classBasic<<16 | 120

	confirmSelect   = classConfirm<<16 | 10
	confirmSelectOk = classConfirm<<16 | 11

	txSelect     = classTx<<16 | 10
	txSelectOk   = classTx<<16 | 11
	txCommit     = classTx<<16 | 20
	txCommitOk   = classTx<<16 | 21
	txRollback   = classTx<<16 | 30
	txRollbackOk = classTx<<16 | 31
)

// Reply codes used by the broker when closing a connection or channel, see
// [Error.Code].
const (
	ReplySuccess       = 200
	ContentTooLarge    = 311
	NoRoute            = 312
	NoConsumers        = 313
	ConnectionForced   = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	ResourceLocked     = 405
	PreconditionFailed = 406
	FrameError         = 501
	SyntaxError        = 502
	CommandInvalid     = 503
	ChannelError       = 504
	UnexpectedFrame    = 505
	ResourceError      = 506
	NotAllowed         = 530
	NotImplemented     = 540
	InternalError      = 541
)

// methodName returns a human readable name of a method, used in error messages.
func methodName(cm uint32) string {
	switch cm {
	case connectionStart:
		return "connection.start"
	case connectionStartOk:
		return "connection.start-ok"
	case connectionSecure:
		return "connection.secure"
	case connectionTune:
		return "connection.tune"
	case connectionOpen:
		return "connection.open"
	case connectionOpenOk:
		return "connection.open-ok"
	case connectionClose:
		return "connection.close"
	case connectionCloseOk:
		return "connection.close-ok"
	case connectionBlocked:
		return "connection.blocked"
	case connectionUnblocked:
		return "connection.unblocked"
	case connectionUpdateSecret:
		return "connection.update-secret"
	case connectionUpdateSecretOk:
		return "connection.update-secret-ok"
	case channelOpen:
		return "channel.open"
	case channelOpenOk:
		return "channel.open-ok"
	case channelFlow:
		return "channel.flow"
	case channelFlowOk:
		return "channel.flow-ok"
	case channelClose:
		return "channel.close"
	case channelCloseOk:
		return "channel.close-ok"
	case exchangeDeclare:
		return "exchange.declare"
	case exchangeDeclareOk:
		return "exchange.declare-ok"
	case exchangeDelete:
		return "exchange.delete"
	case exchangeDeleteOk:
		return "exchange.delete-ok"
	case exchangeBind:
		return "exchange.bind"
	case exchangeBindOk:
		return "exchange.bind-ok"
	case exchangeUnbind:
		return "exchange.unbind"
	case exchangeUnbindOk:
		return "exchange.unbind-ok"
	case queueDeclare:
		return "queue.declare"
	case queueDeclareOk:
		return "queue.declare-ok"
	case queueBind:
		return "queue.bind"
	case queueBindOk:
		return "queue.bind-ok"
	case queuePurge:
		return "queue.purge"
	case queuePurgeOk:
		return "queue.purge-ok"
	case queueDelete:
		return "queue.delete"
	case queueDeleteOk:
		return "queue.delete-ok"
	case queueUnbind:
		return "queue.unbind"
	case queueUnbindOk:
		return "queue.unbind-ok"
	case basicQos:
		return "basic.qos"
	case basicQosOk:
		return "basic.qos-ok"
	case basicConsume:
		return "basic.consume"
	case basicConsumeOk:
		return "basic.consume-ok"
	case basicCancel:
		return "basic.cancel"
	case basicCancelOk:
		return "basic.cancel-ok"
	case basicPublish:
		return "basic.publish"
	case basicReturn:
		return "basic.return"
	case basicDeliver:
		return "basic.deliver"
	case basicGet:
		return "basic.get"
	case basicGetOk:
		return "basic.get-ok"
	case basicGetEmpty:
		return "basic.get-empty"
	case basicAck:
		return "basic.ack"
	case basicReject:
		return "basic.reject"
	case basicRecover:
		return "basic.recover"
	case basicRecoverOk:
		return "basic.recover-ok"
	case basicNack:
		return "basic.nack"
	case confirmSelect:
		return "confirm.select"
	case confirmSelectOk:
		return "confirm.select-ok"
	case txSelect:
		return "tx.select"
	case txSelectOk:
		return "tx.select-ok"
	case txCommit:
		return "tx.commit"
	case txCommitOk:
		return "tx.commit-ok"
	case txRollback:
		return "tx.rollback"
	case txRollbackOk:
		return "tx.rollback-ok"
	}
	return "unknown method"
}
