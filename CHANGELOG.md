# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.1] - 2026-10-06

### Fixed

- `Queue.Publish` to a server-named queue while the client was reconnecting used the queue's old name, so the message was dropped as unroutable and still reported as published.
- A handler calling `Subscription.Cancel` while another goroutine (or `Client.Close`) was waiting for it deadlocked.
- Buffered deliveries of a no-ack subscription were lost when the subscription replaced its consumer, e.g. after a channel error.
- A publish failing because its exchange was deleted didn't always make the client check exchanges again, so later publishes to it closed the shared publish channel.
- An `RPCClient` closed while it was recovering its reply channel left the new channel open.
- Topology recovery wasn't bounded by `Config.ConnectTimeout`, and a failed recovery waited for the broker to confirm the close.
- Concurrent first `RPCCall`s waited for the one creating the shared RPC client without honouring their own contexts.
- Bindings whose arguments differed only in value types, e.g. `int32(1)` and `"1"` for a headers exchange, were treated as duplicates and not recovered.
- `BasicAck(0, true)` and `BasicNack(0, true, …)`, which settle all deliveries, didn't mark the deliveries as acknowledged, so acknowledging them again closed the channel.

## [1.2.0] - 2026-10-06

### Added

- `NewGzip` and `NewDeflate` return encoders with a decoded size limit. `ErrDecodedTooLarge` is returned when a body decodes to more than that.

### Changed

- Frames are written to the socket by the flusher goroutine outside the write lock. A publish that waits for buffer space respects its context, and the read loop never waits for a write. ([#10](https://github.com/cloudamqp/amqp-client.go/issues/10))
- Topology declared with the context passed to `OnConnect` isn't recovered automatically, as `OnConnect` declares it again, and subscriptions started with it end with their connection. ([#14](https://github.com/cloudamqp/amqp-client.go/issues/14))
- `Gzip` and `Deflate` decode at most 128 MiB (`DefaultMaxDecodedSize`). ([#12](https://github.com/cloudamqp/amqp-client.go/issues/12))

### Fixed

- Deliveries to no-ack consumers were dropped when their channel closed, both buffered ones and those arriving while closing. ([#8](https://github.com/cloudamqp/amqp-client.go/issues/8))
- A channel id was reused while the broker was still closing the channel after a close timeout, and the client's cancel-ok for an abandoned consumer the broker had cancelled closed the connection. ([#9](https://github.com/cloudamqp/amqp-client.go/issues/9))
- A publish blocked on a stalled socket ignored its context and stalled the read loop. ([#10](https://github.com/cloudamqp/amqp-client.go/issues/10))
- Headers with the 'U' field type are decoded, headers that can't be decoded are dropped (with a warning) instead of closing the connection, and the property flags continuation bit is honoured. ([#11](https://github.com/cloudamqp/amqp-client.go/issues/11))
- Protocol limits are enforced when reading: the frame size during the handshake, the body size (no longer allocated up front), the nesting depth of field tables and the decompressed size. A broker replying with another protocol version, or a server that doesn't speak AMQP, gives a clear error, and frame errors are reported to the broker with `connection.close`. ([#12](https://github.com/cloudamqp/amqp-client.go/issues/12))
- Acknowledging a no-ack delivery, or one already covered by `BasicAck`/`BasicNack` with multiple, returns `ErrAlreadyAcknowledged` instead of making the broker close the channel. `Queue.Get` uses its own channel, so failed operations no longer prevent acknowledging fetched messages. ([#13](https://github.com/cloudamqp/amqp-client.go/issues/13))
- A subscription created while the client was closing was never stopped, server-named queues declared in `OnConnect` piled up in the recovery list, and `UpdateSecret` could return on the reply to an earlier, abandoned call. ([#14](https://github.com/cloudamqp/amqp-client.go/issues/14))

## [1.1.0] - 2026-10-06

### Added

- `Config.ConnectTimeout` bounds the TCP connect and the TLS and AMQP handshakes. The `Client` uses 30s for its reconnect attempts by default.
- `ErrUnconfirmed`, matched by the error for a message whose channel closed before the broker confirmed it.

### Fixed

- The `Client` didn't recover from a broker-initiated `connection.close`, e.g. 320 CONNECTION_FORCED on a broker restart: subscriptions stopped and operations failed instead of waiting for the reconnect. ([#1](https://github.com/cloudamqp/amqp-client.go/issues/1))
- `Channel.Close` discarded the publisher confirms (and returns) the broker sent before close-ok, so confirmed messages were reported as failed. ([#2](https://github.com/cloudamqp/amqp-client.go/issues/2))
- A publish to a missing exchange failed all concurrent publishes on the `Client`'s shared channel with its 404. The `Client` now checks an exchange it hasn't declared before the first publish to it, and messages left unconfirmed when a channel closes get an `ErrUnconfirmed` error instead of the broker's exception. ([#3](https://github.com/cloudamqp/amqp-client.go/issues/3))
- The `Client` hung if the connection was lost while `OnConnect` was running. `OnConnect` now gets a context that's cancelled when its connection is lost. ([#4](https://github.com/cloudamqp/amqp-client.go/issues/4))
- Reconnect attempts had no timeout, so a peer that accepted TCP but never answered blocked reconnects forever. ([#5](https://github.com/cloudamqp/amqp-client.go/issues/5))
- A connection the broker closed right after it was established was reconnected in a tight loop, ignoring the backoff and `MaxRetries`. ([#6](https://github.com/cloudamqp/amqp-client.go/issues/6))
- A subscription on an auto-delete queue stopped when only its channel closed, as the broker had deleted the queue. The queue is now declared again with its bindings. ([#7](https://github.com/cloudamqp/amqp-client.go/issues/7))

## [1.0.1] - 2026-10-06

### Fixed

- Recovering a binding after a reconnect failed when the broker's cleanup of the old connection deleted an auto-delete exchange after the client had redeclared it. The client now redeclares the exchange and queue and retries the binding.

## [1.0.0] - 2026-10-06

### Added

- Low-level API: `Dial`, `Connection` and `Channel` with all AMQP 0-9-1 client methods, publisher confirms, consumers with `Next` and `Deliveries`, basic.get, transactions, returns, connection.blocked, update-secret and heartbeats.
- High-level API: `Client` with automatic reconnection, recovery of exchanges, queues, bindings and subscriptions, confirmed publishing, subscriptions with worker pools and automatic acknowledgements, codecs and RPC over direct reply-to.

[Unreleased]: https://github.com/cloudamqp/amqp-client.go/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/cloudamqp/amqp-client.go/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/cloudamqp/amqp-client.go/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/cloudamqp/amqp-client.go/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/cloudamqp/amqp-client.go/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/cloudamqp/amqp-client.go/releases/tag/v1.0.0
