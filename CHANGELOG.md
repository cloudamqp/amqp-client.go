# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/cloudamqp/amqp-client.go/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/cloudamqp/amqp-client.go/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/cloudamqp/amqp-client.go/releases/tag/v1.0.0
