# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-10-06

### Added

- Low-level API: `Dial`, `Connection` and `Channel` with all AMQP 0-9-1 client methods, publisher confirms, consumers with `Next` and `Deliveries`, basic.get, transactions, returns, connection.blocked, update-secret and heartbeats.
- High-level API: `Client` with automatic reconnection, recovery of exchanges, queues, bindings and subscriptions, confirmed publishing, subscriptions with worker pools and automatic acknowledgements, codecs and RPC over direct reply-to.

[Unreleased]: https://github.com/cloudamqp/amqp-client.go/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/cloudamqp/amqp-client.go/releases/tag/v1.0.0
