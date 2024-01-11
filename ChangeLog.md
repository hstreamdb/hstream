# Changelog for HStream

## Unreleased


## v0.19.0 [2024-01-12]

HStream v0.19.0 introduces support for [Kafka API](https://kafka.apache.org/protocol.html#protocol_api_keys), so users can interact with HStream using Kafka clients now.

### New Features

- Add support for [Kafka API](https://kafka.apache.org/protocol.html#protocol_api_keys), compatible with Apache Kafka 0.11 or later.
- Add a new `hstream-kafka` command line tool for working with HStream Kafka API.

### Enhancements

- Add support for configuring gRPC channel settings. [#1659](https://github.com/hstreamdb/hstream/pull/1659)
- Add support for logging to files. [#1708](https://github.com/hstreamdb/hstream/pull/1708)
- Add `isAlive` metric for connectors. [#1593](https://github.com/hstreamdb/hstream/pull/1593)
- Support GHC 9.4. [#1698](https://github.com/hstreamdb/hstream/pull/1698)
- cli(hadmin-store): Add support for updating `replicate-across` attribute. [#1608](https://github.com/hstreamdb/hstream/pull/1608)

### Changes and Bug fixes

- Set read timeout for ldreader in readStreamByKey. [#1575](https://github.com/hstreamdb/hstream/pull/1575)
- Ignore `Earliest` position in trimShards. [#1579](https://github.com/hstreamdb/hstream/pull/1579)
- Fix unary handler may block in hs-grpc. [#1585](https://github.com/hstreamdb/hstream/pull/1585)
- Fix destroyed lambda ref in hstore FFI stubs. [#1642](https://github.com/hstreamdb/hstream/pull/1642)
- Fix build for HStreamUseGrpcHaskell. [#1670](https://github.com/hstreamdb/hstream/pull/1670)
- Fix a parsing issue with multiple AdvertisedListeners. [#1671](https://github.com/hstreamdb/hstream/pull/1671)
- Wait for the cluster epoch to sync before making a new assignment. [#1676](https://github.com/hstreamdb/hstream/pull/1676)
- Handle errors in trimShards. [#1707](https://github.com/hstreamdb/hstream/pull/1707)
- Handle unrecoverable gaptype for shardReader. [#1713](https://github.com/hstreamdb/hstream/pull/1713)
- Avoid log flushing when no logging is present. [#1701](https://github.com/hstreamdb/hstream/pull/1701)
- Dockerfile: upgrade base image to ubuntu jammy. [#1664](https://github.com/hstreamdb/hstream/pull/1664)
- Add a `--log-flush-immediately` option to flush logs immediately. [#1654](https://github.com/hstreamdb/hstream/pull/1654)
- connector: Recover tasks from listRecoverableResources. [#1612](https://github.com/hstreamdb/hstream/pull/1612)
- connector: Recover connectors without checking. [#1644](https://github.com/hstreamdb/hstream/pull/1644)
- connector: Handle zk exception. [#1624](https://github.com/hstreamdb/hstream/pull/1624)
- connector: Init ioTaskKvs table. [#1673](https://github.com/hstreamdb/hstream/pull/1673)
- sql: Fix `CREATE CONNECTOR` clauses. [#1588](https://github.com/hstreamdb/hstream/pull/1588)
- query: Recover tasks from listRecoverableResources. [#1613](https://github.com/hstreamdb/hstream/pull/1613)
- cli: Fix read-stream --from timestamp --until latest may fail. [#1595](https://github.com/hstreamdb/hstream/pull/1595)
- cli: Fix wrong server version for the result of `hstream node status` command. [#1631](https://github.com/hstreamdb/hstream/pull/1631)
- cli(hadmin-store): Fix formatting and handling of logReplicateAcross attribute display. [#1622](https://github.com/hstreamdb/hstream/pull/1622)
 
