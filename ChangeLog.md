# Changelog for HStream

## Unreleased

## v0.20.0 [2024-04-26]

HStream Kafka Server

### New Features

- Add a new network server implemented in C++ for better performance 
    - introduce C++ network server [#1739](https://github.com/hstreamdb/hstream/pull/1739)
    - do not free NULL stable_ptr [#1747](https://github.com/hstreamdb/hstream/pull/1747)
    - make Haskell callback asynchronous [#1754](https://github.com/hstreamdb/hstream/pull/1754)
    - switch to using asio [#1761](https://github.com/hstreamdb/hstream/pull/1761) 
    - improve C++ server [#1765](https://github.com/hstreamdb/hstream/pull/1765)
- Add basic supports for Kafka ACL 
    - implement basic ACL lib and handlers [#1751](https://github.com/hstreamdb/hstream/pull/1751)
    - use metastore interfaces for ACL [#1766](https://github.com/hstreamdb/hstream/pull/1766)
    - make ACL mechanism fully optional [#1768](https://github.com/hstreamdb/hstream/pull/1768)
    - support ACL in produce (0) handler [#1767](https://github.com/hstreamdb/hstream/pull/1767)
    - do authorization on create/delete topics & create partitions [#1771](https://github.com/hstreamdb/hstream/pull/1771)
    - do authorization on group-related handlers ([11..16]) [#1772](https://github.com/hstreamdb/hstream/pull/1772)
    - do authorization on list and commit offsets (2, 8) [#1774](https://github.com/hstreamdb/hstream/pull/1774)
    - do authorization on fetch offsets (9) [#1773](https://github.com/hstreamdb/hstream/pull/1773)
    - do authorization on metadata (3) [#1779](https://github.com/hstreamdb/hstream/pull/1779)
    - do authorization on describe configs (32) [#1777](https://github.com/hstreamdb/hstream/pull/1777)
    - do authorization on find coordinator (10) [#1778](https://github.com/hstreamdb/hstream/pull/1778)
    - do authorization on fetch (1) [#1776](https://github.com/hstreamdb/hstream/pull/1776)
 
### Enhancements

- Support CreatePartitions [#1757](https://github.com/hstreamdb/hstream/pull/1757)
    - use partitionId as loggroup name [#1756](https://github.com/hstreamdb/hstream/pull/1756)
    - add listStreamPartitionsOrderedByName [#1755](https://github.com/hstreamdb/hstream/pull/1755)
- Refactor kafka protocol modules [#1787](https://github.com/hstreamdb/hstream/pull/1787)
- Upgrade max produce API version to v7 [#1782](https://github.com/hstreamdb/hstream/pull/1782)
- Do not re-encode batches in produce handler [#1783](https://github.com/hstreamdb/hstream/pull/1783)
- Refactor fetch handler for better performance [#1749](https://github.com/hstreamdb/hstream/pull/1749)
    - add decodeNextRecordOffset [#1750](https://github.com/hstreamdb/hstream/pull/1750) 
- Ensure `fetch` returns some data even when `minBytes` is set to 0 [#1781](https://github.com/hstreamdb/hstream/pull/1781)
- Validate topic name when create [#1788](https://github.com/hstreamdb/hstream/pull/1788)
- Validate topic name in listTopicConfigs [#1793](https://github.com/hstreamdb/hstream/pull/1793)
- Auto trim CheckpointedOffsetStorage [#1796](https://github.com/hstreamdb/hstream/pull/1796)
- [meta] Support auto reconnection for zookeeper client [#1800](https://github.com/hstreamdb/hstream/pull/1800)
- [perf] Increase Haskell RTS -A to 128MB [#1741](https://github.com/hstreamdb/hstream/pull/1741)
- improve TOPIC_ALREADY_EXISTS error message [#1780](https://github.com/hstreamdb/hstream/pull/1780)
- Remove consumer group member if sync timeout and improve group logs [#1746](https://github.com/hstreamdb/hstream/pull/1746)
- Update consumer group heartbeat after committing offsets [#1753](https://github.com/hstreamdb/hstream/pull/1753)
- [build] Upgrade Haskell image to GHC 9.4 [#1763](https://github.com/hstreamdb/hstream/pull/1763)
- [build] Download cabal-store-gc directly in dockerfile [#1764](https://github.com/hstreamdb/hstream/pull/1764)
- [build] Workaround for building hstream with system-provided jemalloc [#1769](https://github.com/hstreamdb/hstream/pull/1769), [#1790](https://github.com/hstreamdb/hstream/pull/1790) 
- [build] Use system's jemalloc [#1770](https://github.com/hstreamdb/hstream/pull/1770) 
- [build] Improve support for arm64 build in dockerfile [#1786](https://github.com/hstreamdb/hstream/pull/1786)
- [ci] Add new Kafka test suits [#1792](https://github.com/hstreamdb/hstream/pull/1792)   
 
### Changes and Bug fixes

- Fix the concurrent issue in withOffsetN [#1784](https://github.com/hstreamdb/hstream/pull/1784) 
- Fix parsing AdvertisedListeners [#1791](https://github.com/hstreamdb/hstream/pull/1791)
- Fix the bug of re startReading while the fetch response is empty [#1794](https://github.com/hstreamdb/hstream/pull/1794)
- Workarounds: make the size of replicaNodes equal to the replicationFactor [#1798](https://github.com/hstreamdb/hstream/pull/1798)

---

HStream gRPC Server

- [io] Add alterConnectorConfig [#1742](https://github.com/hstreamdb/hstream/pull/1742)
- [io] Add fixed-connector-image options [#1752](https://github.com/hstreamdb/hstream/pull/1752)
- [io] Improve logs [#1785](https://github.com/hstreamdb/hstream/pull/1785)
- [query] Fix incorrect metadata of queries with namespace [#1744](https://github.com/hstreamdb/hstream/pull/1744)
- [query] Trim log which stores changelog of query state after snapshotting [#1745](https://github.com/hstreamdb/hstream/pull/1745)
- Return empty if trimShards request send empty recordIds [#1797](https://github.com/hstreamdb/hstream/pull/1797)  
- Catch exception for stats handler and gossip nodeChangeEvent [#1760](https://github.com/hstreamdb/hstream/pull/1760)
- Log errors for all unexpected exceptions in Gossip asyncs [#1759](https://github.com/hstreamdb/hstream/pull/1759)


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
 
