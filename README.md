# HStreamDB

[![GitHub top language](https://img.shields.io/github/languages/top/hstreamdb/hstream)](https://www.haskell.org/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/hstreamdb/hstream/CI)](https://github.com/hstreamdb/hstream/actions?query=workflow%3ACI)
[![Docker Pulls](https://img.shields.io/docker/pulls/hstreamdb/hstream)](https://hub.docker.com/r/hstreamdb/hstream)

The database built for IoT streaming data storage and real-time stream processing.

![hstream-db](https://cdn.jsdelivr.net/gh/hstreamdb/hstreamio-cdn@1.0.2/images/hstream-db.png)

- __High Throughput__

    By maximizing the use of network and disk, message delivery of HStreamDB achieves extremely high throughput.

- __Compatible with Redis Stream APIs__

    The client API of HStreamDB is fully compatible with Redis stream data type, and you can interact with it by any of Redis clients with standard Redis stream commands.

- __Persistent Storage with Low Lentency__

    The message storage of HStreamDB is based on optimized RocksDB and provides extremely low write latency.

- __Millions of MQTT Topics on a Single Node__

    The storage design of HStreamDB is not sensitive to the number of topics, and it is able to support millions of MQTT topics on a single server.

For more information, please visit [HStreamDB homepage](https://hstream.io).

## Installation

#### Install via HStreamDB Docker Image

```sh
docker pull hstreamdb/hstream
```

#### Build from Source

See the [documentation](https://docs.hstream.io/development/build-from-source/).

## Getting Started

### Start HStreamDB Server

#### 1. Create a bridge network

The bridge network will enable the containers to communicate as a single cluster while keeping them isolated from external networks.

```sh
docker network create -d bridge some-hstreamdb-net
```

> :warning: If you expose the port outside of your host, it will be open to anyone.

#### 2. Start the server

```sh
docker run -d --network some-hstreamdb-net --name some-hstreamdb hstreamdb/hstream
```

> **_NOTE:_**
> 1. Datas are stored in the `VOLUME /data`, which can be used with `-v /your/host/dir:/data` (see [use volumes](https://docs.docker.com/storage/volumes)).
> 2. Default lisenling port is `6560`, you can expose the port outside of your host (e.g., via `-p` on `docker run`).

### Connecting via redis-cli

```sh
docker run -it --rm --network some-hstreamdb-net redis redis-cli -h some-hstreamdb -p 6560
```

```
some-hstreamdb:6560> xadd users * name alice age 20
"1599444243554-0"
some-hstreamdb:6560> xadd users * name bob age 20
"1599444249940-0"
some-hstreamdb:6560> xrange users - +
1) 1) "1599444243554-0"
   2) 1) "name"
      2) "alice"
      3) "age"
      4) "20"
2) 1) "1599444249940-0"
   2) 1) "name"
      2) "bob"
      3) "age"
      4) "20"
```

## Community, Discussion, Construction and Support

You can reach the HStreamDB community and developers via the following channels:

- [Slack](https://slack-invite.hstream.io)
- [Twitter](https://twitter.com/HStreamDB)
- [Reddit](https://www.reddit.com/r/HStreamDB)

Please submit any bugs, issues, and feature requests to [hstreamdb/hstream](https://github.com/hstreamdb/hstream/issues).

## License

HStreamDB is under the BSD 3-Clause license. See the [LICENSE](https://github.com/hstreamdb/hstream/blob/master/LICENSE) file for details.
