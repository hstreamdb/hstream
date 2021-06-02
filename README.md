[![GitHub top language](https://img.shields.io/github/languages/top/hstreamdb/hstream)](https://www.haskell.org/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/hstreamdb/hstream/CI)](https://github.com/hstreamdb/hstream/actions?query=workflow%3ACI)
[![Docker Pulls](https://img.shields.io/docker/pulls/hstreamdb/hstream)](https://hub.docker.com/r/hstreamdb/hstream)
[![Slack Invite](<https://slack-invite.hstream.io/badge.svg>)](https://slack-invite.hstream.io)
[![Twitter](https://img.shields.io/badge/Follow-HStreamDB-1DA1F2?logo=twitter)](https://twitter.com/HStreamDB)
[![Community](https://img.shields.io/badge/Community-HStreamDB-yellow?logo=github)](https://github.com/hstreamdb/hstream/discussions)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

# HStreamDB

![hstream-db](https://cdn.jsdelivr.net/gh/hstreamdb/hstreamio-cdn@1.0.2/images/hstream-db.png)

The database built for IoT streaming data storage and real-time stream processing.

## Main Features

- __Push real-time data to your apps__

    By subscribing to streams in HStreamDB, any update of the data stream will be pushed to your apps in real time, and this promotes your apps to be more responsive.

    You can also replace message brokers with HStreamDB and everything you do with message brokers can be done better with HStreamDB.

- __Stream processing with familiar SQL__

    HStreamDB provides built-in support for event-time based stream processing. You can use your familiar SQL to perform basic filtering and transformation operations, statistics and aggregation based on multiple kinds of time windows and even joining between multiple streams.

- __Easy integration with a variety of external systems__

    With connectors provided, you can easily integrate HStreamDB with other external systems, such as MQTT Broker, MySQL, Redis and ElasticSearch. More connectors will be added.

- __Real-time query based on live materailze views__

    With maintaining materialized views incrementally, HStreamDB enables you to gain ahead-of-the-curve data insights that response to your business quickly.

- __Reliable persistent storage with low latency__

    With an optimized storage design based on [LogDevice](https://logdevice.io/), not only can HStreamDB provide reliable and persistent storage but also guarantee excellent performance despite large amounts of data written to it.

- __Seamless scaling and high availability__

    With the architecture that separates compute from storage, both compute and storage layers of HStreamDB can be independently scaled seamlessly. And with the consensus algorithm based on the optimized Paxos, data is securely replicated to multiple nodes which ensures high availability of our system.

For more information, please visit [HStreamDB homepage](https://hstream.io).

## Installation

### Install via HStreamDB Docker Image

```sh
docker pull hstreamdb/logdevice
docker pull hstreamdb/hstream
```

## Quickstart

**For detailed instructions, follow [HStreamDB quickstart](https://docs.hstream.io/start/quickstart-with-docker/).**

1. [Install HStreamDB](https://docs.hstream.io/start/quickstart-with-docker/#installation).
2. [Start a local standalone HStream server](https://docs.hstream.io/start/quickstart-with-docker/#start-a-local-standalone-hstream-server-in-docker).
3. [Start HStreamDB's interactive CLI](https://docs.hstream.io/start/quickstart-with-docker/#start-hstreamdbs-interactive-sql-cli) and [create your first stream](https://docs.hstream.io/start/quickstart-with-docker/#create-a-stream).
4. [Run a continuous query](https://docs.hstream.io/start/quickstart-with-docker/#run-a-continuous-query-over-the-stream).
5. [Start another interactive CLI](https://docs.hstream.io/start/quickstart-with-docker/#start-another-cli-session), then [insert some data into the stream and get query results](https://docs.hstream.io/start/quickstart-with-docker/#insert-data-into-the-stream).

## Documentation

Check out [the documentation](https://docs.hstream.io/).

## Community, Discussion, Construction and Support

You can reach the HStreamDB community and developers via the following channels:

- [Slack](https://slack-invite.hstream.io)
- [Twitter](https://twitter.com/HStreamDB)
- [Reddit](https://www.reddit.com/r/HStreamDB)

Please submit any bugs, issues, and feature requests to [hstreamdb/hstream](https://github.com/hstreamdb/hstream/issues).


## How to build (for developers only)

**Pre-requirements**

1. Make sure you have Docker installed, and can run `docker` as a non-root user.
2. You have `python3` installed.
3. Your can clone Github repository by ssh key.

**Get the source code**

```sh
git clone --recursive git@github.com:hstreamdb/hstream.git
cd hstream/
```

**Update images**

```sh
script/dev-tools update-images
```

**Start dev-cluster**

*A dev-cluster is required while running tests. All datas are stored under `your-project-root/local-data/logdevice`*

```sh
script/dev-tools cluster-start
```

**Enter in an interactive shell**

```sh
script/dev-tools shell
```

**Build as other haskell projects**

*Inside the interactive shell, you have all extra dependencies installed.*

```
I have no name!@649bc6bb75ed:~$ make
I have no name!@649bc6bb75ed:~$ cabal build all
```


## License

HStreamDB is under the BSD 3-Clause license. See the [LICENSE](https://github.com/hstreamdb/hstream/blob/master/LICENSE) file for details.

## Acknowledgments

- Thanks [LogDevice](https://logdevice.io/) for the powerful storage engine.
