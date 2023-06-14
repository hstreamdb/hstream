[![GitHub top language](https://img.shields.io/github/languages/top/hstreamdb/hstream)](https://www.haskell.org/)
[![ci](https://github.com/hstreamdb/hstream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/hstreamdb/hstream/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/hstreamdb/hstream)](https://hub.docker.com/r/hstreamdb/hstream)
[![Slack](https://img.shields.io/badge/Slack-HStreamDB-39AE85?logo=slack)](https://slack-invite.hstream.io/)
[![Twitter](https://img.shields.io/badge/Follow-HStreamDB-1DA1F2?logo=twitter)](https://twitter.com/HStreamDB)
[![Community](https://img.shields.io/badge/Community-HStreamDB-yellow?logo=github)](https://github.com/hstreamdb/hstream/discussions)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

# HStreamDB

HStreamDB is an open-source, cloud-native streaming database for IoT and beyond.
Modernize your data stack for real-time applications.

![hstream-db](https://assets.emqx.com/images/hstreamdb-hstream-github-readme-2022121402.png)

## Main Features

- **Push real-time data to your apps**

  By subscribing to streams in HStreamDB, any update of the data stream will be
  pushed to your apps in real-time, and this promotes your apps to be more
  responsive.

  You can also replace message brokers with HStreamDB and everything you do with
  message brokers can be done better with HStreamDB.

- **Stream processing with familiar SQL**

  HStreamDB provides built-in support for event time-based stream processing.
  You can use your familiar SQL to perform basic filtering and transformation
  operations, statistics and aggregation based on multiple kinds of time windows
  and even joining between multiple streams.

- **Easy integration with a variety of external systems**

  With connectors provided, you can easily integrate HStreamDB with other
  external systems, such as MQTT Broker, MySQL, Redis and ElasticSearch. More
  connectors will be added.

- **Real-time query based on live materialized views**

  With maintaining materialized views incrementally, HStreamDB enables you to
  gain ahead-of-the-curve data insights that respond to your business quickly.

- **Reliable persistent storage with low latency**

  With an optimized storage design based on [LogDevice](https://logdevice.io/),
  not only can HStreamDB provide reliable and persistent storage but also
  guarantee excellent performance despite large amounts of data written to it.

- **Seamless scaling and high availability**

  With the architecture that separates compute from storage, both compute and
  storage layers of HStreamDB can be independently scaled seamlessly. And with
  the consensus algorithm based on the optimized Paxos, data is securely
  replicated to multiple nodes which ensures the high availability of our
  system.

For more information, please visit [HStreamDB homepage](https://hstream.io).

## Quickstart

**For detailed instructions, follow
[HStreamDB quickstart](https://docs.hstream.io/start/quickstart-with-docker.html).**

1. [Install HStreamDB](https://docs.hstream.io/start/quickstart-with-docker.html#installation).
2. [Start a local standalone HStream server](https://docs.hstream.io/start/quickstart-with-docker.html#start-hstreamdb-services).
3. [Start HStreamDB's interactive CLI](https://docs.hstream.io/start/quickstart-with-docker.html#start-hstreamdb-s-interactive-sql-cli)
   and
   [create your first stream](https://docs.hstream.io/start/quickstart-with-docker.html#create-a-stream).
4. [Run a continuous query](https://docs.hstream.io/start/quickstart-with-docker.html#run-a-continuous-query-over-the-stream).
5. [Start another interactive CLI](https://docs.hstream.io/start/quickstart-with-docker.html#start-another-cli-session),
   then
   [insert some data into the stream and get query results](https://docs.hstream.io/start/quickstart-with-docker.html#insert-data-into-the-stream).

## Documentation

Check out [the documentation](https://hstream.io/docs/en/latest/).

## Community, Discussion, Construction and Support

You can reach the HStreamDB community and developers via the following channels:

- [Slack](https://slack-invite.hstream.io)
- [Twitter](https://twitter.com/HStreamDB)
- [Reddit](https://www.reddit.com/r/HStreamDB)

Please submit any bugs, issues, and feature requests to
[hstreamdb/hstream](https://github.com/hstreamdb/hstream/issues).

## How to build (for developers only)

**Pre-requirements**

1. You have `python3` and `docker` installed.
2. [Optional] You can run `docker` without `sudo`. For details, see
   [this docs](https://docs.docker.com/engine/install/linux-postinstall/)
3. [Optional] You can clone the GitHub repository by ssh key.

**Get the source code**

```sh
git clone --recursive git@github.com:hstreamdb/hstream.git
cd hstream/
```

**Update images**

```sh
script/dev-tools update-images
```

**Start all required services**

You must have all required services started before entering an interactive shell
to do further development (especially for running tests).

```sh
script/dev-tools start-services
```

To see information about all started services, run

```sh
script/dev-tools info
```

> _All datas are stored under `your-project-root/local-data`_

**Enter in an interactive shell**

```sh
script/dev-tools shell
```

**Build as other Haskell projects**

_Inside the interactive shell, you have all extra dependencies installed._

```
I have no name!@649bc6bb75ed:~$ cabal update
I have no name!@649bc6bb75ed:~$ make
I have no name!@649bc6bb75ed:~$ cabal build all
```

## License

HStreamDB is under the BSD 3-Clause license. See the
[LICENSE](https://github.com/hstreamdb/hstream/blob/master/LICENSE) file for
details.

## Acknowledgments

- Thanks [LogDevice](https://logdevice.io/) for the powerful storage engine.
