# ------------------------------------------------------------------------------

FROM hstreamdb/haskell:8.10 as dependencies-meta
COPY . /opt
WORKDIR /opt
RUN rm -rf /srv/* && \
    cp /opt/cabal.project /srv/cabal.project && \
    find . -type f -name "*.cabal" | xargs cp --parents -t /srv

# ------------------------------------------------------------------------------

FROM hstreamdb/haskell:8.10 as dependencies

ARG cabal_mirror_name="hackage.haskell.org"
ARG cabal_mirror_url="http://hackage.haskell.org/"

RUN cabal user-config init && echo "\
repository $cabal_mirror_name \n\
  url: $cabal_mirror_url \n\
" > /root/.cabal/config && cabal user-config update && cabal update

COPY --from=dependencies-meta /srv /srv
RUN cd /srv && cabal update && cabal build --dependencies-only all

# ------------------------------------------------------------------------------

FROM hstreamdb/haskell:8.10 as builder

COPY --from=dependencies /srv/dist-newstyle /srv/dist-newstyle
COPY --from=dependencies /root/.cabal /root/.cabal
COPY . /srv
RUN cd /srv && cabal install hstream

# ------------------------------------------------------------------------------

FROM ubuntu:bionic

RUN apt-get update && apt-get install -y --no-install-recommends \
      libunwind8                     \
      libdwarf1                      \
      libboost-dev                   \
      libboost-chrono1.65.1          \
      libboost-date-time1.65.1       \
      libboost-atomic1.65.1          \
      libboost-filesystem1.65.1      \
      libboost-program-options1.65.1 \
      libboost-regex1.65.1           \
      libboost-thread1.65.1          \
      libboost-python1.65.1          \
      libboost-context-dev           \
      libssl-dev                     \
      libevent-dev                   \
      libevent-openssl-2.1-6         \
      libdouble-conversion-dev       \
      libzookeeper-mt2               \
      libgoogle-glog-dev             \
      libjemalloc1                   \
      libsnappy1v5                   \
      libpython3.6                   \
      libsodium-dev                  \
      libzstd-dev                    \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

COPY --from=hstreamdb/logdevice-client:latest /usr/local/lib/ /usr/local/lib/

RUN ln -sr /usr/local/lib/librocksdb.so.6.6.1 /usr/local/lib/librocksdb.so.6 && \
    ln -sr /usr/local/lib/librocksdb.so.6.6.1 /usr/local/lib/librocksdb.so

COPY --from=builder /root/.cabal/bin/hstream-server \
                    /root/.cabal/bin/hstream-client \
                    /usr/local/bin/

ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

EXPOSE 6560 6570
CMD ["/usr/local/bin/hstream-server", "-p", "6570"]
