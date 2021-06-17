FROM hstreamdb/haskell as builder

ARG CABAL_MIRROR_NAME="hackage.haskell.org"
ARG CABAL_MIRROR_URL="http://hackage.haskell.org/"

RUN cabal user-config init && echo "\
repository $CABAL_MIRROR_NAME \n\
  url: $CABAL_MIRROR_URL \n\
" > /root/.cabal/config && cabal user-config update && \
    cabal update

RUN rm -rf /srv/*
WORKDIR /srv
COPY . /srv
RUN make && \
    cabal build all && \
    cabal install hstream && \
    cabal install hstore-admin

# ------------------------------------------------------------------------------

FROM ubuntu:focal

ENV LANG C.UTF-8
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
      libunwind8                     \
      libatomic1                     \
      libdwarf1                      \
      libboost-dev                   \
      libboost-context-dev           \
      libboost-atomic1.71.0          \
      libboost-chrono1.71.0          \
      libboost-date-time1.71.0       \
      libboost-filesystem1.71.0      \
      libboost-program-options1.71.0 \
      libboost-system1.71.0          \
      libboost-regex1.71.0           \
      libboost-thread1.71.0          \
      libboost-python1.71.0          \
      libssl-dev                     \
      libevent-dev                   \
      libevent-openssl-2.1-7         \
      libdouble-conversion-dev       \
      libzookeeper-mt2               \
      libgoogle-glog-dev             \
      libsnappy1v5                   \
      libsodium-dev                  \
      libzstd-dev                    \
      libmysqlclient-dev             \
      python3                        \
      libpython3.8                   \
      python3-pip                    \
      bash-completion                \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

COPY --from=hstreamdb/haskell:latest /usr/local/lib/ /usr/local/lib/
COPY --from=hstreamdb/haskell:latest /usr/lib/libjemalloc.so.2 /usr/lib/libjemalloc.so.2
RUN ln -sr /usr/lib/libjemalloc.so.2 /usr/lib/libjemalloc.so

COPY --from=hstreamdb/logdevice:latest /usr/local/bin/logdeviced \
                                       /usr/local/bin/ld-dev-cluster \
                                       /usr/local/bin/ld-admin-server \
                                       /usr/local/bin/
# ld-dev-cluster requires this
COPY --from=hstreamdb/logdevice /logdevice/common/test/ssl_certs/ /logdevice/common/test/ssl_certs/

COPY --from=builder /root/.cabal/bin/hstream-server \
                    /root/.cabal/bin/hstream-client \
                    /root/.cabal/bin/hadmin \
                    /usr/local/bin/
RUN mkdir -p /etc/bash_completion.d && \
    grep -wq '^source /etc/profile.d/bash_completion.sh' /etc/bash.bashrc || echo 'source /etc/profile.d/bash_completion.sh' >> /etc/bash.bashrc && \
    /usr/local/bin/hadmin --bash-completion-script /usr/local/bin/hadmin > /etc/bash_completion.d/hadmin

EXPOSE 6560 6570
CMD ["/usr/local/bin/hstream-server", "-p", "6570"]
