ARG BUILDER_IMAGE="hstreamdb/haskell"
FROM ${BUILDER_IMAGE} as builder

COPY . /hstream
ARG BUILD_TYPE="ReleaseBuild"

RUN cabal update && \
    cd /hstream && make && \
    cabal build --flags "${BUILD_TYPE}" all && \
    cabal install --flags "${BUILD_TYPE}" hstream hstream-admin hstream-store && \
    rm -rf /hstream

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
      libstatgrab10                  \
      python3                        \
      libpython3.8                   \
      python3-pip                    \
      bash-completion                \
      vim                            \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

COPY --from=docker:dind /usr/local/bin/docker /usr/local/bin/

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
                    /root/.cabal/bin/hstream \
                    /root/.cabal/bin/hadmin \
                    /root/.cabal/bin/hstore-bench-writter \
                    /usr/local/bin/
COPY ./script/wait-for-storage.sh /usr/local/script/wait-for-storage.sh
COPY ./conf/hstream.yaml /etc/hstream/config.yaml

RUN mkdir -p /etc/bash_completion.d && \
    grep -wq '^source /etc/profile.d/bash_completion.sh' /etc/bash.bashrc || echo 'source /etc/profile.d/bash_completion.sh' >> /etc/bash.bashrc && \
    /usr/local/bin/hadmin --bash-completion-script /usr/local/bin/hadmin > /etc/bash_completion.d/hadmin

EXPOSE 6560 6570
CMD ["/usr/local/bin/hstream-server", "-p", "6570"]
