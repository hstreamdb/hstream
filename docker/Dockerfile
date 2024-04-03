# ------------------------------------------------------------------------------
# Global args (should be on the top of dockerfile)

ARG HS_IMAGE="hstreamdb/haskell:latest"
ARG LD_IMAGE="hstreamdb/logdevice:latest"
# Options: "cache", "no_cache" or "cache_dist"(NOT Recommended)
ARG BUILD_CACHE="cache"

# ------------------------------------------------------------------------------

# FIXME: we cann't use ONBUILD COPY --from=hstreamdb/hstream-builder directly,
# see this issue: https://github.com/moby/buildkit/issues/816
FROM hstreamdb/hstream-builder AS cache

FROM ${HS_IMAGE} as builder_cache
ONBUILD COPY --from=cache /root/.cabal/ /root/.cabal/
ONBUILD COPY --from=cache /usr/local/bin/cabal-store-gc /usr/local/bin/cabal-store-gc

FROM ${HS_IMAGE} as builder_cache_dist
ONBUILD COPY --from=cache /root/.cabal/ /root/.cabal/
ONBUILD COPY --from=cache /hstream/dist-newstyle/ /hstream/dist-newstyle/
ONBUILD COPY --from=cache /usr/local/bin/cabal-store-gc /usr/local/bin/cabal-store-gc

FROM ${HS_IMAGE} as builder_no_cache

# ------------------------------------------------------------------------------

FROM builder_${BUILD_CACHE} as builder

COPY . /hstream

ARG BUILD_TYPE="ReleaseBuild"
# NOTE: Since we ignore the .git directory, the Makefile itself can not get
# the information by git. Which means you should provide version information
# as docker build args.
ARG HSTREAM_VERSION="unknown"
ARG HSTREAM_VERSION_COMMIT="unknown"

RUN rm -rf /root/.cabal/bin && \
    cd /hstream && \
    cabal update && \
    HSTREAM_VERSION=${HSTREAM_VERSION} HSTREAM_VERSION_COMMIT=${HSTREAM_VERSION_COMMIT} make && \
    cabal install --flags "${BUILD_TYPE}" \
      --install-method copy \
      hstream hstream-admin hstream-store && \
    cabal install --flags "${BUILD_TYPE}" \
      --install-method copy \
      --constraint="hstream +hstream_use_grpc_haskell" \
      --constraint="hstream-gossip +hstream_use_grpc_haskell" \
      --program-suffix="-old" \
      hstream

# Since cabal doesn't support gc the store, here we use cabal-store-gc
# See: https://github.com/haskell/cabal/issues/3333
#
# This is only for reducing the size of builder image. Building hstream doesn't
# require this.
#
# FIXME:
#
# 1. ~/.ghc is requred for running 'cabal-store-gc --collect'
#    Because it will call a subprocess "cwd=/root/.ghc /opt/ghc/9.2.8/lib/ghc-9.2.8/bin/ghc-pkg --version"
# 2. Because of this https://github.com/phadej/cabal-extras/issues/75, I use my own fork.
#
# TODO:
#
# - Release a arm64 version of cabal-store-gc
#
# For manually build:
#
#   TEMP_STORE=$(mktemp -d); \
#   git clone https://github.com/4eUeP/cabal-extras.git; \
#   cd cabal-extras; \
#   cabal --store-dir $TEMP_STORE install \
#     --with-compiler ghc \
#     --installdir /usr/local/bin \
#     --install-method copy \
#     cabal-store-gc; \
#   rm -rf /cabal-extras; rm -rf $TEMP_STORE; \
RUN test ${BUILD_CACHE} = "cache" && \
    set -eux; \
    export TEMP_STORE=$(mktemp -d); \
    export ARCH="$(uname -m)"; \
    if [ -z "$(command -v cabal-store-gc)" ]; then \
      CABAL_STORE_GC_URL="https://github.com/4eUeP/cabal-extras/releases/download/cabal-store-gc/cabal-store-gc-${ARCH}-linux.xz"; \
      curl -fSL "$CABAL_STORE_GC_URL" -o cabal-store-gc.xz; \
      xz -d cabal-store-gc.xz; \
      mv cabal-store-gc /usr/local/bin/cabal-store-gc; \
      chmod +x /usr/local/bin/cabal-store-gc; \
    fi; \
    cd /hstream; \
    cabal-store-gc --add-project-root; \
    mkdir /root/.ghc; \
    cabal-store-gc --collect; \
    rm -rf /root/.cabal/logs; \
    rm -rf /root/.cabal/store/ghc-$(ghc --numeric-version)/hstream*; \
    rm -rf /root/.cabal/store/roots;

# ------------------------------------------------------------------------------

FROM ${LD_IMAGE} as ld_image

# ------------------------------------------------------------------------------

FROM ubuntu:jammy

ENV LANG C.UTF-8
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
      bash-completion                \
      libatomic1                     \
      libboost-atomic1.74.0          \
      libboost-chrono1.74.0          \
      libboost-context-dev           \
      libboost-date-time1.74.0       \
      libboost-dev                   \
      libboost-filesystem1.74.0      \
      libboost-program-options1.74.0 \
      libboost-python1.74.0          \
      libboost-regex1.74.0           \
      libboost-system1.74.0          \
      libboost-thread1.74.0          \
      libcurl4                       \
      libdouble-conversion-dev       \
      libdwarf1                      \
      libevent-dev                   \
      libevent-openssl-2.1-7         \
      libgoogle-glog-dev             \
      libgsasl7                      \
      libjemalloc2                   \
      libmysqlclient-dev             \
      libpython3.10                  \
      librdkafka++1                  \
      libsnappy1v5                   \
      libsodium-dev                  \
      libssl-dev                     \
      libstatgrab10                  \
      libunwind8                     \
      libzookeeper-mt2               \
      libzstd-dev                    \
      python3                        \
      python3-pip                    \
      vim                            \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

COPY --from=docker:dind /usr/local/bin/docker /usr/local/bin/

COPY --from=builder /usr/local/lib/ /usr/local/lib/

COPY --from=ld_image /usr/local/bin/logdeviced \
                     /usr/local/bin/ld-dev-cluster \
                     /usr/local/bin/ld-admin-server \
                     /usr/local/bin/
# ld-dev-cluster requires this
COPY --from=ld_image /logdevice/common/test/ssl_certs/ /logdevice/common/test/ssl_certs/

COPY --from=builder /root/.cabal/bin/hstream-server \
                    /root/.cabal/bin/hstream-server-old \
                    /root/.cabal/bin/hstream-kafka \
                    /root/.cabal/bin/hstream \
                    /root/.cabal/bin/hadmin \
                    /root/.cabal/bin/hstore-bench-writter \
                    /usr/local/bin/hadmin-store \
                    /usr/local/bin/

COPY ./script/wait-for-storage.sh /usr/local/script/wait-for-storage.sh
COPY ./conf/hstream.yaml /etc/hstream/config.yaml

# Run hadmin-store on aarch64 requires libnuma.so.1
RUN ARCH="$(uname -m)" && \
    if [ "$ARCH" = "aarch64" ]; then \
      apt-get update && \
      apt-get install -y --no-install-recommends libnuma1 && \
      rm -rf /var/lib/apt/lists/* && apt-get clean ; \
    fi

RUN mkdir -p /etc/bash_completion.d && \
    grep -wq '^source /etc/profile.d/bash_completion.sh' /etc/bash.bashrc || echo 'source /etc/profile.d/bash_completion.sh' >> /etc/bash.bashrc && \
    /usr/local/bin/hadmin --bash-completion-script /usr/local/bin/hadmin > /etc/bash_completion.d/hadmin && \
    /usr/local/bin/hadmin-store --bash-completion-script /usr/local/bin/hadmin-store > /etc/bash_completion.d/hadmin-store && \
    /usr/local/bin/hstream --bash-completion-script /usr/local/bin/hstream > /etc/bash_completion.d/hstream && \
    /usr/local/bin/hstream-kafka --bash-completion-script /usr/local/bin/hstream-kafka > /etc/bash_completion.d/hstream-kafka

EXPOSE 6560 6570
CMD ["/usr/local/bin/hstream-server", "-p", "6570"]
