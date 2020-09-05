# ------------------------------------------------------------------------------

FROM hstreamdb/haskell-rocksdb:8.10 as dependencies-meta
COPY . /opt
WORKDIR /opt
RUN rm -rf /srv/* && \
    cp /opt/cabal.project /srv/cabal.project && \
    find . -type f -name "*.cabal" | xargs cp --parents -t /srv

# ------------------------------------------------------------------------------

FROM hstreamdb/haskell-rocksdb:8.10 as dependencies

ARG cabal_mirror_name="hackage.haskell.org"
ARG cabal_mirror_url="http://hackage.haskell.org/"

RUN cabal user-config init && echo "\
repository $cabal_mirror_name \n\
  url: $cabal_mirror_url \n\
" > /root/.cabal/config && cabal user-config update && cabal update

COPY --from=dependencies-meta /srv /srv
RUN cd /srv && cabal update && cabal build --dependencies-only all

# ------------------------------------------------------------------------------

FROM hstreamdb/haskell-rocksdb:8.10 as builder

COPY --from=dependencies /srv/dist-newstyle /srv/dist-newstyle
COPY --from=dependencies /root/.cabal /root/.cabal
COPY . /srv
RUN cd /srv && cabal install hstream

# ------------------------------------------------------------------------------

FROM hstreamdb/haskell-rocksdb:base-runtime

COPY --from=builder /root/.cabal/bin/hstream /usr/local/bin/hstream
RUN mkdir -p /etc/hstream/
COPY hstream/config.example.yaml /etc/hstream/config.example.yaml

EXPOSE 6560
CMD ["/usr/local/bin/hstream", "/etc/hstream/config.example.yaml"]
