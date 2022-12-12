#!/bin/bash
#
# $ bash script/format.sh ci

FORMATER_BIN=${FORMATER_BIN:-stylish-haskell}

if [ "$1" == "ci" ] || [ -z "$1" ]; then
    find . -type f \
        -not -path "*/dist-newstyle/*" \
        -not -path "*/.stack-work/*" \
        -not -path "*/external/*" \
        -not -path "*/local*/*" \
        -not -path "*/proto3-*/*" \
        -not -path "*/generated-src/*" \
        -not -path "*/gen-hs2/*" \
        -not -path "*/gen-sql/*" \
        | grep "\.l\?hs$" | xargs $FORMATER_BIN -c .stylish-haskell.yaml -i
elif [ "$1" == "processing" ]; then
    find ./hstream-processing -type f \
        -not -path "*/dist-newstyle/*" \
        -not -path "*/.stack-work/*" \
        | grep "\.l\?hs$" | xargs ormolu -m inplace
    find ./hstream-processing -type f  \
        -not -path "*/dist-newstyle/*" \
        -not -path "*/.stack-work/*" \
        | grep "\.l\?hs$" | xargs $FORMATER_BIN -c ./hstream-processing/.stylish-haskell.yaml -i
    cabal-fmt --inplace ./hstream-processing/hstream-processing.cabal
fi

# Optional
CABAL_FORMATER_BIN="cabal-fmt"

command -v $CABAL_FORMATER_BIN > /dev/null && \
    find -type f -name "*.cabal" \
        -not -path "*/external/*" \
        -not -path "*/local*/*" \
        | xargs $CABAL_FORMATER_BIN -i \
    || true
