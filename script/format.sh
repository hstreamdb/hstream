#!/bin/bash
#
# $ bash script/format.sh ci

FORMATER_BIN=${FORMATER_BIN:-stylish-haskell}

if [ "$1" == "ci" ] || [ -z "$1" ]; then
    find . -type f \
        -not -path "*/dist-newstyle/*" \
        -not -path "*/.stack-work/*" \
        -not -path "*/hstream-processing/*" \
        -not -path "*/z-*" \
        -not -path */hstream-sql/src/HStream/SQL/Abs.hs \
        -not -path */hstream-sql/src/HStream/SQL/Lex.hs \
        -not -path */hstream-sql/src/HStream/SQL/Par.hs \
        -not -path */hstream-sql/src/HStream/SQL/Print.hs \
        -not -path */hstream-sql/src/HStream/SQL/ErrM.hs \
        | grep "\.l\?hs$" | xargs $FORMATER_BIN -c .stylish-haskell.yaml -i
    find ./hstream-processing -type f | grep "\.l\?hs$" | xargs $FORMATER_BIN -c ./hstream-processing/.stylish-haskell.yaml -i
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
