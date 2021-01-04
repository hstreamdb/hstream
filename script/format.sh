#!/bin/bash

FORMATER_BIN=${FORMATER_BIN:-stylish-haskell}

find . -type f -not -path "./dist-newstyle/*" -not -path "*/.stack-work/*" | \
    grep "\.l\?hs$" | xargs $FORMATER_BIN -c .stylish-haskell.yaml -i
