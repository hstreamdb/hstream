#!/bin/bash

find . -type f -not -path "./dist-newstyle/*" -not -path "*/.stack-work/*" | \
    grep "\.l\?hs$" | xargs stylish-haskell -c .stylish-haskell.yaml -i
