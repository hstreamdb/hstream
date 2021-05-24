#!/bin/bash
set -e

GHC=${GHC:-latest}
COMPONENT="$1"

echo "====== Test $COMPONENT ======"
python3 script/dev-tools cabal --check --no-interactive -i docker.io/hstreamdb/haskell:$GHC \
    -- test --test-show-details=direct $COMPONENT
