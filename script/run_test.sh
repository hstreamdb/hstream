#!/bin/bash
set -e

GHC=${GHC:-latest}

echo "====== Test hstream-store ======"
python3 script/dev-tools cabal --check --no-interactive -i docker.io/hstreamdb/haskell:$GHC \
    -- test --test-show-details=direct hstream-store

echo "====== Test hstream-sql ======"
python3 script/dev-tools cabal --check --no-interactive -i docker.io/hstreamdb/haskell:$GHC \
    -- test --test-show-details=direct hstream-sql

echo "====== Test hstream-processing ======"
python3 script/dev-tools cabal --check --no-interactive -i docker.io/hstreamdb/haskell:$GHC \
    -- test --test-show-details=direct hstream-processing

echo "====== Test hstream ======"
python3 script/dev-tools cabal --check --no-interactive -i docker.io/hstreamdb/haskell:$GHC \
    -- test --test-show-details=direct hstream
