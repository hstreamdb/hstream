#!/bin/bash
set -e

SRC_DIR=${SRC_DIR:-$(pwd)}
WORKDIR=${WORKDIR:-$(mktemp --directory)}
GHC_VERSION=${GHC_VERSION:-8.10}
IMAGE_NAME=${IMAGE_NAME:-hstreamdb/haskell}
IMAGE="$IMAGE_NAME:$GHC_VERSION"
CABAL_HOME=${CABAL_HOME:-$HOME/.cabal}

log_info() {
    echo -e "\033[96m$@\033[0m"
}

pack_and_unpack() {
    docker run --rm -v $SRC_DIR:/srv -w /srv $IMAGE cabal sdist all
    mkdir -p $WORKDIR && rm -rf $WORKDIR/*
    cp dist-newstyle/sdist/*.tar.gz $WORKDIR/ && cd $WORKDIR/
    find . -maxdepth 1 -type f -name '*.tar.gz' -exec tar -xvf '{}' \;
    find . -maxdepth 1 -type f -name '*.tar.gz' -exec rm       '{}' \;
    echo "packages: */*.cabal" >> cabal.project
    cp ${SRC_DIR}/hstream/config.example.yaml ${WORKDIR}/config.example.yaml
}

start_builder_container() {
    local container_name=$1
    log_info "Start builder container..."
    docker run -td --rm \
        --name $container_name \
        -e LC_ALL=en_US.UTF-8 \
        -v $CABAL_HOME:/root/.cabal \
        -v $WORKDIR:/srv \
        -w /srv $IMAGE bash
}

run_cabal_build_all() {
    local container_name=$1
    log_info "Install dependencies & Build"
    docker exec $container_name cabal update
    docker exec $container_name cat cabal.project || true
    docker exec $container_name cabal build --flag server-tests --upgrade-dependencies --only-dependencies --enable-tests --enable-benchmarks all
    docker exec $container_name cabal build --flag server-tests --enable-tests --enable-benchmarks all
}

start_server_container() {
    local container_name=$1

    log_info "Start server..."
    docker run -td --rm \
        --name $container_name \
        -v $CABAL_HOME:/root/.cabal \
        -v $WORKDIR:/srv \
        -w /srv $IMAGE \
        cabal exec hstream config.example.yaml

    # FIXME: Here we sleep 2 seconds to wait the server start.
    sleep 2
}

start_tester_container() {
    local container_name=$1
    local linked_server_name=$2
    log_info "Start tester container..."
    docker run -td --rm \
        --name $container_name \
        -e LC_ALL=C.UTF-8 \
        --network container:${linked_server_name} \
        -v $CABAL_HOME:/root/.cabal \
        -v $WORKDIR:/srv \
        -w /srv $IMAGE bash
}

run_cabal_test_all() {
    container_name=$1
    log_info "Run all tests..."
    docker exec $container_name cabal test --flag server-tests --test-show-details=always all
}

run_check_all() {
    container_name=$1

    # unfortunately, there is no `cabal check all`
    log_info "Run all cabal check..."
    docker exec $container_name bash -c "find . -maxdepth 1 -type d -not -path './dist*' -not -path '.' | xargs -I % bash -c 'cd % && echo checking %... && cabal check'"

    log_info "Run cabal haddock"
    docker exec $container_name cabal haddock --flag server-tests --enable-tests --enable-benchmarks all
}

# --------------------------------------

build_container_name="build-$GHC_VERSION"
server_container_name="server-$GHC_VERSION"
test_container_name="test-$GHC_VERSION"

build_all() {
    docker kill $build_container_name || true
    log_info "Cabal build from $WORKDIR, src_dir: $SRC_DIR, ghc_version: $GHC_VERSION"
    pack_and_unpack
    start_builder_container $build_container_name
    run_cabal_build_all $build_container_name
}

# NOTE: you must first run "build_all", and then you can run this "test_all"
test_all() {
    log_info "Cabal test from $WORKDIR, src_dir: $SRC_DIR, ghc_version: $GHC_VERSION"
    docker kill $server_container_name || true
    docker kill $test_container_name || true
    start_server_container $server_container_name
    start_tester_container $test_container_name $server_container_name
    run_cabal_test_all $test_container_name
    run_check_all $test_container_name
}

if [ "$1" == "build" ]; then
    build_all
elif [ "$1" == "test" ]; then
    test_all
else
    build_all
    test_all
fi
