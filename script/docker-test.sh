#!/bin/bash
#
# You can customize options through environments, e.g.
#
# $ CONTAINER_BIN=podman WORKDIR=/tmp/hstream-test script/docker-test.sh

set -e

SRC_DIR=${SRC_DIR:-$(pwd)}
WORKDIR=${WORKDIR:-$(mktemp --directory)}
STORAGE_DIR=${STORAGE_DIR:-$(mktemp --directory)}

GHC_VERSION=${GHC_VERSION:-8.10}
USE_STABLE_IMAGE=${USE_STABLE_IMAGE:-""}
if [ -n "$USE_STABLE_IMAGE" ]; then
    HS_IMAGE="hstreamdb/haskell:$GHC_VERSION-stable"
    STORE_SERVER_IMAGE="hstreamdb/logdevice:2.46.5"
else
    HS_IMAGE="hstreamdb/haskell:$GHC_VERSION"
    STORE_SERVER_IMAGE="hstreamdb/logdevice"
fi
IGNORE_CABAL_UPDATE=${IGNORE_CABAL_UPDATE:-""}
CONTAINER_BIN="${CONTAINER_BIN:-docker}"

SERVER_CONTAINER_NAME="ci-server-$GHC_VERSION"
STORAGE_SERVER_CONTAINER_NAME="ci-store-$GHC_VERSION"
TEST_CONTAINER_NAME="ci-test-$GHC_VERSION"

log_info() {
    echo -e "\033[96m$@\033[0m"
}

container_run() {
    $CONTAINER_BIN run --rm \
        -v $SRC_DIR:$SRC_DIR \
        -v $HOME/.cabal:$HOME/.cabal \
        -v $WORKDIR:$WORKDIR \
        -w $SRC_DIR $HS_IMAGE $@
}

pack_and_unpack() {
    container_run cabal sdist all
    container_run chown -R $(id -u):$(id -g) $SRC_DIR
    mkdir -p $WORKDIR && container_run chown -R $(id -u):$(id -g) $WORKDIR && rm -rf $WORKDIR/*

    cd $WORKDIR/
    find $SRC_DIR/dist-newstyle/sdist -maxdepth 1 -type f -name '*.tar.gz' -exec tar -xvf '{}' \;
    find $SRC_DIR/dist-newstyle/sdist -maxdepth 1 -type f -name '*.tar.gz' -exec rm       '{}' \;
    echo "packages: */*.cabal" >> cabal.project
    cp ${SRC_DIR}/hstream/config.example.yaml ${WORKDIR}/config.example.yaml

    mkdir -p $WORKDIR/dist-newstyle/build
    cp -r $SRC_DIR/dist-newstyle/* $WORKDIR/dist-newstyle/
    # FIXME: Sometimes we need rebuild hstream-store, hstream-sql
    find $WORKDIR/dist-newstyle/build/ -type d | grep hstream-store | xargs rm -rf
    find $WORKDIR/dist-newstyle/build/ -type d | grep hstream-sql | xargs rm -rf
}

start_tester_container() {
    $CONTAINER_BIN run -td --rm \
        --name $TEST_CONTAINER_NAME \
        -e LC_ALL=C.UTF-8 \
        -e HOME=$HOME \
        -v $HOME/.cabal:$HOME/.cabal \
        -v $WORKDIR:/srv \
        -v $STORAGE_DIR:/data/store \
        -w /srv $HS_IMAGE bash
}

start_storage_server_container() {
    $CONTAINER_BIN run -td --rm \
        --name $STORAGE_SERVER_CONTAINER_NAME \
        --network container:$TEST_CONTAINER_NAME \
        -v $STORAGE_DIR:/data/store \
        $STORE_SERVER_IMAGE /usr/local/bin/ld-dev-cluster --root /data/store --use-tcp

    # NOTE: Here we sleep 2 seconds to wait the server start.
    sleep 2
}

run_cabal_build_all() {
    test "$IGNORE_CABAL_UPDATE" || $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal update
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cat cabal.project || true
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal build --upgrade-dependencies --only-dependencies --enable-tests --enable-benchmarks all
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal build --enable-tests --enable-benchmarks all
}

run_cabal_test_all() {
    log_info "======== hstream-store ========"
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal test --test-show-details=always hstream-store

    log_info "======== hstream-sql ========"
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal test --test-show-details=always hstream-sql

    log_info "======== hstream-processing ========"
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal test --test-show-details=always hstream-processing

    log_info "======== hstream ========"
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal test --test-show-details=always hstream
}

run_check_all() {
    # unfortunately, there is no `cabal check all`
    log_info "Run all cabal check..."
    # Note that we ignore hstream-store package to run cabal check, because there
    # is an unexpected warning:
    #   ...
    #   Warning: 'cpp-options': -std=c++17 is not portable C-preprocessor flag
    #   Warning: Hackage would reject this package.
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME bash -c \
        "find . -maxdepth 1 -type d -not -path './dist*' -not -path '.' -not -path './hstream-store*' | xargs -I % bash -c 'cd % && echo checking %... && cabal check'"

    log_info "Run cabal haddock"
    $CONTAINER_BIN exec $TEST_CONTAINER_NAME cabal haddock --flag server-tests --enable-tests --enable-benchmarks all
}

try_release_container() {
    $CONTAINER_BIN rm -f $STORAGE_SERVER_CONTAINER_NAME || true
    $CONTAINER_BIN rm -f $SERVER_CONTAINER_NAME || true
    $CONTAINER_BIN rm -f $TEST_CONTAINER_NAME || true
}

# --------------------------------------

try_release_container &> /dev/null

log_info "Pack & Unpack packages from $SRC_DIR to $WORKDIR..."
pack_and_unpack

log_info "Start tester container from $HS_IMAGE..."
start_tester_container

log_info "Start store server from $STORE_SERVER_IMAGE..."
start_storage_server_container

# TODO
#log_info "Start main server..."
#start_server_container

log_info "Build from $WORKDIR..."
run_cabal_build_all

log_info "Test all..."
run_cabal_test_all

run_check_all

container_run chown -R $(id -u):$(id -g) $WORKDIR
container_run chown -R $(id -u):$(id -g) $HOME/.cabal

log_info "Save dist-newstyle..."
cp -r $WORKDIR/dist-newstyle/* $SRC_DIR/dist-newstyle

try_release_container &> /dev/null
