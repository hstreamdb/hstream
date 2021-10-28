#!/bin/bash
set -e

IMAGE=${IMAGE:-"docker.io/hstreamdb/haskell"}
CONTAINER_NAME=${CONTAINER_NAME:-"hstream-server-dev-$(id -u)"}
EXTRA_OPTS=${EXTRA_OPTS:-""}
COMMAND=${COMMAND:-"cabal run -- "}
EXE=${EXE:-"hstream-server"}

python3 script/dev-tools get-info

SERVER_PORT=$(cat local-data/dev_tools.env|grep SERVER_LOCAL_PORT|cut -d '=' -f2)
LD_ADMIN_PORT=$(cat local-data/dev_tools.env|grep STORE_ADMIN_LOCAL_PORT|cut -d '=' -f2)
ZOOKEEPER_PORT=$(cat local-data/dev_tools.env|grep ZOOKEEPER_LOCAL_PORT|cut -d '=' -f2)

python3 script/dev-tools shell $EXTRA_OPTS --command "$COMMAND" \
    --container-name $CONTAINER_NAME -i $IMAGE -- \
    $EXE --port $SERVER_PORT --log-with-color --store-admin-port $LD_ADMIN_PORT --zkuri "127.0.0.1:$ZOOKEEPER_PORT"
