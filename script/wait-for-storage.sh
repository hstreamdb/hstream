#!/bin/bash
set -e

store_admin_host="$1"
store_admin_port="$2"
zk_host="$3"
zk_port="$4"
timeout="$5"    # total timeout
shift 5

until (echo -n > /dev/tcp/$zk_host/$zk_port); do
    >&2 echo "Waiting for zookeeper $zk_host:$zk_port ..."
    sleep 1
    timeout=$((timeout - 1))
    [ $timeout -le 0 ] && echo "Timeout!" && exit 1;
done

until ( \
    echo -n > /dev/tcp/$store_admin_host/$store_admin_port && \
    /usr/local/bin/hadmin --host "$store_admin_host" --port "$store_admin_port" status \
    ) >/dev/null 2>&1;
do
    >&2 echo "Waiting for storage $store_admin_host:$store_admin_port ..."
    sleep 1
    timeout=$((timeout - 1))
    [ $timeout -le 0 ] && echo "Timeout!" && exit 1;
done

>&2 echo "HStore is up - Starting HServer..."
exec "$@"
