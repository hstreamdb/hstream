HStream-Store
=============

## Build with docker image

### Start container

```sh
cd hstream/
docker run -td --name hstream-store-dev --rm -v $(pwd):/srv -w /srv hstreamdb/haskell bash
```

**NOTE**

You can also run container with the same uid as your host:

```sh
docker run -td --name hstream-store-dev --rm -u $(id -u):$(id -g) -e HOME=$HOME -v ~/.cabal:$HOME/.cabal -v $(pwd):/srv -w /srv hstreamdb/haskell bash
```

### Build hstream-store

Enter inside:

```sh
docker exec -it hstream-store-dev bash
```

Now you can run build just as in your host machine:

```sh
cabal build hstream-store
```


## Run a local logdevice cluster

TODO
