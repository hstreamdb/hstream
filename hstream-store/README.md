HStream-Store
=============

## Build with docker image

Start container:

```sh
cd hstream/

docker run -td --name hstream-store-dev --rm -v $(pwd):/srv -w /srv hstreamdb/haskell bash
# or with your local cabal cache
docker run -td --name hstream-store-dev --rm -v ~/.cabal:/root/.cabal -v $(pwd):/srv -w /srv hstreamdb/haskell bash
```

Enter inside:

```sh
docker exec -it hstream-store-dev bash
```

Now you can run build just as in your host machine:

```sh
cabal build hstream-store --ghc-options '-optcxx-std=c++17'
```
