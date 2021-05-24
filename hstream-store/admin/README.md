HStore-Admin
============


## Build

First start dev-cluster:

```sh
dev-tools cluster-start
```

Then enter in the shell:

```sh
dev-tools shell
```

Inside the interactive shell created by `dev-tools`, run

```sh
cd hstream-store/admin
make
cabal build
```
