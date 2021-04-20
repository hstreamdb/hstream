# hstream-server

## Introduction

HStream toplevel gRPC server implementation

## Build

### Generate `.hs` file from `.proto` file

**Note: Run the following command to regenerate haskell source file each time you edits `.proto` file**

```bash
cd hstream-server
compile-proto-file --includeDir ./proto --proto HStream/Server/HStreamApi.proto --out ./generated-src
```

### Build with cabal

**Note: you have to set `LD_LIBRARY_PATH` as follows if your `grpc` is installed in a non-default path:**

``` bash
export LD_LIBRARY_PATH=<YOUR_GRPC_LIB_PATH>:$LD_LIBRARY_PATH
```
**Also, you may set `--extra-lib-dirs` when executing cabal commands:**

```bash
cabal build --extra-lib-dirs=<YOUR_GRPC_LIB_PATH>
```

### Run hstream-server and command line client

```bash
## start hstream-server
cabal run --extra-lib-dirs=<YOUR_GRPC_LIB_PATH> hstream-server

## start hstream-client (in project root directory)
cabal run --extra-lib-dirs=<YOUR_GRPC_LIB_PATH> hstream-client
```
