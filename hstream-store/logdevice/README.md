# thrift-file

## How to build

1. compile to haskell code

```shell
cabal exec thrift-compiler -- -o TARGET -r thrift-file/logdevice/admin/if/admin.thrift
```

Substitute TARGET with desired output location.

2. add a new haskell module export all desired api. For exmaple, look at this [file](https://github.com/Time-Hu/hslog/blob/main/gen-hs2/AdminAPI.hs).

3. make a `.cabal` file, and add [thrift-lib](https://github.com/facebookincubator/hsthrift) in the dependency

4. cabal build

## How to use

Use `withSocketChannel` in [SocketChannel.hs](https://github.com/facebookincubator/hsthrift/blob/master/lib/Thrift/Channel/SocketChannel.hs)

```hs
import qualified Gen.AdminApi as Admin

{-
  binaryProtocolId :: ProtocolId
  binaryProtocolId = 0

  compactProtocolId :: ProtocolId
  compactProtocolId = 2

  localhost :: HostName
  localhost =
  #ifdef IPV4
    "127.0.0.1"
  #else
    "::1"
  #endif
-}

getVersion :: IO Text
getVersion = withSocketChannel @AdminClient(SocketConfig localhost 0000 binaryProtocolID) Admin.getVersion
```
