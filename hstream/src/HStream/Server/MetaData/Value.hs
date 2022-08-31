module HStream.Server.MetaData.Value where

import           HStream.IO.Types
import           HStream.MetaStore.Types (HasPath (myRootPath))
import           HStream.Utils           (textToCBytes)
import           Z.Data.CBytes           (CBytes)
import           ZooKeeper.Types         (ZHandle)

rootPath :: CBytes
rootPath = "/hstream"

serverRootPath :: CBytes
serverRootPath = rootPath <> "/servers"

lockPath :: CBytes
lockPath = rootPath <> "/lock"

serverRootLockPath :: CBytes
serverRootLockPath = lockPath <> "/servers"

queriesPath :: CBytes
queriesPath = rootPath <> "/queries"

subscriptionsPath :: CBytes
subscriptionsPath = rootPath <> "/subscriptions"

subscriptionsLockPath :: CBytes
subscriptionsLockPath = lockPath <> "/subscriptions"

readerPath :: CBytes
readerPath = rootPath <> "/shardReader"


ioKvPath :: CBytes
ioKvPath = textToCBytes ioRootPath <> "/kv"

paths :: [CBytes]
paths = [ rootPath
        , serverRootPath
        , lockPath
        , queriesPath
        , subscriptionsPath
        , subscriptionsLockPath
        , textToCBytes ioRootPath
        , textToCBytes ioRootPath
        , textToCBytes $ myRootPath @TaskIdMeta @ZHandle
        , textToCBytes $ myRootPath @TaskMeta   @ZHandle
        , ioKvPath
        , readerPath
        ]
