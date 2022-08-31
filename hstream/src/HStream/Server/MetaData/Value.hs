module HStream.Server.MetaData.Value where

import           Z.Data.CBytes (CBytes)

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

ioPath :: CBytes
ioPath = rootPath <> "/io"

ioTasksPath :: CBytes
ioTasksPath = ioPath <> "/tasks"

ioStatusPath :: CBytes
ioStatusPath = ioPath <> "/status"

ioKvPath :: CBytes
ioKvPath = ioPath <> "/kv"

ioNamesPath :: CBytes
ioNamesPath = ioPath <> "/names"

paths :: [CBytes]
paths = [ rootPath
        , serverRootPath
        , lockPath
        , queriesPath
        , ioPath
        , ioTasksPath
        , ioStatusPath
        , ioKvPath
        , ioNamesPath
        , subscriptionsPath
        , subscriptionsLockPath
        , readerPath
        ]
