module HStream.Server.MetaData.Value where

import           Data.Text                     (Text)
import           Z.Data.CBytes                 (CBytes)
import           ZooKeeper.Types               (ZHandle)

import           HStream.IO.Types
import           HStream.MetaStore.Types       (HasPath (myRootPath),
                                                RHandle (..))
import           HStream.Server.MetaData.Types
import           HStream.Server.Types          (SubscriptionWrap (..))
import           HStream.Utils                 (textToCBytes)

ioKvPath :: CBytes
ioKvPath = textToCBytes ioRootPath <> "/kv"

paths :: [CBytes]
paths = [ textToCBytes rootPath
        , textToCBytes ioRootPath
        , textToCBytes ioRootPath
        , ioKvPath
        , textToCBytes $ myRootPath @TaskIdMeta       @ZHandle
        , textToCBytes $ myRootPath @TaskMeta         @ZHandle
        , textToCBytes $ myRootPath @ShardReader      @ZHandle
        , textToCBytes $ myRootPath @PersistentQuery  @ZHandle
        , textToCBytes $ myRootPath @SubscriptionWrap @ZHandle
        ]

tables :: [Text]
tables = [
    myRootPath @TaskMeta         @RHandle
  , myRootPath @TaskIdMeta       @RHandle
  , myRootPath @PersistentQuery  @RHandle
  , myRootPath @ShardReader      @RHandle
  , myRootPath @SubscriptionWrap @RHandle
  ]
