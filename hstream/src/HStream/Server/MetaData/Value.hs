module HStream.Server.MetaData.Value where

import           Data.Text                     (Text)
import           Z.Data.CBytes                 (CBytes)
import           ZooKeeper.Types               (ZHandle)

import           HStream.IO.Types
import           HStream.MetaStore.Types       (HasPath (myRootPath),
                                                RHandle (..))
import           HStream.Server.MetaData.Types
import           HStream.Server.Types          (SubscriptionWrap (..))
import qualified HStream.ThirdParty.Protobuf   as Proto
import           HStream.Utils                 (textToCBytes)

paths :: [CBytes]
paths = [ textToCBytes rootPath
        , textToCBytes ioRootPath
        , textToCBytes $ myRootPath @TaskIdMeta       @ZHandle
        , textToCBytes $ myRootPath @TaskMeta         @ZHandle
        , textToCBytes $ myRootPath @TaskKvMeta       @ZHandle
        , textToCBytes $ myRootPath @ShardReader      @ZHandle
        , textToCBytes $ myRootPath @PersistentQuery  @ZHandle
        , textToCBytes $ myRootPath @SubscriptionWrap @ZHandle
        , textToCBytes $ myRootPath @Proto.Timestamp  @ZHandle
        ]

tables :: [Text]
tables = [
    myRootPath @TaskMeta         @RHandle
  , myRootPath @TaskIdMeta       @RHandle
  , myRootPath @PersistentQuery  @RHandle
  , myRootPath @ShardReader      @RHandle
  , myRootPath @SubscriptionWrap @RHandle
  , myRootPath @Proto.Timestamp  @RHandle
  ]

clusterStartTimeId :: Text
clusterStartTimeId = "Cluster_Uptime"
