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
import           HStream.Utils                 (TaskStatus (..), textToCBytes)

paths :: [CBytes]
paths = [ textToCBytes rootPath
        , textToCBytes ioRootPath
        , textToCBytes $ myRootPath @TaskIdMeta       @ZHandle
        , textToCBytes $ myRootPath @TaskMeta         @ZHandle
        , textToCBytes $ myRootPath @TaskKvMeta       @ZHandle
        , textToCBytes $ myRootPath @ShardReader      @ZHandle
        , textToCBytes $ myRootPath @QueryInfo        @ZHandle
        , textToCBytes $ myRootPath @QueryStatus      @ZHandle
        , textToCBytes $ myRootPath @SubscriptionWrap @ZHandle
        , textToCBytes $ myRootPath @Proto.Timestamp  @ZHandle
        , textToCBytes $ myRootPath @TaskAllocation   @ZHandle
        ]

tables :: [Text]
tables = [
    myRootPath @TaskMeta         @RHandle
  , myRootPath @TaskIdMeta       @RHandle
  , myRootPath @QueryInfo        @RHandle
  , myRootPath @QueryStatus       @RHandle
  , myRootPath @ShardReader      @RHandle
  , myRootPath @SubscriptionWrap @RHandle
  , myRootPath @Proto.Timestamp  @RHandle
  , myRootPath @TaskAllocation @RHandle
  ]

clusterStartTimeId :: Text
clusterStartTimeId = "Cluster_Uptime"
