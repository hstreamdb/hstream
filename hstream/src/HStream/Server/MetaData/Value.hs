{-# LANGUAGE CPP #-}

module HStream.Server.MetaData.Value where

import           Data.Text                      (Text)
import           Z.Data.CBytes                  (CBytes)

import           HStream.Common.Server.MetaData (TaskAllocation)
import           HStream.Common.ZookeeperClient (ZookeeperClient)
import           HStream.IO.Types
import           HStream.MetaStore.Types        (FHandle, HasPath (myRootPath),
                                                 RHandle (..))
import           HStream.Server.MetaData.Types
import           HStream.Server.Types           (SubscriptionWrap (..))
import qualified HStream.SQL                    as SQL
import qualified HStream.ThirdParty.Protobuf    as Proto
import           HStream.Utils                  (textToCBytes)

paths :: [CBytes]
paths = [ textToCBytes rootPath
        , textToCBytes ioRootPath
        , textToCBytes $ myRootPath @TaskIdMeta       @ZookeeperClient
        , textToCBytes $ myRootPath @TaskMeta         @ZookeeperClient
        , textToCBytes $ myRootPath @TaskKvMeta       @ZookeeperClient
        , textToCBytes $ myRootPath @ShardReaderMeta  @ZookeeperClient
        , textToCBytes $ myRootPath @QueryInfo        @ZookeeperClient
        , textToCBytes $ myRootPath @QueryStatus      @ZookeeperClient
        , textToCBytes $ myRootPath @ViewInfo         @ZookeeperClient
        , textToCBytes $ myRootPath @SubscriptionWrap @ZookeeperClient
        , textToCBytes $ myRootPath @Proto.Timestamp  @ZookeeperClient
        , textToCBytes $ myRootPath @TaskAllocation   @ZookeeperClient
        , textToCBytes $ myRootPath @QVRelation       @ZookeeperClient
#ifdef HStreamEnableSchema
        , textToCBytes $ myRootPath @SQL.Schema       @ZookeeperClient
#endif
        ]

tables :: [Text]
tables = [
    myRootPath @TaskMeta         @RHandle
  , myRootPath @TaskIdMeta       @RHandle
  , myRootPath @TaskKvMeta       @RHandle
  , myRootPath @QueryInfo        @RHandle
  , myRootPath @QueryStatus      @RHandle
  , myRootPath @ViewInfo         @RHandle
  , myRootPath @ShardReaderMeta  @RHandle
  , myRootPath @SubscriptionWrap @RHandle
  , myRootPath @Proto.Timestamp  @RHandle
  , myRootPath @TaskAllocation   @RHandle
  , myRootPath @QVRelation       @RHandle
#ifdef HStreamEnableSchema
  , myRootPath @SQL.Schema       @RHandle
#endif
  ]

fileTables :: [Text]
fileTables = [
    myRootPath @TaskMeta         @FHandle
  , myRootPath @TaskIdMeta       @FHandle
  , myRootPath @QueryInfo        @FHandle
  , myRootPath @QueryStatus      @FHandle
  , myRootPath @ViewInfo         @FHandle
  , myRootPath @ShardReaderMeta  @FHandle
  , myRootPath @SubscriptionWrap @FHandle
  , myRootPath @Proto.Timestamp  @FHandle
  , myRootPath @TaskAllocation   @FHandle
  , myRootPath @QVRelation       @FHandle
#ifdef HStreamEnableSchema
  , myRootPath @SQL.Schema       @FHandle
#endif
  ]
