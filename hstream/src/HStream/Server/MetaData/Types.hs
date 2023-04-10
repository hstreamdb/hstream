{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}

module HStream.Server.MetaData.Types
  ( RelatedStreams
  , QueryInfo (..)
  , ViewInfo (..)
  , QueryStatus (QueryRunning, QueryCreating
               , QueryAborted, QueryResuming, QueryPaused, ..)
  , ShardReader (..)
  , TaskAllocation (..)
  , createInsertQueryInfo
  , deleteQueryInfo
  , getSubscriptionWithStream
  , groupbyStores
  , rootPath
  , getQuerySink
  , getQuerySources
  ) where

import           Control.Concurrent
import           Data.Aeson                    (FromJSON (..), ToJSON (..))
import qualified Data.HashMap.Strict           as HM
import           Data.Int                      (Int64)
import           Data.IORef
import           Data.Maybe                    (fromJust)
import           Data.Text                     (Text)
import           Data.Time.Clock.System        (SystemTime (MkSystemTime),
                                                getSystemTime)
import           Data.Word                     (Word32, Word64)
import           GHC.Generics                  (Generic)
import           GHC.IO                        (unsafePerformIO)
import           ZooKeeper.Types               (ZHandle)

import           HStream.MetaStore.Types       (FHandle, HasPath (..),
                                                MetaHandle,
                                                MetaMulti (metaMulti),
                                                MetaStore (..), MetaType,
                                                RHandle)
import qualified HStream.Server.ConnectorTypes as HCT
import           HStream.Server.HStreamApi     (ServerNode (..),
                                                Subscription (..))
import           HStream.Server.Types          (ServerID, SubscriptionWrap (..))
import qualified HStream.SQL.AST               as AST
import qualified HStream.Store                 as S
import qualified HStream.ThirdParty.Protobuf   as Proto
import           HStream.Utils                 (TaskStatus (..), cBytesToText)
#ifdef HStreamUseV2Engine
import           DiffFlow.Types
#else
import qualified HStream.Processing.Stream     as HS
import           HStream.SQL.Codegen.V1
#endif
--------------------------------------------------------------------------------

data QueryInfo = QueryInfo
  { queryId          :: Text
  , querySql         :: Text
  , queryCreatedTime :: Int64
  , queryStreams     :: RelatedStreams
  } deriving (Generic, Show, FromJSON, ToJSON)

data QueryStatus = QueryStatus { queryState :: TaskStatus }
  deriving (Generic, Show, FromJSON, ToJSON)

pattern QueryCreating, QueryRunning, QueryResuming, QueryAborted, QueryPaused :: QueryStatus
pattern QueryCreating = QueryStatus { queryState = Creating }
pattern QueryRunning  = QueryStatus { queryState = Running }
pattern QueryResuming = QueryStatus { queryState = Resuming }
pattern QueryAborted  = QueryStatus { queryState = Aborted }
pattern QueryPaused   = QueryStatus { queryState = Paused }

data ViewInfo = ViewInfo {
    viewName  :: Text
  , viewQuery :: QueryInfo
} deriving (Generic, Show, FromJSON, ToJSON)

type SourceStreams  = [Text]
type SinkStream     = Text
type RelatedStreams = (SourceStreams, SinkStream)

  -- = StreamQuery RelatedStreams Text            -- ^ related streams and the stream it creates
  --  | ViewQuery   RelatedStreams Text            -- ^ related streams and the view it creates
  --  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data ShardReader = ShardReader
  { readerStreamName  :: Text
  , readerShardId     :: Word64
  , readerShardOffset :: S.LSN
  , readerReaderId    :: Text
  , readerReadTimeout :: Word32
  } deriving (Show, Generic, FromJSON, ToJSON)

data TaskAllocation = TaskAllocation { taskAllocationEpoch :: Word32, taskAllocationServerId :: ServerID}
  deriving (Show, Generic, FromJSON, ToJSON)

rootPath :: Text
rootPath = "/hstream"

instance HasPath ShardReader ZHandle where
  myRootPath = rootPath <> "/shardReader"
instance HasPath SubscriptionWrap ZHandle where
  myRootPath = rootPath <> "/subscriptions"
instance HasPath QueryInfo ZHandle where
  myRootPath = rootPath <> "/queries"
instance HasPath ViewInfo ZHandle where
  myRootPath = rootPath <> "/views"
instance HasPath QueryStatus ZHandle where
  myRootPath = rootPath <> "/queryStatus"
instance HasPath Proto.Timestamp ZHandle where
  myRootPath = rootPath <> "/timestamp"
instance HasPath TaskAllocation ZHandle where
  myRootPath = rootPath <> "/taskAllocations"

instance HasPath ShardReader RHandle where
  myRootPath = "readers"
instance HasPath SubscriptionWrap RHandle where
  myRootPath = "subscriptions"
instance HasPath QueryInfo RHandle where
  myRootPath = "queries"
instance HasPath ViewInfo RHandle where
  myRootPath = "views"
instance HasPath QueryStatus RHandle where
  myRootPath = "queryStatus"
instance HasPath Proto.Timestamp RHandle where
  myRootPath = "timestamp"
instance HasPath TaskAllocation RHandle where
  myRootPath = "taskAllocations"

instance HasPath ShardReader FHandle where
  myRootPath = "readers"
instance HasPath SubscriptionWrap FHandle where
  myRootPath = "subscriptions"
instance HasPath QueryInfo FHandle where
  myRootPath = "queries"
instance HasPath ViewInfo FHandle where
  myRootPath = "views"
instance HasPath QueryStatus FHandle where
  myRootPath = "queryStatus"
instance HasPath Proto.Timestamp FHandle where
  myRootPath = "timestamp"
instance HasPath TaskAllocation FHandle where
  myRootPath = "taskAllocations"

insertQuery :: (MetaType QueryInfo handle, MetaType QueryStatus handle, MetaMulti handle)
  => QueryInfo -> handle -> IO ()
insertQuery qInfo@QueryInfo{..} h = do
  metaMulti [ insertMetaOp queryId qInfo h
            , insertMetaOp queryId QueryCreating h
            ]
            h

deleteQueryInfo :: (MetaType QueryInfo handle, MetaType QueryStatus handle, MetaMulti handle)
  => Text -> handle -> IO ()
deleteQueryInfo qid h = do
  metaMulti [ deleteMetaOp @QueryInfo qid Nothing h
            , deleteMetaOp @QueryStatus qid Nothing h
            ]
            h

getSubscriptionWithStream :: MetaType SubscriptionWrap handle => handle -> Text -> IO [SubscriptionWrap]
getSubscriptionWithStream zk sName = do
  subs <- listMeta @SubscriptionWrap zk
  return $ filter ((== sName) . subscriptionStreamName . originSub) subs

--------------------------------------------------------------------------------

getQuerySink :: QueryInfo -> SinkStream
getQuerySink QueryInfo{..} = snd queryStreams

getQuerySources :: QueryInfo -> SourceStreams
getQuerySources QueryInfo{..} = fst queryStreams

createInsertQueryInfo :: Text -> Text -> RelatedStreams -> MetaHandle -> IO QueryInfo
createInsertQueryInfo queryId querySql queryStreams h = do
  MkSystemTime queryCreatedTime _ <- getSystemTime
  let qInfo = QueryInfo {..}
  insertQuery qInfo h
  return qInfo

#ifdef HStreamUseV2Engine
groupbyStores :: IORef (HM.HashMap Text (MVar (DataChangeBatch AST.FlowObject HCT.Timestamp)))
groupbyStores = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE groupbyStores #-}
#else
groupbyStores :: IORef (HM.HashMap Text (HS.Materialized K V V))
groupbyStores = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE groupbyStores #-}
#endif
--------------------------------------------------------------------------------
