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
  , QVRelation (..)
  , QueryStatus (QueryRunning, QueryCreating, QueryTerminated
               , QueryAborted, QueryResuming, QueryPaused, ..)
  , ShardReaderMeta (..)
  , TaskAllocation (..)
  , createInsertQueryInfo
  , createInsertViewQueryInfo
  , deleteQueryInfo
  , deleteViewQuery
  , getSubscriptionWithStream
  , groupbyStores
  , rootPath
  , getQuerySink
  , getQuerySources
  , renderQueryInfosToTable
  , renderQueryStatusToTable
  , renderViewInfosToTable
  , renderQVRelationToTable
  , renderTaskAllocationsToTable
  ) where

import           Control.Exception                 (catches)
import           Data.Aeson                        (FromJSON (..), ToJSON (..))
import qualified Data.HashMap.Strict               as HM
import           Data.Int                          (Int64)
import           Data.IORef
import           Data.Text                         (Text)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import qualified Data.Text.Lazy.Encoding           as TL
import           Data.Time.Clock.System            (SystemTime (MkSystemTime),
                                                    getSystemTime)
import           Data.Word                         (Word32, Word64)
import           GHC.Generics                      (Generic)
import           GHC.IO                            (unsafePerformIO)
import           ZooKeeper.Types                   (ZHandle)

import           Control.Monad                     (forM)
import qualified Data.Aeson                        as Aeson
import           HStream.MetaStore.Types           (FHandle, HasPath (..),
                                                    MetaHandle,
                                                    MetaMulti (metaMulti),
                                                    MetaStore (..), MetaType,
                                                    RHandle)
import qualified HStream.Server.ConnectorTypes     as HCT
import           HStream.Server.HStreamApi         (ServerNode (..),
                                                    Subscription (..))
import           HStream.Server.MetaData.Exception
import           HStream.Server.Types              (ServerID,
                                                    SubscriptionWrap (..))
import qualified HStream.SQL.AST                   as AST
import qualified HStream.Store                     as S
import qualified HStream.ThirdParty.Protobuf       as Proto
import           HStream.Utils
#ifdef HStreamUseV2Engine
import           DiffFlow.Types
#else
import qualified HStream.Processing.Stream         as HS
import           HStream.SQL.Codegen.V1
#endif
--------------------------------------------------------------------------------

newtype QueryStatus = QueryStatus { queryState :: TaskStatus }
  deriving (Generic, Show, FromJSON, ToJSON)

pattern QueryCreating, QueryRunning, QueryResuming, QueryAborted, QueryPaused, QueryTerminated :: QueryStatus
pattern QueryCreating   = QueryStatus { queryState = Creating }
pattern QueryRunning    = QueryStatus { queryState = Running }
pattern QueryResuming   = QueryStatus { queryState = Resuming }
pattern QueryAborted    = QueryStatus { queryState = Aborted }
pattern QueryPaused     = QueryStatus { queryState = Paused }
pattern QueryTerminated = QueryStatus { queryState = Terminated }

renderQueryStatusToTable :: [QueryStatus] -> Aeson.Value
renderQueryStatusToTable infos =
  let headers = ["Query Status" :: Text]
      rows = forM infos $ \QueryStatus{..} ->
        [ queryState ]
   in Aeson.object ["headers" Aeson..= headers, "rows" Aeson..= rows]

data QueryInfo = QueryInfo
  { queryId          :: Text
  , querySql         :: Text
  , queryCreatedTime :: Int64
  , queryStreams     :: RelatedStreams
  , queryRefinedAST  :: AST.RSQL
  , workerNodeId     :: Word32
  , queryType        :: QType
  } deriving (Generic, Show, FromJSON, ToJSON)

showQueryInfo :: QueryInfo -> Text
showQueryInfo QueryInfo{..} = "queryId: " <> queryId
                           <> ", sql: " <> querySql
                           <> ", createdTime: " <> (T.pack . show $ queryCreatedTime)
                           <> ", sourceStream: " <> T.intercalate "," (fst queryStreams)
                           <> ", sinkStream: " <> snd queryStreams
                           <> ", ast: " <> (T.pack . show $ queryRefinedAST)
                           <> ", workNode: " <> (T.pack . show $ workerNodeId)
                           <> ", type: " <> (T.pack . formatQueryType . getQueryType $ queryType)

renderQueryInfosToTable :: [QueryInfo] -> Text
renderQueryInfosToTable infos = T.intercalate "\n" $ map (\info -> "{ " <> showQueryInfo info <> " }") infos

data ViewInfo = ViewInfo {
    viewName  :: Text
  , viewQuery :: QueryInfo
} deriving (Generic, Show, FromJSON, ToJSON)

showViewInfo :: ViewInfo -> Text
showViewInfo ViewInfo{..} = "viewName: " <> viewName <> ", " <> showQueryInfo viewQuery

renderViewInfosToTable :: [ViewInfo] -> Text
renderViewInfosToTable infos = T.intercalate "\n" $ map (\info -> "{ " <> showViewInfo info <> " }") infos

data QVRelation = QVRelation {
    qvRelationQueryName :: Text
  , qvRelationViewName  :: Text
} deriving (Generic, Show, FromJSON, ToJSON)

renderQVRelationToTable :: [QVRelation] -> Aeson.Value
renderQVRelationToTable relations =
  let headers = ["Query ID" :: Text, "View Name"]
      rows = map (\QVRelation{..} -> [qvRelationQueryName, qvRelationViewName]) relations
   in Aeson.object ["headers" Aeson..= headers, "rows" Aeson..= rows]

type SourceStreams  = [Text]
type SinkStream     = Text
type RelatedStreams = (SourceStreams, SinkStream)

  -- = StreamQuery RelatedStreams Text            -- ^ related streams and the stream it creates
  --  | ViewQuery   RelatedStreams Text            -- ^ related streams and the view it creates
  --  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data ShardReaderMeta = ShardReaderMeta
  { readerStreamName  :: Text
  , readerShardId     :: Word64
  , readerShardOffset :: S.LSN
  , readerReaderId    :: Text
  , readerReadTimeout :: Word32
  , startTimestamp    :: Maybe Int64
    -- ^ use to record start time offset
  } deriving (Show, Generic, FromJSON, ToJSON)

data TaskAllocation = TaskAllocation { taskAllocationEpoch :: Word32, taskAllocationServerId :: ServerID}
  deriving (Show, Generic, FromJSON, ToJSON)

renderTaskAllocationsToTable :: [TaskAllocation] -> Aeson.Value
renderTaskAllocationsToTable relations =
  let headers = ["Server ID" :: Text]
      rows = map (\TaskAllocation{..} -> [taskAllocationServerId]) relations
   in Aeson.object ["headers" Aeson..= headers, "rows" Aeson..= rows]

rootPath :: Text
rootPath = "/hstream"

instance HasPath ShardReaderMeta ZHandle where
  myRootPath = rootPath <> "/shardReader"
  myExceptionHandler = zkExceptionHandlers ResShardReader
instance HasPath SubscriptionWrap ZHandle where
  myRootPath = rootPath <> "/subscriptions"
  myExceptionHandler = zkExceptionHandlers ResSubscription
instance HasPath QueryInfo ZHandle where
  myRootPath = rootPath <> "/queries"
  myExceptionHandler = zkExceptionHandlers ResQuery
instance HasPath ViewInfo ZHandle where
  myRootPath = rootPath <> "/views"
  myExceptionHandler = zkExceptionHandlers ResView
instance HasPath QueryStatus ZHandle where
  myRootPath = rootPath <> "/queryStatus"
  myExceptionHandler = zkExceptionHandlers ResQuery
instance HasPath Proto.Timestamp ZHandle where
  myRootPath = rootPath <> "/timestamp"
instance HasPath TaskAllocation ZHandle where
  myRootPath = rootPath <> "/taskAllocations"
instance HasPath QVRelation ZHandle where
  myRootPath = rootPath <> "/qvRelation"

instance HasPath ShardReaderMeta RHandle where
  myRootPath = "readers"
  myExceptionHandler = rqExceptionHandlers ResShardReader
instance HasPath SubscriptionWrap RHandle where
  myRootPath = "subscriptions"
  myExceptionHandler = rqExceptionHandlers ResSubscription
instance HasPath QueryInfo RHandle where
  myRootPath = "queries"
  myExceptionHandler = rqExceptionHandlers ResQuery
instance HasPath ViewInfo RHandle where
  myRootPath = "views"
  myExceptionHandler = rqExceptionHandlers ResView
instance HasPath QueryStatus RHandle where
  myRootPath = "queryStatus"
  myExceptionHandler = rqExceptionHandlers ResQuery
instance HasPath Proto.Timestamp RHandle where
  myRootPath = "timestamp"
instance HasPath TaskAllocation RHandle where
  myRootPath = "taskAllocations"
instance HasPath QVRelation RHandle where
  myRootPath = "qvRelation"

instance HasPath ShardReaderMeta FHandle where
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
instance HasPath QVRelation FHandle where
  myRootPath = "qvRelation"

insertQuery :: (MetaType QueryInfo handle, MetaType QueryStatus handle, MetaMulti handle)
  => QueryInfo -> handle -> IO ()
insertQuery qInfo@QueryInfo{..} h = do
  metaMulti [ insertMetaOp queryId qInfo h
            , insertMetaOp queryId QueryCreating h
            ]
            h
    `catches` (rqExceptionHandlers ResQuery queryId ++ zkExceptionHandlers ResQuery queryId)

insertViewQuery
  :: ( MetaType QueryInfo handle
     , MetaType QueryStatus handle
     , MetaType ViewInfo handle
     , MetaType QVRelation handle
     , MetaMulti handle)
  => ViewInfo -> handle -> IO ()
insertViewQuery vInfo@ViewInfo{..} h = do
  let qid = queryId viewQuery
  metaMulti [ insertMetaOp qid viewQuery h
            , insertMetaOp qid QueryCreating h
            , insertMetaOp viewName vInfo h
            , insertMetaOp qid QVRelation{ qvRelationQueryName = qid
                                         , qvRelationViewName  = viewName} h
            ]
            h
    `catches` (rqExceptionHandlers ResView viewName ++ zkExceptionHandlers ResView viewName)

deleteQueryInfo :: (MetaType QueryInfo handle, MetaType QueryStatus handle, MetaMulti handle)
  => Text -> handle -> IO ()
deleteQueryInfo qid h = do
  metaMulti [ deleteMetaOp @QueryInfo qid Nothing h
            , deleteMetaOp @QueryStatus qid Nothing h
            ]
            h
    `catches` (rqExceptionHandlers ResView qid ++ zkExceptionHandlers ResView qid)

deleteViewQuery
  :: ( MetaType QueryInfo handle
     , MetaType QueryStatus handle
     , MetaType ViewInfo handle
     , MetaType QVRelation handle
     , MetaMulti handle)
  => Text -> Text -> handle -> IO ()
deleteViewQuery vName qName h = do
  metaMulti [ deleteMetaOp @QueryInfo   qName Nothing h
            , deleteMetaOp @QueryStatus qName Nothing h
            , deleteMetaOp @ViewInfo    vName Nothing h
            , deleteMetaOp @QVRelation  qName Nothing h
            ]
            h
    `catches` (rqExceptionHandlers ResView vName ++ zkExceptionHandlers ResView vName)

getSubscriptionWithStream :: MetaType SubscriptionWrap handle => handle -> Text -> IO [SubscriptionWrap]
getSubscriptionWithStream zk sName = do
  subs <- listMeta @SubscriptionWrap zk
  return $ filter ((== sName) . subscriptionStreamName . originSub) subs

--------------------------------------------------------------------------------

getQuerySink :: QueryInfo -> SinkStream
getQuerySink QueryInfo{..} = snd queryStreams

getQuerySources :: QueryInfo -> SourceStreams
getQuerySources QueryInfo{..} = fst queryStreams

createInsertQueryInfo :: Text -> Text -> RelatedStreams -> AST.RSQL -> Word32 -> MetaHandle -> IO QueryInfo
createInsertQueryInfo queryId querySql queryStreams queryRefinedAST workerNodeId h = do
  MkSystemTime queryCreatedTime _ <- getSystemTime
  let queryType = QueryCreateStream
      qInfo = QueryInfo {..}
  insertQuery qInfo h
  return qInfo

createInsertViewQueryInfo :: Text -> Text -> AST.RSQL -> RelatedStreams -> Text -> Word32 -> MetaHandle -> IO ViewInfo
createInsertViewQueryInfo queryId querySql queryRefinedAST queryStreams viewName workerNodeId h = do
  MkSystemTime queryCreatedTime _ <- getSystemTime
  let queryType = QueryCreateView
      vInfo = ViewInfo{ viewName = viewName, viewQuery = QueryInfo{..} }
  insertViewQuery vInfo h
  return vInfo

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
