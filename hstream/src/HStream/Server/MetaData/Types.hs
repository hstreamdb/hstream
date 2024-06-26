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
  , renderTaskMetaMapToTable
  , renderTaskIdMetaMapToTable

#ifdef HStreamEnableSchema
  , hstreamColumnCatalogToColumnCatalog
  , columnCatalogToHStreamColumnCatalog
  , hstreamSchemaToSchema
  , schemaToHStreamSchema

  , registerSchema
  , getSchema
  , isSchemaRegistered
  , unregisterSchema
#endif
  ) where

import           Control.Exception                 (catches)
import           Control.Monad                     (forM)
import           Data.Aeson                        (FromJSON (..), ToJSON (..))
import qualified Data.Aeson                        as Aeson
import qualified Data.HashMap.Strict               as HM
import           Data.Int                          (Int64)
import qualified Data.IntMap                       as IntMap
import           Data.IORef
import qualified Data.List                         as L
import           Data.Map                          (Map)
import qualified Data.Map.Strict                   as M
import           Data.Text                         (Text)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import           Data.Time.Clock.System            (SystemTime (MkSystemTime),
                                                    getSystemTime)
import qualified Data.Vector                       as V
import           Data.Word                         (Word32, Word64)
import           GHC.Generics                      (Generic)
import           GHC.IO                            (unsafePerformIO)
import           GHC.Stack

import           HStream.Common.Server.MetaData    (rootPath)
import           HStream.Common.ZookeeperClient    (ZookeeperClient)
import           HStream.IO.Types                  (TaskIdMeta (..),
                                                    TaskInfo (..),
                                                    TaskMeta (..))
import qualified HStream.Logger                    as Log
import           HStream.MetaStore.Types           (FHandle, HasPath (..),
                                                    MetaHandle,
                                                    MetaMulti (metaMulti),
                                                    MetaStore (..), MetaType,
                                                    RHandle)
import qualified HStream.Server.ConnectorTypes     as HCT
import           HStream.Server.HStreamApi         (Subscription (..))
import qualified HStream.Server.HStreamApi         as API
import           HStream.Server.MetaData.Exception
import           HStream.Server.Types              (SubscriptionWrap (..))
import qualified HStream.Store                     as S
import           HStream.ThirdParty.Protobuf       (toRFC3339)
import           HStream.Utils
#ifdef HStreamUseV2Engine
import           DiffFlow.Types
#else
import qualified HStream.Processing.Stream         as HS
import           HStream.SQL                       (K, V)
import qualified HStream.SQL                       as SQL
#endif
--------------------------------------------------------------------------------
#ifdef HStreamEnableSchema
type SQLIRType = SQL.BoundSQL
#else
type SQLIRType = SQL.RSQL
#endif

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
  , queryRefinedAST  :: SQLIRType -- FIXME: use a proper name
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

renderTaskMetaMapToTable :: Map Text TaskMeta -> Aeson.Value
renderTaskMetaMapToTable mp =
  let headers = [ "Connector Name" :: Text
                , "Task Id"        :: Text
                , "Task Type"      :: Text
                , "Created Time"   :: Text
                , "State"          :: Text
                ]
      rows = map getMetaInfo $ M.toList mp
   in Aeson.object ["headers" Aeson..= headers, "rows" Aeson..= rows]
 where
   getMetaInfo (taskId, TaskMeta{taskInfoMeta=TaskInfo{..}, ..}) =
     let createTime = TL.toStrict . toRFC3339 $ taskCreatedTime
      in [taskName, taskId, T.pack . show $ taskType, createTime, T.pack . show $ taskStateMeta]

renderTaskIdMetaMapToTable :: Map Text TaskIdMeta -> Aeson.Value
renderTaskIdMetaMapToTable mp =
  let headers = ["Connector Name" :: Text , "Task Id"]
      rows = map (\(name, TaskIdMeta{..}) -> [name, taskIdMeta]) $ M.toList mp
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

instance HasPath ShardReaderMeta ZookeeperClient where
  myRootPath = rootPath <> "/shardReader"
  myExceptionHandler = zkExceptionHandlers ResShardReader
instance HasPath SubscriptionWrap ZookeeperClient where
  myRootPath = rootPath <> "/subscriptions"
  myExceptionHandler = zkExceptionHandlers ResSubscription
instance HasPath QueryInfo ZookeeperClient where
  myRootPath = rootPath <> "/queries"
  myExceptionHandler = zkExceptionHandlers ResQuery
instance HasPath ViewInfo ZookeeperClient where
  myRootPath = rootPath <> "/views"
  myExceptionHandler = zkExceptionHandlers ResView
instance HasPath QueryStatus ZookeeperClient where
  myRootPath = rootPath <> "/queryStatus"
  myExceptionHandler = zkExceptionHandlers ResQuery
instance HasPath QVRelation ZookeeperClient where
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

createInsertQueryInfo :: Text -> Text -> RelatedStreams -> SQLIRType -> Word32 -> MetaHandle -> IO QueryInfo
createInsertQueryInfo queryId querySql queryStreams queryRefinedAST workerNodeId h = do
  MkSystemTime queryCreatedTime _ <- getSystemTime
  let queryType = QueryCreateStream
      qInfo = QueryInfo {..}
  insertQuery qInfo h
  return qInfo

createInsertViewQueryInfo :: Text -> Text -> SQLIRType -> RelatedStreams -> Text -> Word32 -> MetaHandle -> IO ViewInfo
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

#ifdef HStreamEnableSchema
instance HasPath SQL.Schema ZookeeperClient where
  myRootPath = rootPath <> "/schemas"
instance HasPath SQL.Schema FHandle where
  myRootPath = "schemas"
instance HasPath SQL.Schema RHandle where
  myRootPath = "schemas"

hstreamColumnCatalogToColumnCatalog :: SQL.ColumnCatalog -> API.ColumnCatalog
hstreamColumnCatalogToColumnCatalog SQL.ColumnCatalog{..} =
  API.ColumnCatalog
  { API.columnCatalogIndex = fromIntegral columnId
  , API.columnCatalogName = columnName
  , API.columnCatalogType = T.pack (show columnType)
  , API.columnCatalogIsNullable = columnIsNullable
  , API.columnCatalogIsHidden = columnIsHidden
  }

columnCatalogToHStreamColumnCatalog :: Text -> API.ColumnCatalog -> SQL.ColumnCatalog
columnCatalogToHStreamColumnCatalog streamName API.ColumnCatalog{..} =
  SQL.ColumnCatalog
  { SQL.columnId = fromIntegral columnCatalogIndex
  , SQL.columnName = columnCatalogName
  , SQL.columnType = read (T.unpack columnCatalogType)
  , SQL.columnIsNullable = columnCatalogIsNullable
  , SQL.columnIsHidden = columnCatalogIsHidden
  , SQL.columnStream = streamName
  , SQL.columnStreamId = 0
  }

hstreamSchemaToSchema :: SQL.Schema -> API.Schema
hstreamSchemaToSchema SQL.Schema{..} =
  API.Schema
  { API.schemaOwner = schemaOwner
  , API.schemaColumns = V.map hstreamColumnCatalogToColumnCatalog (V.fromList $ IntMap.elems schemaColumns)
  }

schemaToHStreamSchema :: API.Schema -> SQL.Schema
schemaToHStreamSchema API.Schema{..} =
  SQL.Schema
  { SQL.schemaOwner = schemaOwner
  , SQL.schemaColumns = IntMap.fromList $
    L.map (\col -> (fromIntegral (API.columnCatalogIndex col), columnCatalogToHStreamColumnCatalog schemaOwner col)) (V.toList schemaColumns)
  }

registerSchema :: ( MetaType SQL.Schema handle
                  , HasCallStack
                  )
               => handle -> SQL.Schema -> IO ()
registerSchema h schema = do
  isSchemaRegistered h (SQL.schemaOwner schema) >>= \case
    True  -> Log.warning $ "Schema " <> Log.build (SQL.schemaOwner schema) <> " already exists, skip."
    False -> insertMeta (SQL.schemaOwner schema) schema h

getSchema :: ( MetaType SQL.Schema handle
             , HasCallStack
             ) => handle -> Text -> IO (Maybe SQL.Schema)
getSchema h schemaName = do
  getMeta schemaName h

isSchemaRegistered :: ( MetaType SQL.Schema handle
                      , HasCallStack
                      ) => handle -> Text -> IO Bool
isSchemaRegistered h schemaName = do
  checkMetaExists @SQL.Schema schemaName h

unregisterSchema :: ( MetaType SQL.Schema handle
                    , HasCallStack
                    ) => handle -> Text -> IO ()
unregisterSchema h schemaName = do
  isSchemaRegistered h schemaName >>= \case
    True  -> deleteMeta @SQL.Schema schemaName Nothing h
    False -> Log.warning $ "Schema " <> Log.build schemaName <> " does not exist, skip."
#endif
