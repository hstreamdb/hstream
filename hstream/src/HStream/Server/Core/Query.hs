{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Core.Query
  ( executeQuery
  , executePushQuery
  , terminateQueries

  , createQuery
  , listQueries
  , getQuery
  , deleteQuery

  , hstreamQueryToQuery
  ) where

import           Control.Concurrent
import           Control.Concurrent.Async         (async, cancel, wait)
import           Control.Exception                (throw, throwIO)
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (atomicModifyIORef',
                                                   readIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel           (StreamSend)
import           Network.GRPC.HighLevel.Generated (GRPCIOError)
import           Network.GRPC.HighLevel.Server    (ServerCallMetadata)
import qualified Proto3.Suite.JSONPB              as PB
import qualified Z.Data.CBytes                    as CB

import           DiffFlow.Types                   (DataChange (..),
                                                   DataChangeBatch (..))
import qualified HStream.Exception                as HE
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.ConnectorTypes    hiding (StreamName, Timestamp)
import           HStream.Server.Core.Common
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.View         as Core
import           HStream.Server.Handler.Common
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData
import qualified HStream.Server.MetaData          as P
import qualified HStream.Server.Shard             as Shard
import           HStream.Server.Types
import           HStream.SQL.AST
import           HStream.SQL.Codegen              hiding (StreamName)
import qualified HStream.SQL.Codegen              as HSC
import qualified HStream.SQL.Internal.Codegen     as HSC
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-------------------------------------------------------------------------------

executeQuery :: ServerContext -> CommandQuery -> IO CommandQueryResponse
executeQuery sc@ServerContext{..} CommandQuery{..} = do
  Log.debug $ "Receive Query Request: " <> Log.buildText commandQueryStmtText
  plan <- streamCodegen commandQueryStmtText
  case plan of
    SelectPlan {} ->
      let x = "Inconsistent method called: select from streams SQL statements should be sent to rpc `ExecutePushQuery`"
       in throwIO $ HE.InvalidSqlStatement x
    CreateBySelectPlan _ inNodesWithStreams outNodeWithStream _ _ _ -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
          query   = P.StreamQuery sources sink
      create (transToStreamName sink)
      (qid,_) <- handleCreateAsSelect sc plan commandQueryStmtText query
      pure $ API.CommandQueryResponse (mkVectorStruct qid "stream_query_id")
    CreateViewPlan _ schema inNodesWithStreams outNodeWithStream _ _ accumulation -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
          query   = P.ViewQuery sources sink schema
      -- make sure source streams exist
      nonExistedSource <- filterM (fmap not . S.doesStreamExist scLDClient . transToStreamName) sources :: IO [T.Text]
      case nonExistedSource of
        [] -> do
          create (transToStreamName sink)
          (qid,_) <- handleCreateAsSelect sc plan commandQueryStmtText query
          atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink accumulation hm, ()))
          pure $ API.CommandQueryResponse (mkVectorStruct qid "view_query_id")
        _ : _ -> do
          let x = "Source " <> show (T.concat $ L.intersperse ", " nonExistedSource) <> " doesn't exist"
          throwIO $ HE.InvalidSqlStatement x
    CreatePlan stream fac -> do
      let s = API.Stream
            { streamStreamName = stream
            , streamReplicationFactor = fromIntegral fac
            , streamBacklogDuration = 0
            , streamShardCount = 1
            }
      Core.createStream sc s
      pure $ API.CommandQueryResponse (mkVectorStruct s "created_stream")
    CreateConnectorPlan {} -> do
      void $ IO.createIOTaskFromSql scIOWorker commandQueryStmtText
      pure $ CommandQueryResponse V.empty
    InsertPlan _ _ _ -> discard "Append"
    DropPlan checkIfExist dropObject ->
      case dropObject of
        DStream stream -> do
          let request = DeleteStreamRequest
                      { deleteStreamRequestStreamName = stream
                      , deleteStreamRequestIgnoreNonExist = not checkIfExist
                      , deleteStreamRequestForce = False
                      }
          Core.deleteStream sc request
          pure $ API.CommandQueryResponse V.empty
        DView view -> do
          void $ Core.deleteView sc view checkIfExist
          pure $ API.CommandQueryResponse V.empty
        DConnector conn -> do
          IO.deleteIOTask scIOWorker conn
          pure $ API.CommandQueryResponse V.empty
    ShowPlan showObject ->
      case showObject of
        SStreams -> do
          streams <- Core.listStreams sc ListStreamsRequest
          pure $ API.CommandQueryResponse (mkVectorStruct streams "streams")
        SQueries -> do
          queries <- listQueries sc
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList queries) "queries")
        SConnectors -> do
          connectors <- IO.listIOTasks scIOWorker
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList connectors) "connectors")
        SViews -> do
          views <- Core.listViews sc
          pure $ API.CommandQueryResponse (mkVectorStruct (V.fromList views) "views")
    TerminatePlan sel -> do
      let request = case sel of
            AllQueries       -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.empty
                                , terminateQueriesRequestAll = True
                                }
            OneQuery qid     -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.singleton qid
                                , terminateQueriesRequestAll = False
                                }
            ManyQueries qids -> TerminateQueriesRequest
                                { terminateQueriesRequestQueryId = V.fromList qids
                                , terminateQueriesRequestAll = False
                                }
      TerminateQueriesResponse{..} <- terminateQueries sc request
      let value  = PB.toAesonValue terminateQueriesResponseQueryId
          object = HM.fromList [("terminated", value)]
          result = V.singleton (jsonObjectToStruct object)
      pure $ API.CommandQueryResponse result
    SelectViewPlan RSelectView {..} -> do
      hm <- readIORef P.groupbyStores
      case HM.lookup rSelectViewFrom hm of
        Nothing -> throwIO $ HE.ViewNotFound "VIEW not found"
        Just accumulation -> do
          DataChangeBatch{..} <- readMVar accumulation
          case dcbChanges of
            [] -> sendResp mempty
            _  -> do
              sendResp $ V.map (dcRow . mapView rSelectViewSelect)
                (V.fromList $ filterView rSelectViewWhere dcbChanges)
    ExplainPlan _ -> throwIO $ HE.ExecPlanUnimplemented "ExplainPlan Unimplemented"
    PausePlan (PauseObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False False
      pure (CommandQueryResponse V.empty)
    ResumePlan (ResumeObjectConnector name) -> do
      IO.startIOTask scIOWorker name
      pure (CommandQueryResponse V.empty)
  where
    create sName = do
      Log.debug . Log.buildString $ "CREATE: new stream " <> show sName
      createStreamWithShard scLDClient sName "query" scDefaultStreamRepFactor
    discard rpcName = throwIO $ HE.DiscardedMethod $
      "Discarded method called: should call rpc `" <> rpcName <> "` instead"
    sendResp results = pure $ API.CommandQueryResponse $
      V.map (structToStruct "SELECTVIEW" . jsonObjectToStruct) results
    mkVectorStruct a label =
      let object = HM.fromList [(label, PB.toAesonValue a)]
       in V.singleton (jsonObjectToStruct object)

executePushQuery
  :: ServerContext
  -> API.CommandPushQuery
  -> ServerCallMetadata
  -> StreamSend Struct
  -> IO ()
executePushQuery ctx@ServerContext{..} API.CommandPushQuery{..} meta streamSend = do
    Log.debug $ "Receive Push Query Request: " <> Log.buildText commandPushQueryQueryText
    plan <- streamCodegen commandPushQueryQueryText
    case plan of
      SelectPlan _ inNodesWithStreams outNodeWithStream _ _ -> do
        let sources = snd <$> inNodesWithStreams
            sink    = snd outNodeWithStream
        exists <- mapM (S.doesStreamExist scLDClient . transToStreamName) sources
        case and exists of
          False -> do
            Log.warning $ "At least one of the streams do not exist: "
              <> Log.buildString (show sources)
            throwIO $ HE.StreamNotFound $ "At least one of the streams do not exist: " <> T.pack (show sources)
          True  -> do
            createStreamWithShard scLDClient (transToStreamName sink) "query" scDefaultStreamRepFactor
            let query = P.StreamQuery sources sink
            -- run task
            (qid,_) <- handleCreateAsSelect ctx plan commandPushQueryQueryText query
            tid <- readMVar runningQueries >>= \hm -> return $ (HM.!) hm qid

            -- sub from sink stream and push to client
            consumerName <- newRandomText 20
            let sc = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
            subscribeToStreamWithoutCkp sc sink API.SpecialOffsetLATEST

            sending <- async (sendToClient metaHandle qid sink sc streamSend)

            void . forkIO $ handlePushQueryCanceled meta $ do
              killThread tid
              cancel sending
              P.setQueryStatus qid Terminated metaHandle
              unSubscribeToStreamWithoutCkp sc sink

            wait sending
      _ -> do
        Log.warning "Push Query: Inconsistent Method Called"
        throwIO $ HE.InvalidSqlStatement "inconsistent method called"

sendToClient
  :: MetaHandle
  -> T.Text
  -> T.Text
  -> SourceConnectorWithoutCkp
  -> (Struct -> IO (Either GRPCIOError ()))
  -> IO ()
sendToClient metaHandle qid streamName SourceConnectorWithoutCkp{..} streamSend = do
  P.getQueryStatus qid metaHandle >>= \case
    Terminated -> throwIO $ HE.PushQueryTerminated ""
    Created -> throwIO $ HE.PushQueryCreated ""
    Running -> do
      withReadRecordsWithoutCkp streamName $ \sourceRecords -> do
        let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
            structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
        void $ streamSendMany structs
    _ -> throwIO $ HE.UnknownPushQueryStatus ""
  where
    streamSendMany = \case
      []        -> pure ()
      (x : xs') ->
        streamSend (structToStruct "SELECT" x) >>= \case
          Left err -> do
            Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
            throwIO $ HE.PushQuerySendError (show err)
          Right _ -> streamSendMany xs'

createQuery ::
  ServerContext -> CreateQueryRequest -> IO Query
createQuery
  sc@ServerContext {..} CreateQueryRequest {..} = do
    plan <- streamCodegen createQueryRequestSql
    let (inNodesWithStreams, outNodeWithStream) = case plan of
          CreateBySelectPlan _ ins out _ _ _ -> (ins, out)
          SelectPlan         _ ins out _ _   -> (ins, out)
          _ -> throw $ HE.WrongExecutionPlan "Create query only support select / create stream as select statements"
    let sources = snd <$> inNodesWithStreams
    exists <- mapM (S.doesStreamExist scLDClient . transToStreamName) sources
    unless (and exists) $ do
      Log.warning $ "At least one of the streams do not exist: "
                 <> Log.buildString (show sources)
      throwIO $ HE.StreamNotFound $ "At least one of the streams do not exist: "
                                <> T.pack (show sources)
    let sink    = snd outNodeWithStream
        query   = P.StreamQuery sources sink
    createStreamWithShard scLDClient (transToStreamName sink) "query" scDefaultStreamRepFactor
    -- run task
    (qid, _) <- handleCreateAsSelect sc plan createQueryRequestSql query
    getMeta @P.PersistentQuery qid metaHandle >>= \case
      Just pQuery -> return $ hstreamQueryToQuery pQuery
      Nothing     -> throwIO $ HE.UnexpectedError "Failed to create query for some unknown reason"

listQueries :: ServerContext -> IO [Query]
listQueries ServerContext{..} = do
  queries <- M.listMeta metaHandle
  return $ map hstreamQueryToQuery queries

getQuery :: ServerContext -> GetQueryRequest -> IO Query
getQuery ctx req = do
  m_query <- getQuery' ctx req
  maybe (throwIO $ HE.QueryNotFound "Query does not exist") pure m_query

getQuery' :: ServerContext -> GetQueryRequest -> IO (Maybe Query)
getQuery' ServerContext{..} GetQueryRequest{..} = do
  queries <- M.listMeta metaHandle
  return $ hstreamQueryToQuery <$>
    L.find (\P.PersistentQuery{..} -> queryId == getQueryRequestId) queries

terminateQueries
  :: ServerContext -> TerminateQueriesRequest -> IO TerminateQueriesResponse
terminateQueries ctx req = terminateQueries' ctx req >>= \case
  Left terminatedQids ->
    let x = "Only the following queries are terminated " <> show terminatedQids
     in throwIO $ HE.TerminateQueriesError x
  Right r -> pure r

terminateQueries'
  :: ServerContext
  -> TerminateQueriesRequest
  -> IO (Either [T.Text] TerminateQueriesResponse)
terminateQueries' ctx@ServerContext{..} TerminateQueriesRequest{..} = do
  qids <- if terminateQueriesRequestAll
          then HM.keys <$> readMVar runningQueries
          else return . V.toList $ terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate ctx (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then return (Left terminatedQids)
    else return (Right $ TerminateQueriesResponse (V.fromList terminatedQids))

deleteQuery :: ServerContext -> DeleteQueryRequest -> IO ()
deleteQuery ServerContext{..} DeleteQueryRequest{..} =
  M.deleteMeta @P.PersistentQuery deleteQueryRequestId Nothing metaHandle

-------------------------------------------------------------------------------

hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery (P.PersistentQuery queryId sqlStatement createdTime _ status _ _) =
  Query
  { queryId          = queryId
  , queryStatus      = getPBStatus status
  , queryCreatedTime = createdTime
  , queryQueryText   = sqlStatement
  }

-------------------------------------------------------------------------------

mapView :: SelectViewSelect -> DataChange a -> DataChange a
mapView SVSelectAll change = change
mapView (SVSelectFields fieldsWithAlias) change@DataChange{..} =
  let newRow = HM.fromList $
               L.map (\(field,alias) ->
                         (T.pack alias, HSC.getFieldByName dcRow field)
                     ) fieldsWithAlias
  in change {dcRow = newRow}

filterView :: RWhere -> [DataChange a] -> [DataChange a]
filterView rwhere =
  L.filter (\DataChange{..} -> HSC.genFilterR rwhere dcRow)

createStreamWithShard :: S.LDClient -> S.StreamId -> CB.CBytes -> Int -> IO ()
createStreamWithShard client streamId shardName factor = do
  S.createStream client streamId (S.def{ S.logReplicationFactor = S.defAttr1 factor })
  let extrAttr = Map.fromList [(Shard.shardStartKey, Shard.keyToCBytes minBound), (Shard.shardEndKey, Shard.keyToCBytes maxBound), (Shard.shardEpoch, "1")]
  void $ S.createStreamPartition client streamId (Just shardName) extrAttr
