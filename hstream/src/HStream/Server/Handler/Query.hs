{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Query where

import           Control.Concurrent
import           Control.Concurrent.Async         (async, cancel, wait)
import           Control.Exception                (Exception, Handler (..),
                                                   handle)
import           Control.Monad                    (join)
import qualified Data.Aeson                       as Aeson
import           Data.Bifunctor
import qualified Data.ByteString.Char8            as BS
import           Data.Function                    (on, (&))
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.IORef                       (atomicModifyIORef',
                                                   readIORef)
import           Data.List                        (find, (\\))
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (catMaybes, fromJust,
                                                   fromMaybe, isJust)
import           Data.Scientific
import           Data.String                      (IsString (fromString))
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as TE
import qualified Data.Time                        as Time
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (HasDefault (def))
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Exception
import           ZooKeeper.Types                  (ZHandle)

import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Processing.Connector     (SourceConnectorWithoutCkp (..))
import           HStream.Processing.Encoding      (Deserializer (..),
                                                   Serde (..), Serializer (..))
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Store
import qualified HStream.Processing.Stream        as PS
import           HStream.Processing.Type          hiding (StreamName, Timestamp)
import           HStream.Server.Config
import qualified HStream.Server.Core.Common       as Core
import           HStream.Server.Exception
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import qualified HStream.Server.Shard             as Shard
import           HStream.Server.Types
import           HStream.SQL                      (parseAndRefine)
import           HStream.SQL.AST
import           HStream.SQL.Codegen              hiding (StreamName)
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
-- import           HStream.SQL.ExecPlan             (genExecutionPlan)
import qualified HStream.Store                    as HS
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-------------------------------------------------------------------------------
-- Stream with Select Query

createQueryStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal CreateQueryStreamRequest CreateQueryStreamResponse ->
  IO (ServerResponse 'Normal CreateQueryStreamResponse)
createQueryStreamHandler
  sc@ServerContext {..}
  (ServerNormalRequest _metadata CreateQueryStreamRequest {..}) = queryExceptionHandle $ do
    plan@(SelectPlan tName inNodesWithStreams outNodeWithStream builder)
      <- streamCodegen createQueryStreamRequestQueryStatements
    let source = snd <$> inNodesWithStreams
        sink   = snd outNodeWithStream
    let sName = streamStreamName <$> createQueryStreamRequestQueryStream
        rFac  = maybe 1 (fromIntegral . streamReplicationFactor) createQueryStreamRequestQueryStream
        logDuration = streamBacklogDuration <$> createQueryStreamRequestQueryStream
        shardCount = streamShardCount <$> createQueryStreamRequestQueryStream
    createStreamWithShard scLDClient (transToStreamName sink) "query" rFac
    let query = P.StreamQuery (textToCBytes <$> source) (textToCBytes sink)
    void $
      handleCreateAsSelect
        sc
        plan
        createQueryStreamRequestQueryStatements
        query
        S.StreamTypeStream
    let streamResp = Stream sink (fromIntegral rFac) (fromMaybe 0 logDuration) (fromMaybe 1 shardCount)
        -- FIXME: The value query returned should have been fully assigned
        queryResp = def { queryId = tName }
    returnResp $ CreateQueryStreamResponse (Just streamResp) (Just queryResp)

--------------------------------------------------------------------------------

executeQueryHandler ::
  ServerContext ->
  ServerRequest 'Normal CommandQuery CommandQueryResponse ->
  IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext {..} (ServerNormalRequest _metadata CommandQuery {..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Query Request: " <> Log.buildText commandQueryStmtText
  plan' <- streamCodegen commandQueryStmtText
  case plan' of
    SelectPlan {} -> returnErrResp StatusInvalidArgument "inconsistent method called"
    -- execute plans that can be executed with this method
    CreateViewPlan tName schema inNodesWithStreams outNodeWithStream builder ->
      do
        let sources = snd <$> inNodesWithStreams
            sink    = snd outNodeWithStream
        create (transToStreamName sink)
        handleCreateAsSelect
          sc
          plan'
          commandQueryStmtText
          (P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema)
          S.StreamTypeView
        -- >> atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
        >> returnCommandQueryEmptyResp
    CreateConnectorPlan _cType _cName _ifNotExist _cConfig -> do
      IO.createIOTaskFromSql scIOWorker commandQueryStmtText >> returnCommandQueryEmptyResp
    SelectViewPlan RSelectView {..} -> do
      queries   <- P.getQueries zkHandle
      condNameM <- getGrpByFieldName queries rSelectViewFrom
      let neqCond = not case condNameM of
            Nothing       -> True
            Just condName -> fst rSelectViewWhere == condName
      if neqCond
        then returnErrResp StatusAborted $ StatusDetails . TE.encodeUtf8 . T.pack $
          "VIEW is grouped by " ++ show (fromJust condNameM) ++ ", not " ++ show (fst rSelectViewWhere)
        else do
          hm <- readIORef P.groupbyStores
          case HM.lookup rSelectViewFrom hm of
            Nothing -> returnErrResp StatusNotFound "VIEW not found"
            Just materialized -> do
              let (keyName, keyExpr) = rSelectViewWhere
                  (_, keyValue) = genRExprValue keyExpr (HM.fromList [])
              let keySerde = PS.mKeySerde materialized
                  valueSerde = PS.mValueSerde materialized
              let key = runSer (serializer keySerde) (HM.fromList [(keyName, keyValue)])
              case PS.mStateStore materialized of
                KVStateStore store -> do
                  sizeM <-
                    getFixedWinSize queries rSelectViewFrom
                      <&> fmap diffTimeToScientific
                  if isJust sizeM
                    then do
                      let size = fromJust sizeM & fromJust . toBoundedInteger @Int64
                      subset <-
                        ksDump store
                          <&> Map.filterWithKey
                            (\k _ -> all (`elem` HM.toList k) (HM.toList key))
                          <&> Map.toList
                      let winStarts =
                            subset
                              <&> (lookup "winStart" . HM.toList) . fst
                                & L.sort . L.nub . catMaybes
                          singlWinStart =
                            subset
                              <&> first (filter (\(k, _) -> k == "winStart") . HM.toList)
                              <&> first HM.fromList
                          grped =
                            winStarts <&> \winStart ->
                              let Aeson.Number winStart'' = winStart
                                  winStart' = fromJust . toBoundedInteger @Int64 $ winStart''
                              in ( "winStart = "
                                      <> (T.pack . show) winStart'
                                      <> " ,winEnd = "
                                      <> (T.pack . show) (winStart' + size),
                                    lookup (HM.fromList [("winStart", winStart)]) singlWinStart
                                      & fromJust
                                      & mapAlias rSelectViewSelect
                                  )
                          notEmpty = filter (\x -> snd x /= HM.empty) grped
                            <&> second Aeson.Object
                      sendResp (Just $ HM.fromList notEmpty) valueSerde
                    else do
                      resp <- ksGet key store
                      sendResp (mapAlias rSelectViewSelect <$> resp) valueSerde
                SessionStateStore store -> do
                  dropSurfaceTimeStamp <- ssDump store <&> Map.elems
                  let subset =
                        dropSurfaceTimeStamp
                          <&> Map.elems
                            . Map.filterWithKey \k _ -> all (`elem` HM.toList k) (HM.toList key)
                  let res =
                        subset
                          & filter (not . null) . join
                          <&> Map.toList
                            & L.sortBy (compare `on` fst) . filter (not . null) . join
                      grped = res <&> \(k, v) -> ("winStart = " <> (T.pack . show) k, mapAlias rSelectViewSelect v)
                      notEmpty = filter (\x -> snd x /= HM.empty) grped <&> second Aeson.Object
                  flip sendResp valueSerde $
                    Just . HM.fromList $ notEmpty
                TimestampedKVStateStore _ ->
                  returnErrResp StatusInternal "Impossible happened"
    ExplainPlan sql -> do
      undefined
      {-
      execPlan <- genExecutionPlan sql
      let object = HM.fromList [("PLAN", Aeson.String . T.pack $ show execPlan)]
      returnCommandQueryResp $ V.singleton (jsonObjectToStruct object)
    StartPlan (StartObjectConnector name) -> do
      IO.startIOTask scIOWorker name >> returnCommandQueryEmptyResp
    StopPlan (StopObjectConnector name) -> do
      IO.stopIOTask scIOWorker name False >> returnCommandQueryEmptyResp
      -}
    _ -> discard
  where
    create sName = do
      Log.debug . Log.buildString $ "CREATE: new stream " <> show sName
      createStreamWithShard scLDClient sName "query" scDefaultStreamRepFactor
    sendResp ma valueSerde = do
      case ma of
        Nothing -> returnCommandQueryResp V.empty
        Just x -> do
          let result = runDeser (deserializer valueSerde) x
          returnCommandQueryResp
            (V.singleton $ structToStruct "SELECTVIEW" $ jsonObjectToStruct result)
    discard = (Log.warning . Log.buildText) "impossible happened" >> returnErrResp StatusInternal "discarded method called"

executePushQueryHandler ::
  ServerContext ->
  ServerRequest 'ServerStreaming CommandPushQuery Struct ->
  IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler
  ctx@ServerContext {..}
  (ServerWriterRequest meta CommandPushQuery {..} streamSend) = defaultServerStreamExceptionHandle $ do
    Log.debug $ "Receive Push Query Request: " <> Log.buildText commandPushQueryQueryText
    plan' <- streamCodegen commandPushQueryQueryText
    case plan' of
      SelectPlan tName inNodesWithStreams outNodeWithStream builder -> do
        let sources = snd <$> inNodesWithStreams
            sink    = snd outNodeWithStream
        exists <- mapM (S.doesStreamExist scLDClient . transToStreamName) sources
        if (not . and) exists
          then do
            Log.warning $
              "At least one of the streams do not exist: "
                <> Log.buildString (show sources)
            throwIO StreamNotExist
          else do
            createStreamWithShard scLDClient (transToStreamName sink) "query" scDefaultStreamRepFactor
            -- create persistent query
            (qid, _) <-
              P.createInsertPersistentQuery
                tName
                commandPushQueryQueryText
                (P.PlainQuery $ textToCBytes <$> sources)
                serverID
                zkHandle
            -- run task
            -- FIXME: take care of the life cycle of the thread and global state
            P.setQueryStatus qid Running zkHandle
            tid <- forkIO $ runTaskWrapper tName inNodesWithStreams outNodeWithStream S.StreamTypeStream S.StreamTypeTemp builder scLDClient

            takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
            -- sub from sink stream and push to client
            consumerName <- newRandomText 20
            let sc = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
            subscribeToStreamWithoutCkp sc sink SpecialOffsetLATEST

            sending <- async (sendToClient zkHandle qid sink sc streamSend)

            forkIO $ handlePushQueryCanceled meta $ do
              killThread tid
              cancel sending
              P.setQueryStatus qid Terminated zkHandle
              unSubscribeToStreamWithoutCkp sc sink

            wait sending

      _ -> do
        Log.fatal "Push Query: Inconsistent Method Called"
        returnServerStreamingResp StatusInternal "inconsistent method called"

createStreamWithShard :: S.LDClient -> S.StreamId -> CB.CBytes -> Int -> IO ()
createStreamWithShard client streamId shardName factor = do
  S.createStream client streamId (S.def{ S.logReplicationFactor = S.defAttr1 factor })
  let extrAttr = Map.fromList [(Shard.shardStartKey, Shard.keyToCBytes minBound), (Shard.shardEndKey, Shard.keyToCBytes maxBound), (Shard.shardEpoch, "1")]
  void $ S.createStreamPartitionWithExtrAttr client streamId (Just shardName) extrAttr

--------------------------------------------------------------------------------

sendToClient ::
  ZHandle ->
  CB.CBytes ->
  T.Text ->
  SourceConnectorWithoutCkp ->
  (Struct -> IO (Either GRPCIOError ())) ->
  IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid streamName sc@SourceConnectorWithoutCkp {..} streamSend = do
  let f (e :: ZooException) = do
        Log.fatal $ "ZooKeeper Exception: " <> Log.buildString (show e)
        return $ ServerWriterResponse [] StatusAborted "failed to get status"
  handle f $
    do
      P.getQueryStatus qid zkHandle
      >>= \case
        Terminated -> return (ServerWriterResponse [] StatusAborted "")
        Created -> return (ServerWriterResponse [] StatusAlreadyExists "")
        Running -> do
          withReadRecordsWithoutCkp streamName $ \sourceRecords -> do
            let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
                structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
            void $ streamSendMany structs
          return (ServerWriterResponse [] StatusOk "")
        _ -> return (ServerWriterResponse [] StatusUnknown "")
  where
    streamSendMany = \case
      []        -> return (ServerWriterResponse [] StatusOk "")
      (x : xs') ->
        streamSend (structToStruct "SELECT" x) >>= \case
          Left err -> do
            Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
            return (ServerWriterResponse [] StatusInternal (fromString (show err)))
          Right _ -> streamSendMany xs'

--------------------------------------------------------------------------------

getFixedWinSize :: [P.PersistentQuery] -> T.Text -> IO (Maybe Time.DiffTime)
getFixedWinSize [] _ = pure Nothing
getFixedWinSize queries viewNameRaw = do
  sizes <-
    queries <&> P.queryBindedSql
      <&> parseAndRefine
        & sequence
      <&> filter \case
        RQCreate (RCreateView viewNameSQL (RSelect _ _ _ (RGroupBy _ _ (Just rWin)) _)) ->
          viewNameRaw == viewNameSQL && isFixedWin rWin
        _ -> False
      <&> map \case
        RQCreate (RCreateView _ (RSelect _ _ _ (RGroupBy _ _ (Just rWin)) _)) ->
          coeRWindowToDiffTime rWin
        _ -> error "Impossible happened..."
  pure case sizes of
    []       -> Nothing
    size : _ -> Just size
  where
    isFixedWin :: RWindow -> Bool = \case
      RTumblingWindow _  -> True
      RHoppingWIndow _ _ -> True
      RSessionWindow _   -> False
    coeRWindowToDiffTime :: RWindow -> Time.DiffTime = \case
      RTumblingWindow size  -> size
      RHoppingWIndow size _ -> size
      RSessionWindow _      -> error "Impossible happened..."

getGrpByFieldName :: [P.PersistentQuery] -> T.Text -> IO (Maybe T.Text)
getGrpByFieldName [] _ = pure Nothing
getGrpByFieldName queries viewNameRaw = do
  rSQL <- queries <&> parseAndRefine . P.queryBindedSql & sequence
  let filtered = flip filter rSQL \case
        RQCreate (RCreateView viewNameSQL (RSelect _ _ _ RGroupBy {} _))
          -> viewNameRaw == viewNameSQL
        _ -> False
  pure case filtered <&> pickCondName of
    []    -> Nothing
    x : _ -> pure x
  where
    pickCondName = \case
      RQCreate (RCreateView _ (RSelect _ _ _ (RGroupBy _ condName _) _)) -> condName
      _ -> error "Impossible happened..."

diffTimeToScientific :: Time.DiffTime -> Scientific
diffTimeToScientific = flip scientific (-9) . Time.diffTimeToPicoseconds
hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery (P.PersistentQuery queryId sqlStatement createdTime _ status _ _) =
  Query
  { queryId          = cBytesToText queryId
  , queryStatus      = getPBStatus status
  , queryCreatedTime = createdTime
  , queryQueryText   = sqlStatement
  }

createQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
createQueryHandler ctx@ServerContext{..} (ServerNormalRequest _ CreateQueryRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Create Query Request."
    <> "Query ID: "      <> Log.buildText createQueryRequestId
    <> "Query Command: " <> Log.buildText createQueryRequestQueryText
  plan <- HSC.streamCodegen createQueryRequestQueryText
  case plan of
    HSC.SelectPlan tName inNodesWithStreams outNodeWithStream builder -> do
      let sources = snd <$> inNodesWithStreams
          sink    = snd outNodeWithStream
      exists <- mapM (HS.doesStreamExist scLDClient . HCH.transToStreamName) sources
      if (not . and) exists
      then do
        Log.warning $ "At least one of the streams do not exist: "
          <> Log.buildString (show sources)
        throwIO StreamNotExist
      else do
        createStreamWithShard scLDClient (transToStreamName sink) "query" scDefaultStreamRepFactor
        (qid, timestamp) <- handleCreateAsSelect ctx plan
          createQueryRequestQueryText (P.PlainQuery $ textToCBytes <$> sources) HS.StreamTypeTemp

        consumerName <- newRandomText 20
        let sc = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
        subscribeToStreamWithoutCkp sc sink SpecialOffsetLATEST
        returnResp $
          Query
          { queryId          = cBytesToText qid
          , queryStatus      = getPBStatus Running
          , queryCreatedTime = timestamp
          , queryQueryText   = createQueryRequestQueryText
          }
    _ -> do
      Log.fatal "Push Query: Inconsistent Method Called"
      returnErrResp StatusInvalidArgument "inconsistent method called"

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListQueriesRequest ListQueriesResponse
  -> IO (ServerResponse 'Normal ListQueriesResponse)
listQueriesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List Query Request"
  queries <- P.getQueries zkHandle
  let records = map hstreamQueryToQuery queries
  let resp    = ListQueriesResponse . V.fromList $ records
  returnResp resp

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
getQueryHandler ServerContext{..} (ServerNormalRequest _metadata GetQueryRequest{..}) = do
  Log.debug $ "Receive Get Query Request. "
    <> "Query ID: " <> Log.buildText getQueryRequestId
  query <- do
    queries <- P.getQueries zkHandle
    return $ find (\P.PersistentQuery{..} -> cBytesToText queryId == getQueryRequestId) queries
  case query of
    Just q -> returnResp $ hstreamQueryToQuery q
    _      -> returnErrResp StatusNotFound "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler sc@ServerContext{..} (ServerNormalRequest _metadata TerminateQueriesRequest{..}) = queryExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  qids <-
    if terminateQueriesRequestAll
      then HM.keys <$> readMVar runningQueries
      else return . V.toList $ textToCBytes <$> terminateQueriesRequestQueryId
  terminatedQids <- Core.handleQueryTerminate sc (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then do
      Log.warning $ "Following queries cannot be terminated: "
        <> Log.buildString (show $ qids \\ terminatedQids)
      returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
    else returnResp $ TerminateQueriesResponse (V.fromList $ cBytesToText <$> terminatedQids)

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ServerContext{..} (ServerNormalRequest _metadata DeleteQueryRequest{..}) =
  queryExceptionHandle $ do
    Log.debug $ "Receive Delete Query Request. "
      <> "Query ID: " <> Log.buildText deleteQueryRequestId
    P.removeQuery (textToCBytes deleteQueryRequestId) zkHandle
    returnResp Empty

-- FIXME: Incorrect implementation!
restartQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartQueryHandler ServerContext{..} (ServerNormalRequest _metadata RestartQueryRequest{..}) = do
  Log.fatal "Restart Query Not Supported"
  returnErrResp StatusUnimplemented "restart query not suppported yet"
    -- queries <- P.withMaybeZHandle zkHandle P.getQueries
    -- case find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == restartQueryRequestId) queries of
    --   Just query -> do
    --     P.withMaybeZHandle zkHandle $ P.setQueryStatus (P.queryId query) P.Running
    --     returnResp Empty
      -- Nothing    -> returnErrResp StatusInternal "Query does not exist"

--------------------------------------------------------------------------------
-- Exception and Exception Handler

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

queryExceptionHandlers :: Handlers (StatusCode, StatusDetails)
queryExceptionHandlers =[
  Handler (\(err :: QueryTerminatedOrNotExist) -> do
    Log.warning $ Log.buildString' err
    return (StatusInvalidArgument, "Query is already terminated or does not exist"))
  ]

sqlExceptionHandlers :: Handlers (StatusCode, StatusDetails)
sqlExceptionHandlers =[
  Handler (\(err :: SomeSQLException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInvalidArgument, StatusDetails . BS.pack . formatSomeSQLException $ err))
  ]

queryExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
queryExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  sqlExceptionHandlers ++ queryExceptionHandlers ++
  connectorExceptionHandlers ++ defaultHandlers
