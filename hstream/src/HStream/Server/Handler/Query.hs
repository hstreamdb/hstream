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
import           Control.Exception                (handle)
import           Control.Monad                    (join)
import qualified Data.Aeson                       as Aeson
import           Data.Bifunctor
import           Data.Function                    (on, (&))
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (atomicModifyIORef',
                                                   readIORef)
import           Data.Int                         (Int64)
import           Data.List                        (find, (\\))
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (catMaybes, fromJust, isJust)
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

import           HStream.Connector.HStore
import qualified HStream.Connector.HStore         as HCH
import qualified HStream.Logger                   as Log
import           HStream.Processing.Connector     (SourceConnector (..))
import           HStream.Processing.Encoding      (Deserializer (..),
                                                   Serde (..), Serializer (..))
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Store
import qualified HStream.Processing.Stream        as PS
import           HStream.Processing.Type          hiding (StreamName, Timestamp)
import           HStream.SQL                      (parseAndRefine)
import           HStream.SQL.AST
import           HStream.SQL.Codegen              hiding (StreamName)
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.ExecPlan             (genExecutionPlan)
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
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
  (ServerNormalRequest _metadata CreateQueryStreamRequest {..}) = defaultExceptionHandle $ do
    RQSelect select <- parseAndRefine createQueryStreamRequestQueryStatements
    tName <- genTaskName
    let sName = streamStreamName <$> createQueryStreamRequestQueryStream
        rFac  = maybe 1 (fromIntegral . streamReplicationFactor) createQueryStreamRequestQueryStream
    (builder, source, sink, _) <-
      genStreamBuilderWithStream tName sName select
    S.createStream scLDClient (transToStreamName sink) $ S.LogAttrs (S.HsLogAttrs rFac Map.empty)
    let query = P.StreamQuery (textToCBytes <$> source) (textToCBytes sink)
    void $
      handleCreateAsSelect
        sc
        (PS.build builder)
        createQueryStreamRequestQueryStatements
        query
        S.StreamTypeStream
    let streamResp = Stream sink (fromIntegral rFac)
        -- FIXME: The value query returned should have been fully assigned
        queryResp = def { queryId = tName }
    returnResp $ CreateQueryStreamResponse (Just streamResp) (Just queryResp)

--------------------------------------------------------------------------------

executeQueryHandler ::
  ServerContext ->
  ServerRequest 'Normal CommandQuery CommandQueryResponse ->
  IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext {..} (ServerNormalRequest _metadata CommandQuery {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Query Request: " <> Log.buildText commandQueryStmtText
  plan' <- streamCodegen commandQueryStmtText
  case plan' of
    SelectPlan {} -> returnErrResp StatusInternal "inconsistent method called"
    -- execute plans that can be executed with this method
    CreateViewPlan schema sources sink taskBuilder _repFactor materialized ->
      do
        create (transToViewStreamName sink)
        >> handleCreateAsSelect
          sc
          taskBuilder
          commandQueryStmtText
          (P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema)
          S.StreamTypeView
        >> atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
        >> returnCommandQueryEmptyResp
    CreateSinkConnectorPlan _cName _ifNotExist _sName _cConfig _ -> do
      createConnector sc commandQueryStmtText >> returnCommandQueryEmptyResp
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
            Nothing -> returnErrResp StatusInternal "VIEW not found"
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
      execPlan <- genExecutionPlan sql
      let object = HM.fromList [("PLAN", Aeson.String . T.pack $ show execPlan)]
      returnCommandQueryResp $ V.singleton (jsonObjectToStruct object)
    _ -> discard
  where
    mkLogAttrs = S.HsLogAttrs scDefaultStreamRepFactor
    create sName = do
      let attrs = mkLogAttrs Map.empty
      Log.debug . Log.buildString $
        "CREATE: new stream " <> show sName
          <> " with attributes: "
          <> show attrs
      S.createStream scLDClient sName (S.LogAttrs attrs)
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
  ServerContext {..}
  (ServerWriterRequest meta CommandPushQuery {..} streamSend) = defaultStreamExceptionHandle $ do
    Log.debug $ "Receive Push Query Request: " <> Log.buildText commandPushQueryQueryText
    plan' <- streamCodegen commandPushQueryQueryText
    case plan' of
      SelectPlan sources sink taskBuilder -> do
        exists <- mapM (S.doesStreamExist scLDClient . transToStreamName) sources
        if (not . and) exists
          then do
            Log.warning $
              "At least one of the streams do not exist: "
                <> Log.buildString (show sources)
            throwIO StreamNotExist
          else do
            S.createStream
              scLDClient
              (transToTempStreamName sink)
              (S.LogAttrs $ S.HsLogAttrs scDefaultStreamRepFactor Map.empty)
            -- create persistent query
            (qid, _) <-
              P.createInsertPersistentQuery
                (getTaskName taskBuilder)
                commandPushQueryQueryText
                (P.PlainQuery $ textToCBytes <$> sources)
                serverID
                zkHandle
            -- run task
            -- FIXME: take care of the life cycle of the thread and global state
            tid <-
              forkIO $
                P.setQueryStatus qid Running zkHandle
                  >> runTaskWrapper S.StreamTypeStream S.StreamTypeTemp taskBuilder scLDClient
            takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
            _ <-
              forkIO $
                handlePushQueryCanceled
                  meta
                  (killThread tid >> P.setQueryStatus qid Terminated zkHandle)
            ldreader' <-
              S.newLDRsmCkpReader
                scLDClient
                (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
                S.checkpointStoreLogID
                5000
                1
                Nothing
                10
            let sc = hstoreSourceConnector scLDClient ldreader' S.StreamTypeTemp
            subscribeToStream sc sink Latest
            sendToClient zkHandle qid sc streamSend
      _ -> do
        Log.fatal "Push Query: Inconsistent Method Called"
        returnStreamingResp StatusInternal "inconsistent method called"

--------------------------------------------------------------------------------

sendToClient ::
  ZHandle ->
  CB.CBytes ->
  SourceConnector ->
  (Struct -> IO (Either GRPCIOError ())) ->
  IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid sc@SourceConnector {..} streamSend = do
  let f (e :: ZooException) = do
        Log.fatal $ "ZooKeeper Exception: " <> Log.buildString (show e)
        return $ ServerWriterResponse [] StatusAborted "failed to get status"
  handle f $
    do
      P.getQueryStatus qid zkHandle
      >>= \case
        Terminated -> return (ServerWriterResponse [] StatusUnknown "")
        Created -> return (ServerWriterResponse [] StatusUnknown "")
        Running -> do
          sourceRecords <- readRecords
          let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
              structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
          streamSendMany structs
  where
    streamSendMany = \case
      [] -> sendToClient zkHandle qid sc streamSend
      (x : xs') ->
        streamSend (structToStruct "SELECT" x) >>= \case
          Left err -> do
            Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
            return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
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
createQueryHandler ctx@ServerContext{..} (ServerNormalRequest _ CreateQueryRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Query Request."
    <> "Query ID: "      <> Log.buildText createQueryRequestId
    <> "Query Command: " <> Log.buildText createQueryRequestQueryText
  plan <- HSC.streamCodegen createQueryRequestQueryText
  case plan of
    HSC.SelectPlan sources sink taskBuilder -> do
      let taskBuilder' = taskBuilderWithName taskBuilder createQueryRequestId
      exists <- mapM (HS.doesStreamExist scLDClient . HCH.transToStreamName) sources
      if (not . and) exists
      then do
        Log.warning $ "At least one of the streams do not exist: "
          <> Log.buildString (show sources)
        throwIO StreamNotExist
      else do
        HS.createStream scLDClient (HCH.transToTempStreamName sink)
          (HS.LogAttrs $ HS.HsLogAttrs scDefaultStreamRepFactor Map.empty)
        (qid, timestamp) <- handleCreateAsSelect ctx taskBuilder'
          createQueryRequestQueryText (P.PlainQuery $ textToCBytes <$> sources) HS.StreamTypeTemp
        ldreader' <- HS.newLDRsmCkpReader scLDClient
          (textToCBytes (T.append (getTaskName taskBuilder') "-result"))
          HS.checkpointStoreLogID 5000 1 Nothing 10
        let sc = HCH.hstoreSourceConnector scLDClient ldreader' HS.StreamTypeTemp -- FIXME: view or temp?
        subscribeToStream sc sink Latest
        returnResp $
          Query
          { queryId          = cBytesToText qid
          , queryStatus      = getPBStatus Running
          , queryCreatedTime = timestamp
          , queryQueryText   = createQueryRequestQueryText
          }
    _ -> do
      Log.fatal "Push Query: Inconsistent Method Called"
      returnErrResp StatusInternal "inconsistent method called"

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
    _      -> returnErrResp StatusInternal "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler sc@ServerContext{..} (ServerNormalRequest _metadata TerminateQueriesRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Terminate Query Request. "
    <> "Query ID: " <> Log.buildString (show terminateQueriesRequestQueryId)
  qids <-
    if terminateQueriesRequestAll
      then HM.keys <$> readMVar runningQueries
      else return . V.toList $ textToCBytes <$> terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate sc (HSC.ManyQueries qids)
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
  defaultExceptionHandle $ do
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
  returnErrResp StatusInternal "restart query not suppported yet"
    -- queries <- P.withMaybeZHandle zkHandle P.getQueries
    -- case find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == restartQueryRequestId) queries of
    --   Just query -> do
    --     P.withMaybeZHandle zkHandle $ P.setQueryStatus (P.queryId query) P.Running
    --     returnResp Empty
      -- Nothing    -> returnErrResp StatusInternal "Query does not exist"
