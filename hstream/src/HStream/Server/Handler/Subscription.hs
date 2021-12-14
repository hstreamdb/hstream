{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Subscription
  (
    createSubscriptionHandler,
    deleteSubscriptionHandler,
    listSubscriptionsHandler,
    checkSubscriptionExistHandler,
    streamingFetchHandler
  )
where

import           Control.Concurrent
import           Control.Monad                    (when, zipWithM)
import           Data.Function                    (on)
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (modifyIORef', newIORef,
                                                   readIORef, writeIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (catMaybes, fromJust)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64, Word8)
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Z.Data.Vector                    (Bytes)
import qualified Z.Data.Vector                    as ZV
import           Z.Foreign                        (toByteString)
import           Z.IO.LowResTimer                 (registerLowResTimer)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (getStartRecordId,
                                                   getSuccessor,
                                                   insertAckedRecordId)
import           HStream.Server.LoadBalance       (getNodesRanking)
import           HStream.Server.Persistence       (ObjRepType (..))
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (returnErrResp, returnResp,
                                                   textToCBytes)

--------------------------------------------------------------------------------

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ServerContext {..} (ServerNormalRequest _metadata subscription@Subscription {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString (show subscription)

  let streamName = transToStreamName subscriptionStreamName
  streamExists <- S.doesStreamExist scLDClient streamName
  if not streamExists
    then do
      Log.warning $
        "Try to create a subscription to a nonexistent stream"
          <> "Stream Name: "
          <> Log.buildString (show streamName)
      returnErrResp StatusInternal $ StatusDetails "stream not exist"
    else do
      logId <- S.getUnderlyingLogId scLDClient (transToStreamName subscriptionStreamName)
      offset <- convertOffsetToRecordId logId

      P.checkIfExist @ZHandle @'SubRep
        subscriptionSubscriptionId zkHandle >>= \case
        True  -> returnErrResp StatusUnknown "Subsctiption already exists"
        False -> do
          let newSub = subscription {subscriptionOffset = Just . SubscriptionOffset . Just . SubscriptionOffsetOffsetRecordOffset $ offset}
          P.storeObject subscriptionSubscriptionId newSub zkHandle
          returnResp subscription
  where
    convertOffsetToRecordId logId = do
      let SubscriptionOffset {..} = fromJust subscriptionOffset
          sOffset = fromJust subscriptionOffsetOffset
      case sOffset of
        SubscriptionOffsetOffsetSpecialOffset subOffset ->
          case subOffset of
            Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) -> do
              return $ RecordId S.LSN_MIN 0
            Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST) -> do
              startLSN <- (+ 1) <$> S.getTailLSN scLDClient logId
              return $ RecordId startLSN 0
            Enumerated _ -> error "Wrong SpecialOffset!"
        SubscriptionOffsetOffsetRecordOffset recordId -> return recordId

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext {..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString (show req)

  modifyMVar_ subscribeRuntimeInfo $ \store -> do
    case HM.lookup deleteSubscriptionRequestSubscriptionId store of
      Just infoMVar -> do
        modifyMVar infoMVar removeSubscriptionFromZK >>= \case
          True -> do
            modifyMVar_ subscriptionCtx (return . Map.delete (T.unpack deleteSubscriptionRequestSubscriptionId))
            return $ HM.delete deleteSubscriptionRequestSubscriptionId store
          False -> return store
      Nothing -> do
        P.removeObject @ZHandle @'SubRep
          deleteSubscriptionRequestSubscriptionId zkHandle

        -- Note: The subscription may never be fetched so there is no 'SubscriptionContext' in zk
        P.checkIfExist @ZHandle @'SubCtxRep
          deleteSubscriptionRequestSubscriptionId zkHandle >>= \case
          True  -> P.removeObject @ZHandle @'SubCtxRep
                    deleteSubscriptionRequestSubscriptionId zkHandle
          False -> return ()

        return store
  returnResp Empty
  where
    removeSubscriptionFromZK info@SubscribeRuntimeInfo {..}
      | HM.null sriStreamSends = do
          -- remove sub from zk
          P.removeObject @ZHandle @'SubRep
            deleteSubscriptionRequestSubscriptionId zkHandle
          -- remove subctx from zk
          P.removeObject @ZHandle @'SubCtxRep
            deleteSubscriptionRequestSubscriptionId zkHandle
          let newInfo = info {sriValid = False, sriStreamSends = HM.empty}
          return (newInfo, True)
      | otherwise = return (info, False)

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = checkSubscriptionExistRequestSubscriptionId
  res <- P.checkIfExist @ZHandle @'SubRep sid zkHandle
  returnResp . CheckSubscriptionExistResponse $ res

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler ServerContext {..} (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse . V.fromList . Map.elems <$> P.listObjects zkHandle
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx@ServerContext {..} (ServerBiDiRequest _ streamRecv streamSend) = do
  Log.debug "Receive streamingFetch request"

  consumerNameRef   <- newIORef T.empty
  subscriptionIdRef <- newIORef T.empty
  handleRequest True consumerNameRef subscriptionIdRef
  where
    handleRequest isFirst consumerNameRef subscriptionIdRef = do
      streamRecv >>= \case
        Left (err :: grpcIOError) -> do
          Log.fatal . Log.buildString $ "streamRecv error: " <> show err
          cleanupStreamSend isFirst consumerNameRef subscriptionIdRef >>= \case
            Nothing -> return $ ServerBiDiResponse [] StatusInternal (StatusDetails "")
            Just errorMsg -> return $ ServerBiDiResponse [] StatusInternal (StatusDetails errorMsg)
        Right Nothing -> do
          -- This means that the consumer finished sending acks actively.
          name <- readIORef consumerNameRef
          Log.info $ "consumer closed:" <> Log.buildText name
          cleanupStreamSend isFirst consumerNameRef subscriptionIdRef >>= \case
            Nothing -> return $ ServerBiDiResponse [] StatusInternal (StatusDetails "")
            Just errorMsg -> return $ ServerBiDiResponse [] StatusInternal (StatusDetails errorMsg)
        Right (Just streamingFetchReq@StreamingFetchRequest {..})
          | isFirst -> do
            -- if it is the first fetch request from current client, need to do some extra check and add a new streamSender
              Log.debug $ "stream recive requst from " <> Log.buildText streamingFetchRequestConsumerName <> ", do check in isFirst branch"
              -- the subscription has to exist and be bound to a server node
              P.checkIfExist @ZHandle @'SubRep
                streamingFetchRequestSubscriptionId zkHandle >>= \case
                True -> do
                  P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
                    Just SubscriptionContext{..}
                      | _subctxNode == serverID -> do
                          doFirstFetchCheck streamingFetchReq
                      | otherwise -> return $
                          ServerBiDiResponse [] StatusInternal "The subscription is bound to another node. Call `lookupSubscription` to get the right one"
                    Nothing -> do
                      Log.debug $ Log.buildText streamingFetchRequestSubscriptionId <> " need to assign to a server node."
                      nodeIDs <- getNodesRanking ctx <&> fmap serverNodeId
                      if serverID `L.elem` nodeIDs
                        then do
                          let subCtx = SubscriptionContext { _subctxNode = serverID }
                          modifyMVar_ subscriptionCtx
                            (\ctxs -> do
                                newCtxMVar <- newMVar subCtx
                                return $ Map.insert (T.unpack streamingFetchRequestSubscriptionId) newCtxMVar ctxs
                            )
                          P.storeObject streamingFetchRequestSubscriptionId subCtx zkHandle -- sync subctx to zk
                          doFirstFetchCheck streamingFetchReq
                        else do
                          return $ ServerBiDiResponse [] StatusInternal "There is no available node for allocating the subscription"
                False ->
                  return $ ServerBiDiResponse [] StatusInternal "Subscription does not exist"
          | otherwise -> do
              handleAcks
                streamingFetchRequestSubscriptionId
                streamingFetchRequestAckIds
                consumerNameRef
                subscriptionIdRef
      where
        doFirstFetchCheck StreamingFetchRequest{..} = do
          writeIORef consumerNameRef streamingFetchRequestConsumerName
          writeIORef subscriptionIdRef streamingFetchRequestSubscriptionId

          mRes <- modifyMVar subscribeRuntimeInfo
            ( \store -> do
                case HM.lookup streamingFetchRequestSubscriptionId store of
                  Just infoMVar -> do
                    modifyMVar_ infoMVar
                      ( \info@SubscribeRuntimeInfo {..} -> do
                          -- bind a new sender to current client
                          let newSends = HM.insert streamingFetchRequestConsumerName streamSend sriStreamSends
                          if V.null sriSignals
                            then return $ info {sriStreamSends = newSends}
                            else do
                              -- wake up all threads waiting for a new consumer to join
                              V.forM_ sriSignals $ flip putMVar ()
                              return $ info {sriStreamSends = newSends, sriSignals = V.empty}
                      )
                    return (store, Nothing)
                  Nothing -> do
                    P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
                      Nothing -> return (store, Just "Subscription has been removed")
                      Just sub@Subscription {..} -> do
                        let startRecordId = getStartRecordId sub
                        newInfoMVar <-
                          initSubscribe
                          scLDClient
                          streamingFetchRequestSubscriptionId
                          subscriptionStreamName
                          streamingFetchRequestConsumerName
                          startRecordId
                          streamSend
                          subscriptionAckTimeoutSeconds
                        Log.info $ "Subscription " <> Log.buildString (show subscriptionSubscriptionId) <> " inits done."
                        let newStore = HM.insert streamingFetchRequestSubscriptionId newInfoMVar store
                        return (newStore, Nothing)
            )
          case mRes of
            Just errorMsg -> do
              consumerName <- readIORef consumerNameRef
              Log.fatal $ "consumer " <> Log.buildText consumerName <> " error: " <> Log.buildString (show errorMsg)
              return $ ServerBiDiResponse [] StatusInternal (StatusDetails errorMsg)
            Nothing ->
              handleAcks
              streamingFetchRequestSubscriptionId
              streamingFetchRequestAckIds
              consumerNameRef
              subscriptionIdRef

    handleAcks subId acks consumerNameRef subscriptionIdRef =
      if V.null acks
        then handleRequest False consumerNameRef subscriptionIdRef
        else do
          withMVar subscribeRuntimeInfo ( return . HM.lookup subId ) >>= \case
            Just infoMVar -> do
              doAck scLDClient infoMVar acks
              handleRequest False consumerNameRef subscriptionIdRef
            Nothing ->
              return $ ServerBiDiResponse [] StatusInternal (StatusDetails "Subscription has been removed")

    -- We should cleanup according streamSend before returning ServerBiDiResponse.
    cleanupStreamSend True _ _ = return Nothing
    cleanupStreamSend False consumerNameRef subscriptionIdRef =
      withMVar subscribeRuntimeInfo
        ( \store -> do
            subscriptionId <- readIORef subscriptionIdRef
            consumerName <- readIORef consumerNameRef
            case HM.lookup subscriptionId store of
              Nothing -> return $ Just "Subscription has been removed"
              Just infoMVar -> do
                modifyMVar_ infoMVar
                  ( \info@SubscribeRuntimeInfo {..} -> do
                      if sriValid
                        then do
                          let newStreamSends = HM.delete consumerName sriStreamSends
                          return $ info {sriStreamSends = newStreamSends}
                        else return info
                  )
                return Nothing
        )

    initSubscribe ldclient subscriptionId streamName consumerName startRecordId sSend ackTimeout = do
      -- create a ldCkpReader for reading new records
      ldCkpReader <-
        S.newLDRsmCkpReader
          ldclient
          (textToCBytes subscriptionId)
          S.checkpointStoreLogID
          5000
          1
          Nothing
          10
      -- seek ldCkpReader to start offset
      logId <- S.getUnderlyingLogId ldclient (transToStreamName streamName)
      let startLSN = recordIdBatchId startRecordId
      S.ckpReaderStartReading ldCkpReader logId startLSN S.LSN_MAX
      -- set ldCkpReader timeout to 0
      _ <- S.ckpReaderSetTimeout ldCkpReader 0
      Log.debug $ "created a ldCkpReader for subscription {" <> Log.buildText subscriptionId <> "} with startLSN {" <> Log.buildInt startLSN <> "}"

      -- create a ldReader for rereading unacked records
      ldReader <- S.newLDReader ldclient 1 Nothing
      Log.debug $ "created a ldReader for subscription {" <> Log.buildText subscriptionId <> "}"

      -- init SubscribeRuntimeInfo
      let info =
            SubscribeRuntimeInfo
              { sriStreamName = streamName,
                sriLogId = logId,
                sriAckTimeoutSeconds = ackTimeout,
                sriLdCkpReader = ldCkpReader,
                sriLdReader = Just ldReader,
                sriWindowLowerBound = startRecordId,
                sriWindowUpperBound = maxBound,
                sriAckedRanges = Map.empty,
                sriBatchNumMap = Map.empty,
                sriStreamSends = HM.singleton consumerName sSend,
                sriValid = True,
                sriSignals = V.empty
              }

      infoMVar <- newMVar info
      -- create a task for reading and dispatching records periodicly
      _ <- forkIO $ readAndDispatchRecords infoMVar
      return infoMVar

    -- read records from logdevice and dispatch them to consumers
    readAndDispatchRecords runtimeInfoMVar = do
      Log.debug "enter readAndDispatchRecords"

      modifyMVar runtimeInfoMVar doReadAndDispatch >>= \case
        Nothing -> return ()
        Just signal -> do
          void $ takeMVar signal
          readAndDispatchRecords runtimeInfoMVar
      where
        doReadAndDispatch info@SubscribeRuntimeInfo {..}
          | not sriValid = do
              return (info, Nothing)
          | HM.null sriStreamSends = do
              signal <- newEmptyMVar
              return (info {sriSignals = V.cons signal sriSignals}, Just signal)
          | otherwise = do
              void $ registerLowResTimer 10 $ void . forkIO $ readAndDispatchRecords runtimeInfoMVar
              doRead info >>= \case
                (newInfo, Nothing)      -> return (newInfo, Nothing)
                (newInfo, Just records) -> doDispatch records newInfo

        doRead info@SubscribeRuntimeInfo {..} = do
          S.ckpReaderReadAllowGap sriLdCkpReader 1000 >>= \case
            Left gap@S.GapRecord {..} -> do
              -- insert gap range to ackedRanges
              let gapLoRecordId = RecordId gapLoLSN minBound
                  gapHiRecordId = RecordId gapHiLSN maxBound
                  newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) sriAckedRanges
                  -- also need to insert lo_lsn record and hi_lsn record to batchNumMap
                  -- because we need to use these info in `tryUpdateWindowLowerBound` function later.
                  groupNums = map (, 0) [gapLoLSN, gapHiLSN]
                  newBatchNumMap = Map.union sriBatchNumMap (Map.fromList groupNums)
                  newInfo = info { sriAckedRanges = newRanges
                                 , sriBatchNumMap = newBatchNumMap
                                 }
              Log.debug . Log.buildString $ "reader meet a gapRecord for stream " <> show sriStreamName <> ", the gap is " <> show gap
              Log.debug . Log.buildString $ "update ackedRanges to " <> show newRanges
              Log.debug . Log.buildString $ "update batchNumMap to " <> show newBatchNumMap
              return (newInfo, Nothing)
            Right dataRecords
              | null dataRecords -> do
                  Log.debug . Log.buildString $ "reader read empty dataRecords from stream " <> show sriStreamName
                  return (info, Nothing)
              | otherwise -> do
                  Log.debug . Log.buildString $ "reader read " <> show (length dataRecords) <> " records: " <> show (formatDataRecords dataRecords)

                  -- XXX: Should we add a server option to toggle Stats?
                  --
                  -- WARNING: we assume there is one stream name in all dataRecords.
                  --
                  -- Make sure you have read only ONE stream(log), otherwise you should
                  -- group dataRecords by stream name.
                  let len_bs = sum $ map (ZV.length . S.recordPayload) dataRecords
                  Stats.stream_time_series_add_record_bytes scStatsHolder (textToCBytes sriStreamName) (fromIntegral len_bs)

                  -- TODO: List operations are very inefficient, use a more efficient data structure(vector or sth.) to replace
                  let groups = L.groupBy ((==) `on` S.recordLSN) dataRecords
                      len = length groups
                      (batch, lastBatch) = splitAt (len - 1) groups
                      -- When the number of records in an LSN exceeds the maximum number of records we
                      -- can fetch in a single read call, `batch' will be an empty list.
                  when (null lastBatch) $ do
                      Log.fatal $ "lastBatch empty: " <> "groups = " <> Log.buildString (show groups)
                               <> ", length of groups = " <> Log.buildInt len
                  Log.debug $ "length batch=" <> Log.buildInt (length batch)
                           <> ", length lastBatch=" <> Log.buildInt (length lastBatch)
                  let maxReadSize = if null batch
                                      then length . last $ lastBatch
                                      else length . last $ batch
                      lastLSN = S.recordLSN . head . head $ lastBatch
                  Log.debug $ "maxReadSize = "<> Log.buildInt maxReadSize <> ", lastLSN=" <> Log.buildInt lastLSN

                  lastBatchRecords <- fetchLastLSN sriLogId lastLSN sriLdCkpReader maxReadSize
                  (newGroups, isEmpty) <- if | null batch && null lastBatchRecords -> do
                                                 Log.debug $ "doRead: both batch and lastBatchRecords are empty, lastLSN = " <> Log.buildInt lastLSN
                                                 return ([[]], True)
                                             | null batch -> return ([lastBatchRecords], False)
                                             | null lastBatchRecords -> do
                                                 Log.debug $ "doRead: read lastBatchRecords return empty, lastLSN = " <> Log.buildInt lastLSN
                                                 return (batch, False)
                                             | otherwise -> return (batch ++ [lastBatchRecords], False)

                  if isEmpty
                    then return (info, Nothing)
                    else do
                      let groupNums = map (\gp -> (S.recordLSN $ head gp, (fromIntegral $ length gp) :: Word32)) newGroups
                          maxRecordId = RecordId lastLSN (fromIntegral $ length lastBatchRecords - 1)
                          -- update window upper bound and batchNumMap
                          newBatchNumMap = Map.union sriBatchNumMap (Map.fromList groupNums)
                          receivedRecords = fetchResult newGroups
                          newInfo = info { sriBatchNumMap = newBatchNumMap
                                         , sriWindowUpperBound = maxRecordId
                                         }
                      void $ S.ckpReaderSetTimeout sriLdCkpReader 0
                      S.ckpReaderStartReading sriLdCkpReader sriLogId (lastLSN + 1) S.LSN_MAX
                      return (newInfo, Just receivedRecords)

        doDispatch receivedRecords info@SubscribeRuntimeInfo {..} = do
          newStreamSends <- dispatchRecords receivedRecords sriStreamSends
          let receivedRecordIds = V.map (fromJust . receivedRecordRecordId) receivedRecords
              newInfo = info { sriStreamSends = newStreamSends }
          -- register task for resending timeout records
          void $ registerLowResTimer
               (fromIntegral sriAckTimeoutSeconds * 10)
               (void $ forkIO $ tryResendTimeoutRecords receivedRecordIds sriLogId runtimeInfoMVar)
          return (newInfo, Nothing)

        fetchLastLSN :: S.C_LogID -> S.LSN -> S.LDSyncCkpReader -> Int -> IO [S.DataRecord (ZV.PrimVector Word8)]
        fetchLastLSN logId lsn reader size = do
          void $ S.ckpReaderSetTimeout reader 10
          S.ckpReaderStartReading reader logId lsn lsn
          run []
          where
            run res = do
              records <- S.ckpReaderRead reader size
              if null records
                then return res
                else run (res ++ records)

        formatDataRecords records =
          L.foldl' (\acc s -> acc ++ ["(" <> show (S.recordLSN s) <> "," <> show (S.recordBatchOffset s) <> ")"]) [] records

    filterUnackedRecordIds recordIds ackedRanges windowLowerBound =
      flip V.filter recordIds $ \recordId ->
        (recordId >= windowLowerBound)
          && case Map.lookupLE recordId ackedRanges of
            Nothing                               -> True
            Just (_, RecordIdRange _ endRecordId) -> recordId > endRecordId

    tryResendTimeoutRecords recordIds logId infoMVar = do
      Log.debug "enter tryResendTimeoutRecords"
      modifyMVar infoMVar resendTimeoutRecords >>= \case
        Nothing -> return ()
        Just signal -> do
          void $ takeMVar signal
          tryResendTimeoutRecords recordIds logId infoMVar
      where
        registerResend records timeout =
          registerLowResTimer timeout $
            void . forkIO $ tryResendTimeoutRecords records logId infoMVar

        resendTimeoutRecords info@SubscribeRuntimeInfo{sriValid = False} = return (info, Nothing)
        resendTimeoutRecords info@SubscribeRuntimeInfo{sriValid = True, ..} = do
          let unackedRecordIds = filterUnackedRecordIds recordIds sriAckedRanges sriWindowLowerBound
          if V.null unackedRecordIds
            then return (info, Nothing)
            else do
              Log.info $ Log.buildInt (V.length unackedRecordIds) <> " records need to be resend"
              doResend info unackedRecordIds

        -- TODO: maybe we can read these unacked records concurrently
        -- TODO: if all senders in streamSendValidRef are invalid, no need to do resend
        doResend info@SubscribeRuntimeInfo {..} unackedRecordIds
          | HM.null sriStreamSends = do
              Log.debug . Log.buildString $ "no consumer to resend unacked msg, will block"
              signal <- newEmptyMVar
              return (info {sriSignals = V.cons signal sriSignals}, Just signal)
          | otherwise = do
              let consumerNum = HM.size sriStreamSends
              streamSendValidRef <- newIORef $ V.replicate consumerNum True
              let senders = HM.toList sriStreamSends

              cache <- newIORef Map.empty
              lastResendLSN <- newIORef 0
              V.iforM_ unackedRecordIds $ \i RecordId {..} -> do
                dataRecords <- getDataRecords cache lastResendLSN recordIdBatchId
                if null dataRecords
                  then do
                    -- TODO: retry or error
                    Log.fatal $ "can not read log " <> Log.buildString (show logId) <> " at " <> Log.buildString (show recordIdBatchId)
                  else do
                    let ci = i `mod` consumerNum
                    streamSendValid <- readIORef streamSendValidRef
                    when (streamSendValid V.! ci) $ do
                      let cs = snd $ senders L.!! ci
                          rr = mkReceivedRecord (fromIntegral recordIdBatchIndex) (dataRecords !! fromIntegral recordIdBatchIndex)
                      cs (StreamingFetchResponse $ V.singleton rr) >>= \case
                        Left grpcIOError -> do
                          Log.fatal $ "streamSend error:" <> Log.buildString (show grpcIOError)
                          let newStreamSendValid = V.update streamSendValid (V.singleton (ci, False))
                          writeIORef streamSendValidRef newStreamSendValid
                        Right _ -> return ()

              void $ registerResend unackedRecordIds (fromIntegral sriAckTimeoutSeconds * 10)

              valids <- readIORef streamSendValidRef
              let newStreamSends = map snd $ L.filter (\(i, _) -> valids V.! i) $ zip [0 ..] senders
              return (info {sriStreamSends = HM.fromList newStreamSends}, Nothing)
          where
            getDataRecords cache lastResendLSN recordIdBatchId = do
              lastLSN <- readIORef lastResendLSN
              if lastLSN == recordIdBatchId
                 then do
                   readIORef cache <&> fromJust . Map.lookup recordIdBatchId
                 else do
                   S.readerStartReading (fromJust sriLdReader) logId recordIdBatchId recordIdBatchId
                   let batchSize = fromJust $ Map.lookup recordIdBatchId sriBatchNumMap
                   res <- S.readerRead (fromJust sriLdReader) (fromIntegral batchSize)
                   modifyIORef' cache (pure $ Map.singleton recordIdBatchId res)
                   modifyIORef' lastResendLSN (pure recordIdBatchId)
                   return res

--------------------------------------------------------------------------------
--

fetchResult :: [[S.DataRecord Bytes]] -> V.Vector ReceivedRecord
fetchResult groups = V.fromList $ concatMap (zipWith mkReceivedRecord [0 ..]) groups

mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
mkReceivedRecord index record =
  let recordId = RecordId (S.recordLSN record) (fromIntegral index)
   in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

commitCheckPoint :: S.LDClient -> S.LDSyncCkpReader -> T.Text -> RecordId -> IO ()
commitCheckPoint client reader streamName RecordId {..} = do
  logId <- S.getUnderlyingLogId client $ transToStreamName streamName
  S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

dispatchRecords
  :: Show a
  => V.Vector ReceivedRecord
  -> HM.HashMap ConsumerName (StreamingFetchResponse -> IO (Either a ()))
  -> IO (HM.HashMap ConsumerName (StreamingFetchResponse -> IO (Either a ())))
dispatchRecords records streamSends
  | HM.null streamSends = return HM.empty
  | otherwise = do
    let slen = HM.size streamSends
    Log.debug $ Log.buildString "ready to dispatchRecords to " <> Log.buildInt slen <> " consumers"
    let initVec = V.replicate slen V.empty
    -- recordGroups aggregates the data to be sent by each sender
    let recordGroups =
          V.ifoldl'
            ( \vec idx record ->
                let senderIdx = idx `mod` slen -- Assign the idx-th record to the senderIdx-th sender to send
                    dataSet = vec V.! senderIdx -- get the set of data to be sent by snederIdx-th sender
                    newSet = V.snoc dataSet record -- add idx-th record to dataSet
                 in V.update vec $ V.singleton (senderIdx, newSet)
            )
            initVec
            records

    newSenders <- zipWithM doDispatch (cycle . HM.toList $ streamSends) (V.toList recordGroups)
    return . HM.fromList . catMaybes $ newSenders
  where
    doDispatch (name, sender) record = do
      Log.debug $ Log.buildString "dispatch " <> Log.buildInt (V.length record) <> " records to " <> "consumer " <> Log.buildText name
      sender (StreamingFetchResponse record) >>= \case
        Left err -> do
          -- if send record error, this batch of records will resend next round
          Log.fatal . Log.buildString $ "dispatch error, will remove a consumer: " <> show err
          return Nothing
        Right _ -> do
          return $ Just (name, sender)

doAck
  :: S.LDClient
  -> MVar SubscribeRuntimeInfo
  -> V.Vector RecordId
  -> IO ()
doAck client infoMVar ackRecordIds =
  modifyMVar_ infoMVar
    ( \info@SubscribeRuntimeInfo {..} -> do
        if sriValid
          then do
            Log.debug $ "before handle acks, length of ackedRanges is: " <> Log.buildInt (Map.size sriAckedRanges)
            let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b sriWindowLowerBound a sriBatchNumMap) sriAckedRanges ackRecordIds
            Log.debug $ "after handle acks, length of ackedRanges is: " <> Log.buildInt (Map.size newAckedRanges)
                     <> ", 10 smallest ackedRanges: " <> Log.buildString (printAckedRanges $ Map.take 10 newAckedRanges)

            case tryUpdateWindowLowerBound newAckedRanges sriWindowLowerBound sriBatchNumMap of
              Just (ranges, newLowerBound, checkpointRecordId) -> do
                Log.debug . Log.buildString $ "newWindowLowerBound = " <> show newLowerBound <> ", checkpointRecordId = " <> show checkpointRecordId
                commitCheckPoint client sriLdCkpReader sriStreamName checkpointRecordId
                -- after a checkpoint is committed, informations of records before checkpoint are no need to be retained, so just clear them
                let newBatchNumMap = updateBatchNumMap newLowerBound sriBatchNumMap
                Log.info $ "update window lower bound, from {"
                        <> Log.buildString (show sriWindowLowerBound)
                        <> "} to {"
                        <> Log.buildString (show newLowerBound)
                        <> "}"
                return $ info {sriAckedRanges = ranges, sriWindowLowerBound = newLowerBound, sriBatchNumMap = newBatchNumMap}
              Nothing ->
                return $ info {sriAckedRanges = newAckedRanges}
          else return info
    )
  where
    updateBatchNumMap RecordId{..} mp = Map.dropWhileAntitone (< recordIdBatchId) mp

tryUpdateWindowLowerBound
  :: Map.Map RecordId RecordIdRange -- ^ ackedRanges
  -> RecordId                       -- ^ lower bound record of current window
  -> Map.Map Word64 Word32          -- ^ batchNumMap
  -> Maybe (Map.Map RecordId RecordIdRange, RecordId, RecordId)
tryUpdateWindowLowerBound ackedRanges lowerBoundRecordId batchNumMap =
  Map.lookupMin ackedRanges >>= \(_, RecordIdRange minStartRecordId minEndRecordId) ->
    if minStartRecordId == lowerBoundRecordId
      then Just (Map.delete minStartRecordId ackedRanges, getSuccessor minEndRecordId batchNumMap, minEndRecordId)
      else Nothing
