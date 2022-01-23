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
    watchSubscriptionHandler,
    streamingFetchHandler,
    routineForSubs,
    stopSendingRecords
  )
where


import           Control.Concurrent
import           Control.Concurrent.Async         (concurrently_)
import           Control.Exception                (Exception, SomeException,
                                                   displayException,
                                                   onException, throwIO, try)
import           Control.Monad                    (forM_, unless, when,
                                                   zipWithM)
import qualified Data.ByteString.Char8            as BS
import           Data.Function                    (on)
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (modifyIORef', newIORef,
                                                   readIORef, writeIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (catMaybes, fromJust)
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64, Word8)
import           Network.GRPC.HighLevel           (StreamRecv, StreamSend)
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Z.Data.Vector                    (Bytes)
import qualified Z.Data.Vector                    as ZV
import           Z.Foreign                        (toByteString)
import           Z.IO.LowResTimer                 (registerLowResTimer)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Common.ConsistentHashing (getAllocatedNode)
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (ConsumerExist (..),
                                                   SubscriptionIdNotFound (..),
                                                   defaultBiDiStreamExceptionHandle,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (getCommitRecordId,
                                                   getSuccessor,
                                                   insertAckedRecordId)
import           HStream.Server.Persistence       (ObjRepType (..),
                                                   mkPartitionKeysPath)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (cBytesToText, returnErrResp,
                                                   returnResp, textToCBytes)

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
      Log.debug $ "Try to create a subscription to a nonexistent stream"
               <> "Stream Name: "
               <> Log.buildString (show streamName)
      returnErrResp StatusFailedPrecondition . StatusDetails $ "stream " <> encodeUtf8 subscriptionStreamName <> " not exist"
    else do
      P.checkIfExist @ZHandle @'SubRep
        subscriptionSubscriptionId zkHandle >>= \case
        True  -> returnErrResp StatusAlreadyExists . StatusDetails $ "Subsctiption "
                                                                  <> encodeUtf8 subscriptionSubscriptionId
                                                                  <> " already exists"
        False -> do
          P.storeObject subscriptionSubscriptionId subscription zkHandle
          returnResp subscription

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler = undefined
-- deleteSubscriptionHandler ServerContext {..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest {..}) = defaultExceptionHandle $ do
--  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString (show req)
--
--  modifyMVar_ subscribeRuntimeInfo $ \store -> do
--    case HM.lookup deleteSubscriptionRequestSubscriptionId store of
--      Just infoMVar -> do
--        modifyMVar infoMVar removeSubscriptionFromZK >>=
--          \case True -> return $ HM.delete deleteSubscriptionRequestSubscriptionId store;
--                _    -> return store;
--      Nothing -> do
--        P.removeObject @ZHandle @'SubRep
--          deleteSubscriptionRequestSubscriptionId zkHandle
--        return store
--  returnResp Empty
--  where
--    -- FIXME: For now, if there are still some consumers consuming current subscription,
--    -- we ignore the delete command. Should confirm the delete semantics of delete command
--    removeSubscriptionFromZK info@SubscribeRuntimeInfo {..}
--      | HM.null sriStreamSends = do
--          -- remove sub from zk
--          P.removeObject @ZHandle @'SubRep
--            deleteSubscriptionRequestSubscriptionId zkHandle
--          let newInfo = info {sriValid = False, ssriStreamSends = HM.empty}
--          return (newInfo, True)
--      | otherwise = return (info, False)

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

--------------------------------------------------------------------------------

watchSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming WatchSubscriptionRequest WatchSubscriptionResponse
  -> IO (ServerResponse 'ServerStreaming WatchSubscriptionResponse)
watchSubscriptionHandler ctx@ServerContext{..} (ServerWriterRequest _ req@WatchSubscriptionRequest {..} streamSend) = do
  (subInfo@SubscribeRuntimeInfo{..}, isInited) <- modifyMVar scSubscribeRuntimeInfo
    ( \infoMap -> do
        case HM.lookup watchSubscriptionRequestSubscriptionId infoMap of
          Nothing -> do
            subInfo <- newSubscriptionRuntimeInfo zkHandle watchSubscriptionRequestSubscriptionId
            return (HM.insert watchSubscriptionRequestSubscriptionId subInfo infoMap, (subInfo, False))
          Just subInfo -> return (infoMap, (subInfo, True))
    )

  -- unless isInited
  --   (watchStreamShardsForSubscription ctx watchSubscriptionRequestSubscriptionId)

  stopSignal <- modifyMVar sriWatchContext
    (
      \watchCtx@WatchContext{..} -> do
        let consumerWatch =
              ConsumerWatch {
                cwConsumerName = watchSubscriptionRequestConsumerName
              , cwWatchStream = streamSend
              }
        stopSignal <- newEmptyMVar
        let signals = HM.insert watchSubscriptionRequestConsumerName stopSignal wcWatchStopSignals
        return
          ( watchCtx {
              wcWaitingConsumers = wcWaitingConsumers ++ [consumerWatch]
            , wcWatchStopSignals = signals
            }
          , stopSignal
          )
    )

  -- block util the watch stream broken or closed
  void $ takeMVar stopSignal
  modifyMVar_ sriWatchContext
    (
      \watchCtx@WatchContext{..} -> do
        let signals = HM.delete watchSubscriptionRequestConsumerName wcWatchStopSignals
        return watchCtx {wcWatchStopSignals = signals}
    )
  return $ ServerWriterResponse [] StatusCancelled . StatusDetails $ "connection broken"

--------------------------------------------------------------------------------
-- find the stream according to the subscriptionId,
-- and watch stream shards in zk.
-- when there is a new shard, assign it for Reading.
watchStreamShardsForSubscription :: ServerContext -> T.Text -> IO ()
watchStreamShardsForSubscription = undefined
--------------------------------------------------------------------------------

-- find a consumer for the orderingKey and push the notification to the consumer to
-- ask it to do streamingFetch.
assignShardForReading :: SubscribeRuntimeInfo -> OrderingKey -> IO ()
assignShardForReading info@SubscribeRuntimeInfo{..} orderingKey =
  modifyMVar_ sriWatchContext
    (
      \watchCtx@WatchContext{..} ->
        if null wcWaitingConsumers
        then
          if Set.null wcWorkingConsumers
          then return watchCtx
          else do
            let (minConsumerWorkload@ConsumerWorkload{..}, leftSet) = Set.deleteFindMin wcWorkingConsumers
            -- push SubscriptionAdd to the choosed consumer
            let ConsumerWatch {..} = cwConsumerWatch
            let stopSignal =  wcWatchStopSignals HM.! cwConsumerName
            pushAdd cwWatchStream sriSubscriptionId orderingKey stopSignal
            let newSet = Set.insert
                          (
                            minConsumerWorkload {
                              cwShards = Set.insert orderingKey cwShards
                            }
                          )
                          leftSet
            return watchCtx {wcWorkingConsumers = newSet}
        else do
          -- 1. choose the first consumer in waiting list
          let consumer@ConsumerWatch{..} = head wcWaitingConsumers
          let stopSignal = wcWatchStopSignals HM.! cwConsumerName
          -- 2. push SubscriptionAdd to the choosed consumer
          pushAdd cwWatchStream sriSubscriptionId orderingKey stopSignal
          -- 3. remove the consumer from the waiting list and add it to the workingList
          let newWaitingList = drop 1 wcWaitingConsumers
          let newWorkingSet = Set.insert
                                ( ConsumerWorkload {
                                    cwConsumerWatch = consumer
                                  , cwShards = Set.singleton orderingKey
                                  }
                                )
                                wcWorkingConsumers
          return watchCtx {wcWaitingConsumers = newWaitingList, wcWorkingConsumers = newWorkingSet}
    )
--------------------------------------------------------------------------------
pushAdd :: StreamSend WatchSubscriptionResponse -> T.Text -> T.Text -> MVar () -> IO ()
pushAdd streamSend subId orderingKey stopSignal = do
  let changeAdd = WatchSubscriptionResponseChangeChangeAdd $ WatchSubscriptionResponse_SubscriptionAdd orderingKey
  let resp = WatchSubscriptionResponse {
                watchSubscriptionResponseSubscriptionId = subId
              , watchSubscriptionResponseChange = Just changeAdd
              }
  tryPush streamSend resp stopSignal

pushRemove :: StreamSend WatchSubscriptionResponse -> T.Text -> T.Text -> MVar () ->  IO ()
pushRemove streamSend subId orderingKey stopSignal = do
  let changeRemove = WatchSubscriptionResponseChangeChangeRemove $ WatchSubscriptionResponse_SubscriptionRemove orderingKey
  let resp = WatchSubscriptionResponse {
                watchSubscriptionResponseSubscriptionId = subId
              , watchSubscriptionResponseChange = Just changeRemove
              }
  tryPush streamSend resp stopSignal

tryPush :: StreamSend WatchSubscriptionResponse -> WatchSubscriptionResponse -> MVar () ->  IO ()
tryPush streamSend resp stopSignal = do
  streamSend resp >>= \case
    Left _   -> void $ tryPutMVar stopSignal ()
    Right () -> return ()
--------------------------------------------------------------------------------
-- first, for each subscription In serverContext,
-- check if there are shards which is not assigned,
-- then run assingShardForReading.
--
-- second, try to rebalance workloads between consumers.
--
-- this should be run in a backgroud thread and start before grpc server
-- started.
routineForSubs :: ServerContext -> IO ()
routineForSubs ServerContext {..} = do
  subs <- withMVar scSubscribeRuntimeInfo
    (
      \subs -> return subs
    )
  forM_ (HM.elems subs)
    (
      \sub -> do
        tryAssign sub
        tryRebalance sub
    )
  where
    tryAssign :: SubscribeRuntimeInfo -> IO ()
    tryAssign sub@SubscribeRuntimeInfo {..} = do
      -- 1. get all shards in the stream from zk
      -- 2. for each shard, check whether it is in shardRuntimeInfos:
      --      - if not, assign it to a consumer.
      --      - if yes, do nothing in this step.
      --
      --
      let path = mkPartitionKeysPath (textToCBytes sriStreamName)
      shards <- P.tryGetChildren zkHandle path
      shardInfoMap <- readMVar sriShardRuntimeInfo
      forM_ shards
        (
          \shard ->
            if HM.member (cBytesToText shard) shardInfoMap
            then return ()
            else assignShardForReading sub (cBytesToText shard)
        )

    tryRebalance :: SubscribeRuntimeInfo -> IO ()
    tryRebalance sub@SubscribeRuntimeInfo{..} = loop
      -- check waitingList, if not empty, try to balance it.
      -- get a shard from the max consumer:
      --  - stop send to the old consuemr
      --  - push Remove to old consumer
      --  - push Add to new consumer
      --  - delete one workload from the old list
      --  - move the new consumer to working list
      where
        loop = do
          modifyMVar sriWatchContext
            (
              \watchCtx@WatchContext{..} -> do
                if null wcWaitingConsumers
                then return (watchCtx, Nothing)
                else
                  if Set.null wcWorkingConsumers
                  then return (watchCtx, Nothing)
                  else do
                    let (maxConsumer@ConsumerWorkload{..}, leftSet) = Set.deleteFindMax wcWorkingConsumers
                    if Set.size cwShards > 1
                    then do
                      let (shard, leftShards) = Set.deleteFindMin cwShards
                      let consumer@ConsumerWatch {cwWatchStream = newWatchStream , cwConsumerName = newConsumerName } = head wcWaitingConsumers
                      pushRemove
                        (cwWatchStream cwConsumerWatch)
                        sriSubscriptionId
                        shard
                        (wcWatchStopSignals HM.! (cwConsumerName cwConsumerWatch))
                      pushAdd
                        newWatchStream
                        sriSubscriptionId
                        shard
                        (wcWatchStopSignals HM.! newConsumerName)
                      let newMaxConsumer = maxConsumer {cwShards = leftShards}
                      let newWorkingConsumer = ConsumerWorkload {cwConsumerWatch = consumer, cwShards = Set.singleton shard}
                      let newWorkingSet = Set.insert newWorkingConsumer (Set.insert newMaxConsumer leftSet)
                      let newWaitingList = tail wcWaitingConsumers
                      return (watchCtx {wcWaitingConsumers = newWaitingList, wcWorkingConsumers = newWorkingSet}, Just shard)
                    else
                      return (watchCtx {wcWorkingConsumers = Set.insert maxConsumer leftSet}, Nothing)
            ) >>= \case
              Nothing -> return ()
              Just shard -> do
                stopSendingRecords sub shard
                loop

--------------------------------------------------------------------------------


stopSendingRecords :: SubscribeRuntimeInfo -> T.Text -> IO ()
stopSendingRecords SubscribeRuntimeInfo {..} shard = do
  shardMap <- readMVar sriShardRuntimeInfo
  case HM.lookup shard shardMap of
    Nothing -> return ()
    Just shardInfoMVar ->
      modifyMVar_ shardInfoMVar
        (
          \shardInfo@ShardSubscribeRuntimeInfo{..} ->
            case ssriSendStatus of
              SendRunning -> return shardInfo {ssriSendStatus = SendStopping}
              _           -> return shardInfo
        )


--------------------------------------------------------------------------------
newSubscriptionRuntimeInfo :: ZHandle -> T.Text -> IO SubscribeRuntimeInfo
newSubscriptionRuntimeInfo zkHandle subId = do
  watchCtx <- newMVar $
    WatchContext {
        wcWaitingConsumers = []
      , wcWorkingConsumers = Set.empty
      , wcWatchStopSignals = HM.empty
      }
  shardCtx <- newMVar HM.empty
  streamName <- P.getObject subId zkHandle >>= \case
    Nothing                    -> throwIO $ SubscriptionIdNotFound subId
    Just sub@Subscription {..} -> return subscriptionStreamName

  return SubscribeRuntimeInfo {
    sriSubscriptionId = subId
  , sriStreamName = streamName
  , sriWatchContext = watchCtx
  , sriShardRuntimeInfo = shardCtx
  }
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx bidiRequest =
  try (streamingFetchInternal ctx bidiRequest) >>= \case
    Right _ -> return $
      ServerBiDiResponse [] StatusUnknown . StatusDetails $ "should not reach here"
    Left (err :: SubscribeInnerError) -> handleException err
    Left _  -> return $
      ServerBiDiResponse [] StatusUnknown . StatusDetails $ ""
  where
    handleException :: SubscribeInnerError -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
    handleException GRPCStreamRecvError = return $
      ServerBiDiResponse [] StatusCancelled . StatusDetails $ "consumer recv error"
    handleException GRPCStreamRecvCloseError = return $
      ServerBiDiResponse [] StatusCancelled . StatusDetails $ "consumer is closed"
    handleException _ = return $
      ServerBiDiResponse [] StatusUnknown . StatusDetails $ ""


streamingFetchInternal
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO ()
streamingFetchInternal ctx@ServerContext {..} (ServerBiDiRequest _ streamRecv streamSend) =
  streamRecv >>= \case
    Left err -> throwIO GRPCStreamRecvError
    Right Nothing -> throwIO GRPCStreamRecvCloseError
    Right (Just firstReq) -> do
      -- check firstReq
      shardInfoMVar <- initShardRuntimeInfo firstReq

      concurrently_ (handleAckStream shardInfoMVar scLDClient streamRecv) (genRecordStream ctx shardInfoMVar)
        `onException` cleanup firstReq

  where
    cleanup :: StreamingFetchRequest -> IO ()
    cleanup StreamingFetchRequest{..} = do
      SubscribeRuntimeInfo {..} <- withMVar scSubscribeRuntimeInfo
        (
          \ infoMap -> return (infoMap HM.! streamingFetchRequestSubscriptionId)
        )

      modifyMVar_ sriWatchContext
        (
          \ watchCtx@WatchContext {..} -> do
              let workingSet = Set.map
                    (
                      \consumer@ConsumerWorkload {..} ->
                        if cwConsumerName cwConsumerWatch == streamingFetchRequestConsumerName
                        then
                          let shards = Set.delete streamingFetchRequestOrderingKey cwShards
                          in  consumer {cwShards = shards}
                        else consumer
                    )
                    wcWorkingConsumers
              let newWorkingSet = Set.filter
                    (
                      \consumer@ConsumerWorkload {..} -> Set.size cwShards > 0
                    )
                    workingSet
              return watchCtx {wcWorkingConsumers = newWorkingSet}
        )

      shardInfoMVar <- withMVar sriShardRuntimeInfo
        (
          \ infoMap -> return (infoMap HM.! streamingFetchRequestOrderingKey)
        )
      modifyMVar_ shardInfoMVar
        (
          \ info@ShardSubscribeRuntimeInfo {..} -> return info {ssriSendStatus = SendStopped}
        )


    initShardRuntimeInfo :: StreamingFetchRequest -> IO (MVar ShardSubscribeRuntimeInfo)
    initShardRuntimeInfo req@StreamingFetchRequest{..} = do
      SubscribeRuntimeInfo {..} <- withMVar scSubscribeRuntimeInfo
        (
          \ infoMap -> return (infoMap HM.! streamingFetchRequestSubscriptionId)
        )

      shardInfoMVar <- modifyMVar sriShardRuntimeInfo
        (
          \shardInfoMap ->
            if HM.member streamingFetchRequestOrderingKey shardInfoMap
            then
              return (shardInfoMap, (shardInfoMap HM.! streamingFetchRequestOrderingKey))
            else do
              -- get subscription info from zk
              P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
                Nothing -> do
                  Log.debug $ "streamingFetch error because subscription " <> Log.buildText streamingFetchRequestSubscriptionId <> " not exist."
                  throwIO $ SubscriptionIdNotFound streamingFetchRequestSubscriptionId
                Just sub@Subscription {..} -> do
                  -- create a ldCkpReader for reading new records
                  let readerName = textToCBytes (streamingFetchRequestSubscriptionId `T.append` "-" `T.append` streamingFetchRequestOrderingKey)
                  ldCkpReader <-
                    S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 1 Nothing 10
                  -- seek ldCkpReader to start offset
                  let streamID = S.mkStreamId S.StreamTypeStream (textToCBytes subscriptionStreamName)
                  logId <-
                    S.getUnderlyingLogId scLDClient streamID (Just $ textToCBytes streamingFetchRequestOrderingKey)
                  S.startReadingFromCheckpointOrStart ldCkpReader logId (Just S.LSN_MIN) S.LSN_MAX
                  -- set ldCkpReader timeout to 0
                  _ <- S.ckpReaderSetTimeout ldCkpReader 0
                  Log.debug $ "created a ldCkpReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

                  -- create a ldReader for rereading unacked records
                  ldReader <- S.newLDReader scLDClient 1 Nothing
                  Log.debug $ "created a ldReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

                  -- init SubscribeRuntimeInfo
                  let shardInfo =
                        ShardSubscribeRuntimeInfo {
                            ssriStreamName        = subscriptionStreamName
                          , ssriLogId             = logId
                          , ssriAckTimeoutSeconds = subscriptionAckTimeoutSeconds
                          , ssriLdCkpReader       = ldCkpReader
                          , ssriLdReader          = ldReader
                          , ssriWindowLowerBound  = RecordId S.LSN_MIN 0
                          , ssriWindowUpperBound  = maxBound
                          , ssriAckedRanges       = Map.empty
                          , ssriBatchNumMap       = Map.empty
                          , ssriConsumerName      = streamingFetchRequestConsumerName
                          , ssriStreamSend        = streamSend
                          , ssriSendStatus        = SendRunning
                        }
                  shardInfoMVar <- newMVar shardInfo
                  let newShardMap = HM.insert streamingFetchRequestOrderingKey shardInfoMVar shardInfoMap
                  return (newShardMap, shardInfoMVar)
        )
      modifyMVar shardInfoMVar
        (
          \shardInfo@ShardSubscribeRuntimeInfo{..} ->
            case ssriSendStatus of
              SendRunning -> return (shardInfo, Just ())
              SendStopped -> do
                let info = shardInfo {ssriStreamSend = streamSend, ssriSendStatus = SendRunning, ssriConsumerName = streamingFetchRequestConsumerName}
                return (info, Just ())
              SendStopping -> return (shardInfo, Nothing)

        ) >>= \case
          Nothing -> do
            threadDelay 1000
            initShardRuntimeInfo req
          Just _ -> return shardInfoMVar

handleAckStream
  :: MVar ShardSubscribeRuntimeInfo
  -> S.LDClient
  -> StreamRecv StreamingFetchRequest
  -> IO ()
handleAckStream shardInfoMVar ldclient streamRecv = do
  streamRecv >>= \case
    Left (err :: grpcIOError) -> do
      Log.fatal . Log.buildString $ "streamRecv error: " <> show err
      throwIO GRPCStreamRecvError
    Right Nothing -> do
      -- This means that the consumer finished sending acks actively.
      consumerName <-
        withMVar shardInfoMVar
          (
            \shardInfo@ShardSubscribeRuntimeInfo{..} -> do
              return ssriConsumerName
          )
      Log.info $ "consumer closed: " <> Log.buildText consumerName
      throwIO GRPCStreamRecvCloseError
    Right (Just streamingFetchReq@StreamingFetchRequest {..}) ->
      if V.null streamingFetchRequestAckIds
      then handleAckStream shardInfoMVar ldclient streamRecv
      else do
        doAck ldclient shardInfoMVar streamingFetchRequestAckIds
        handleAckStream shardInfoMVar ldclient streamRecv

genRecordStream
  :: ServerContext
  -> MVar ShardSubscribeRuntimeInfo
  -> IO ()
genRecordStream ctx@ServerContext {..} shardInfoMVar = do
  check

  mRecords <- doRead
  case mRecords of
    Nothing -> do
      threadDelay 1000000
      genRecordStream ctx shardInfoMVar
    Just records -> do
      doSend records
      let recordIds = V.map (fromJust . receivedRecordRecordId) records
      registerResend recordIds
      genRecordStream ctx shardInfoMVar
  where
    check =
      modifyMVar shardInfoMVar
        (
          \ info@ShardSubscribeRuntimeInfo {..} ->
            case ssriSendStatus of
              SendStopping -> return (info {ssriSendStatus = SendStopped}, Just ConsumerInValidError)
              SendStopped ->  return (info, Just ConsumerInValidError)
              SendRunning -> return (info, Nothing)
        ) >>= \case
          Nothing  -> return ()
          Just err -> throwIO err

    doRead =
      modifyMVar shardInfoMVar
        (
          \info@ShardSubscribeRuntimeInfo{..} ->
            S.ckpReaderReadAllowGap ssriLdCkpReader 1000 >>= \case
              Left gap@S.GapRecord {..} -> do
                -- insert gap range to ackedRanges
                let gapLoRecordId = RecordId gapLoLSN minBound
                    gapHiRecordId = RecordId gapHiLSN maxBound
                    newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) ssriAckedRanges
                    -- also need to insert lo_lsn record and hi_lsn record to batchNumMap
                    -- because we need to use these info in `tryUpdateWindowLowerBound` function later.
                    groupNums = map (, 0) [gapLoLSN, gapHiLSN]
                    newBatchNumMap = Map.union ssriBatchNumMap (Map.fromList groupNums)
                    newInfo = info { ssriAckedRanges = newRanges
                                   , ssriBatchNumMap = newBatchNumMap
                                   }
                Log.debug . Log.buildString $ "reader meet a gapRecord for stream " <> show ssriStreamName <> ", the gap is " <> show gap
                Log.debug . Log.buildString $ "update ackedRanges to " <> show newRanges
                Log.debug . Log.buildString $ "update batchNumMap to " <> show newBatchNumMap
                return (newInfo, Nothing)
              Right dataRecords
                | null dataRecords -> do
                    Log.debug . Log.buildString $ "reader read empty dataRecords from stream " <> show ssriStreamName
                    return (info, Nothing)
                | otherwise -> do
                    -- Log.debug . Log.buildString $ "reader read " <> show (length dataRecords) <> " records: " <> show (formatDataRecords dataRecords)

                    -- XXX: Should we add a server option to toggle Stats?
                    --
                    -- WARNING: we assume there is one stream name in all dataRecords.
                    --
                    -- Make sure you have read only ONE stream(log), otherwise you should
                    -- group dataRecords by stream name.
                    let len_bs = sum $ map (ZV.length . S.recordPayload) dataRecords
                    Stats.stream_time_series_add_record_bytes scStatsHolder (textToCBytes ssriStreamName) (fromIntegral len_bs)

                    -- TODO: List operations are very inefficient, use a more efficient data structure(vector or sth.) to replace
                    let groups = L.groupBy ((==) `on` S.recordLSN) dataRecords
                        len = length groups
                        (batch, lastBatch) = splitAt (len - 1) groups
                        -- When the number of records in an LSN exceeds the maximum number of records we
                        -- can fetch in a single read call, `batch' will be an empty list.
                    let maxReadSize = if null batch
                                        then length . last $ lastBatch
                                        else length . last $ batch
                        lastLSN = S.recordLSN . head . head $ lastBatch
                    Log.debug $ "maxReadSize = "<> Log.buildInt maxReadSize <> ", lastLSN = " <> Log.buildInt lastLSN

                    -- `ckpReaderReadAllowGap` will return a specific number of records, which may cause the last LSN's
                    -- records to be truncated, so we need to do another point read to get all the records of the last LSN.
                    lastBatchRecords <- fetchLastLSN ssriLogId lastLSN ssriLdCkpReader maxReadSize
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
                        let (finalLastLSN, maxRecordId) = case lastBatchRecords of
                             -- In this case, newGroups = batch and finalLastLSN should be the lsn of the last record in batch.
                             [] -> let (lastGroupLSN, cnt) = last groupNums
                                       lastRId = RecordId lastGroupLSN (cnt - 1)
                                    in (lastGroupLSN, lastRId)
                             -- In this case, newGroups = [lastBatchRecords] or batch ++ [lastBatchRecords],
                             -- in both cases finalLastLSN should be lastLSN
                             xs -> (lastLSN, RecordId lastLSN (fromIntegral $ length xs - 1))
                        Log.debug $ "finalLastLSN = " <> Log.buildInt finalLastLSN <> ", maxRecordId = " <> Log.buildString (show maxRecordId)

                        let newBatchNumMap = Map.union ssriBatchNumMap (Map.fromList groupNums)
                            receivedRecords = fetchResult newGroups
                            newInfo = info { ssriBatchNumMap = newBatchNumMap
                                           , ssriWindowUpperBound = maxRecordId
                                           }
                        void $ S.ckpReaderSetTimeout ssriLdCkpReader 0
                        S.ckpReaderStartReading ssriLdCkpReader ssriLogId (finalLastLSN + 1) S.LSN_MAX
                        return (newInfo, Just receivedRecords)
        )

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

    doSend records =
      void $ withMVar shardInfoMVar
        (
          \info@ShardSubscribeRuntimeInfo{..} -> do
            Log.debug $
              Log.buildString "send " <> Log.buildInt (V.length records)
                <> " records to " <> "consumer " <> Log.buildText ssriConsumerName
            ssriStreamSend (StreamingFetchResponse records) >>= \case
              Left err -> do
                -- if send record error, throw exception
                Log.fatal . Log.buildString $ "send error, will remove a consumer: " <> show err
                throwIO GRPCStreamSendError
              Right _ -> do
                return ()
        )

    registerResend recordIds =
      withMVar shardInfoMVar
        (
          \info@ShardSubscribeRuntimeInfo{..} ->
            void $ registerLowResTimer
                 (fromIntegral ssriAckTimeoutSeconds)
                 (void $ forkIO $ resendTimeoutRecords recordIds)
        )

    resendTimeoutRecords recordIds =
      withMVar shardInfoMVar
        (
          \info@ShardSubscribeRuntimeInfo{..} -> do
            let unackedRecordIds = filterUnackedRecordIds recordIds ssriAckedRanges ssriWindowLowerBound
            if V.null unackedRecordIds
              then return ()
              else do
                Log.info $ Log.buildInt (V.length unackedRecordIds) <> " records need to be resend"

                cache <- newIORef Map.empty
                lastResendLSN <- newIORef 0
                V.iforM_ unackedRecordIds $ \i RecordId {..} -> do
                  dataRecords <- getDataRecords ssriLdReader ssriLogId ssriBatchNumMap cache lastResendLSN recordIdBatchId
                  if null dataRecords
                    then do
                      -- TODO: retry or error
                      Log.fatal $ "can not read log " <> Log.buildString (show ssriLogId) <> " at " <> Log.buildString (show recordIdBatchId)
                    else do
                        let rr = mkReceivedRecord (fromIntegral recordIdBatchIndex) (dataRecords !! fromIntegral recordIdBatchIndex)
                        ssriStreamSend (StreamingFetchResponse $ V.singleton rr) >>= \case
                          Left grpcIOError -> do
                            -- TODO: handle error
                            Log.fatal $ "streamSend error:" <> Log.buildString (show grpcIOError)
                          Right _ -> return ()

                void $ registerResend unackedRecordIds
        )
      where
        filterUnackedRecordIds recordIds ackedRanges windowLowerBound =
          flip V.filter recordIds $ \recordId ->
            (recordId >= windowLowerBound)
              && case Map.lookupLE recordId ackedRanges of
                Nothing                               -> True
                Just (_, RecordIdRange _ endRecordId) -> recordId > endRecordId

        getDataRecords ldreader logId batchNumMap cache lastResendLSN recordIdBatchId = do
          lastLSN <- readIORef lastResendLSN
          if lastLSN == recordIdBatchId
             then do
               readIORef cache <&> fromJust . Map.lookup recordIdBatchId
             else do
               S.readerStartReading ldreader logId recordIdBatchId recordIdBatchId
               let batchSize = fromJust $ Map.lookup recordIdBatchId batchNumMap
               res <- S.readerRead ldreader (fromIntegral batchSize)
               modifyIORef' cache (pure $ Map.singleton recordIdBatchId res)
               modifyIORef' lastResendLSN (pure recordIdBatchId)
               return res

--------------------------------------------------------------------------------

fetchResult :: [[S.DataRecord Bytes]] -> V.Vector ReceivedRecord
fetchResult groups = V.fromList $ concatMap (zipWith mkReceivedRecord [0 ..]) groups

mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
mkReceivedRecord index record =
  let recordId = RecordId (S.recordLSN record) (fromIntegral index)
   in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

commitCheckPoint :: S.LDClient -> S.LDSyncCkpReader -> T.Text -> RecordId -> IO ()
commitCheckPoint client reader streamName RecordId {..} = do
  logId <- S.getUnderlyingLogId client (transToStreamName streamName) Nothing
  S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

doAck
  :: S.LDClient
  -> MVar ShardSubscribeRuntimeInfo
  -> V.Vector RecordId
  -> IO ()
doAck client infoMVar ackRecordIds =
  modifyMVar_ infoMVar
    ( \info@ShardSubscribeRuntimeInfo {..} -> do
        let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b ssriWindowLowerBound a ssriBatchNumMap) ssriAckedRanges ackRecordIds
        let commitLSN = getCommitRecordId newAckedRanges ssriBatchNumMap

        case tryUpdateWindowLowerBound newAckedRanges ssriWindowLowerBound ssriBatchNumMap commitLSN of
          Just (ranges, newLowerBound) -> do
            Log.info $ "update window lower bound, from {"
                    <> Log.buildString (show ssriWindowLowerBound)
                    <> "} to {"
                    <> Log.buildString (show newLowerBound)
                    <> "}"

            commitCheckPoint client ssriLdCkpReader ssriStreamName (fromJust commitLSN)
            Log.info $ "commit checkpoint = " <> Log.buildString (show . fromJust $ commitLSN)
            Log.debug $ "after commitCheckPoint, length of ackedRanges is: " <> Log.buildInt (Map.size ranges)
                     <> ", 10 smallest ackedRanges: " <> Log.buildString (printAckedRanges $ Map.take 10 ranges)

            -- after a checkpoint is committed, informations of records less then and equal to checkpoint are no need to be retained, so just clear them
            let newBatchNumMap = updateBatchNumMap (fromJust commitLSN) ssriBatchNumMap
            Log.debug $ "update batchNumMap to: " <> Log.buildString (show newBatchNumMap)
            return $ info {ssriAckedRanges = ranges, ssriWindowLowerBound = newLowerBound, ssriBatchNumMap = newBatchNumMap}
          Nothing ->
            return $ info {ssriAckedRanges = newAckedRanges}
    )
  where
    updateBatchNumMap RecordId{..} mp = Map.dropWhileAntitone (<= recordIdBatchId) mp

tryUpdateWindowLowerBound
  :: Map.Map RecordId RecordIdRange -- ^ ackedRanges
  -> RecordId                       -- ^ lower bound record of current window
  -> Map.Map Word64 Word32          -- ^ batchNumMap
  -> Maybe RecordId                 -- ^ commitPoint
  -> Maybe (Map.Map RecordId RecordIdRange, RecordId)
tryUpdateWindowLowerBound ackedRanges lowerBoundRecordId batchNumMap (Just commitPoint) =
  Map.lookupMin ackedRanges >>= \(_, RecordIdRange minStartRecordId minEndRecordId) ->
    if | minStartRecordId == lowerBoundRecordId && (recordIdBatchId minEndRecordId) == (recordIdBatchId commitPoint) ->
            -- The current ackedRange [minStartRecordId, minEndRecordId] contains the complete batch record and can be committed directly,
            -- so remove the hole range [minStartRecordId, minEndRecordId], update windowLowerBound to successor of minEndRecordId
           Just (Map.delete minStartRecordId ackedRanges, getSuccessor minEndRecordId batchNumMap)
       | minStartRecordId == lowerBoundRecordId ->
           -- The ackedRange [minStartRecordId, commitPoint] contains the complete batch record and will be committed,
           -- update ackedRange to [successor of commitPoint, minEndRecordId]
           let newAckedRanges = Map.delete minStartRecordId ackedRanges
               startRecordId = getSuccessor commitPoint batchNumMap
               newAckedRanges' = Map.insert startRecordId (RecordIdRange startRecordId minEndRecordId) newAckedRanges
            in Just(newAckedRanges', startRecordId)
       | otherwise -> Nothing
tryUpdateWindowLowerBound _ _ _ Nothing = Nothing

--------------------------------------------------------------------------------
data SubscribeInnerError = GRPCStreamRecvError
                             | GRPCStreamRecvCloseError
                             | GRPCStreamSendError
                             | ConsumerInValidError
  deriving (Show)
instance Exception SubscribeInnerError













