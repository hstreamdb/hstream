{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
import           Control.Concurrent.Async         (async, cancel, race_)
import           Control.Exception                (catch, throwIO, try)
import           Control.Monad                    (forM_, forever, unless, when)
import           Data.Function                    (on)
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (IORef, modifyIORef',
                                                   newIORef, readIORef,
                                                   writeIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isNothing)
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64, Word8)
import           Network.GRPC.HighLevel           (StreamRecv, StreamSend)
import           Network.GRPC.HighLevel.Generated
import           Z.Data.Vector                    (Bytes)
import qualified Z.Data.Vector                    as ZV
import           Z.Foreign                        (toByteString)
import           ZooKeeper.Types                  (ZHandle)

import           Control.Concurrent.STM           (atomically, check, readTVar,
                                                   registerDelay)
import           Data.Int                         (Int32, Int64)
import           HStream.Common.ConsistentHashing (getAllocatedNode)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception         (StreamNotExist (..),
                                                   SubscribeInnerError (..),
                                                   SubscriptionIdNotFound (..),
                                                   SubscriptionWatchOnDifferentNode (..),
                                                   defaultBiDiStreamExceptionHandle,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (bindSubToStreamPath,
                                                   getCommitRecordId,
                                                   getSuccessor,
                                                   insertAckedRecordId,
                                                   orderingKeyToStoreKey,
                                                   removeSubFromStreamPath)
import           HStream.Server.Persistence       (ObjRepType (..),
                                                   mkPartitionKeysPath)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (cBytesToText,
                                                   currentTimestampMS,
                                                   returnResp, textToCBytes)

--------------------------------------------------------------------------------

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata sub@Subscription{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  bindSubToStreamPath zkHandle streamName subName
  catch (Core.createSubscription ctx sub) $
    \(e :: StreamNotExist) -> removeSubFromStreamPath zkHandle streamName subName >> throwIO e
  returnResp sub
  where
    streamName = textToCBytes subscriptionStreamName
    subName = textToCBytes subscriptionSubscriptionId

-- FIXME: depend on memory info to deal with delete operation may be wrong, even if all the create/delete requests
-- are redirected to same server. What if this server crash, or the consistante hash choose another server to deal
-- these requests? We need some way to rebuild all these memory infos first.
deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest
  { deleteSubscriptionRequestSubscriptionId = subId }) = defaultExceptionHandle $ do
  hr <- readMVar loadBalanceHashRing
  unless (serverNodeId (getAllocatedNode hr subId) == serverID) $
    throwIO SubscriptionWatchOnDifferentNode

  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req
  subscription <- P.getObject @ZHandle @'SubRep subId zkHandle
  when (isNothing subscription) $ throwIO (SubscriptionIdNotFound subId)
  Core.deleteSubscription ctx (fromJust subscription)
  returnResp Empty

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
listSubscriptionsHandler sc (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse <$> Core.listSubscriptions sc
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res

--------------------------------------------------------------------------------

watchSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming WatchSubscriptionRequest WatchSubscriptionResponse
  -> IO (ServerResponse 'ServerStreaming WatchSubscriptionResponse)
watchSubscriptionHandler ServerContext{..} (ServerWriterRequest _ req@WatchSubscriptionRequest {..} streamSend) = do
  Log.debug $ "Receive WatchSubscription request " <> Log.buildString (show req)
  (SubscribeRuntimeInfo{..}, isInited) <- modifyMVar scSubscribeRuntimeInfo
    ( \infoMap -> do
        case HM.lookup watchSubscriptionRequestSubscriptionId infoMap of
          Nothing -> do
            subInfo <- newSubscriptionRuntimeInfo zkHandle watchSubscriptionRequestSubscriptionId
            return (HM.insert watchSubscriptionRequestSubscriptionId subInfo infoMap, (subInfo, False))
          Just subInfo -> return (infoMap, (subInfo, True))
    )

  -- unless isInited
  --   (watchStreamShardsForSubscription ctx watchSubscriptionRequestSubscriptionId)

  stopSignal <- modifyMVar sriWatchContext $
    \ctx -> addNewConsumerToCtx ctx watchSubscriptionRequestConsumerName streamSend
  Log.debug "watchHandler: ready to block..."
  -- block util the watch stream broken or closed
  void $ takeMVar stopSignal
  Log.debug "watchHanlder: will end"
  modifyMVar_ sriWatchContext (`removeConsumerFromCtx` watchSubscriptionRequestConsumerName)
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
assignShardForReading SubscribeRuntimeInfo{..} orderingKey = do
  Log.debug $ "try to assign key: " <> Log.buildText orderingKey
  modifyMVar_ sriWatchContext
    (
      \watchCtx@WatchContext{..} ->
              -- if there are wcWaitingConsumers, assign waiting consumer first
        if | not . null $ wcWaitingConsumers -> do
              -- FIXME: check the process of adding a consumer to waiting list, simplify the process
              -- 1. choose the first consumer in waiting list
              let consumer@ConsumerWatch{..} = head wcWaitingConsumers
              Log.debug $ "[assignShardForReading]: get consumer " <> Log.buildText cwConsumerName <> " from waitingList"
              stopSignal <- newEmptyMVar
              -- 2. push SubscriptionAdd to the choosed consumer
              pushAdd cwWatchStream cwConsumerName sriSubscriptionId orderingKey stopSignal
              -- 3. remove the consumer from the waiting list and add it to the workingList
              let newWaitingList = tail wcWaitingConsumers
              let newWorkload = mkConsumerWorkload consumer (Set.singleton orderingKey)
              let newWorkingSet = Set.insert newWorkload wcWorkingConsumers
              return watchCtx {wcWaitingConsumers = newWaitingList, wcWorkingConsumers = newWorkingSet}
              -- if no wcWaitingConsumers and no workingConsumers, no need to do assign
           | Set.null wcWorkingConsumers -> return watchCtx
              -- if no wcWaitingConsumers but workingConsumers are not empty, the consumer with the minimum workload is seleted as the assign target
           | otherwise -> do
              let (minConsumerWorkload@ConsumerWorkload{cwConsumerWatch = ConsumerWatch {..}, ..}, leftSet) = Set.deleteFindMin wcWorkingConsumers
              Log.debug $ "[assignShardForReading]: get consumer " <> Log.buildText cwConsumerName <> " from working set"
              -- push SubscriptionAdd to the choosed consumer
              -- FIXME: will error when no stopSignal for consumerName
              let stopSignal = wcWatchStopSignals HM.! cwConsumerName
              -- FIXME: if pushAdd error, currently the server will try to send a stopSignal to watch handler, but that's not enough.
              -- 1. current key will be add to the minConsumerWorkload, but actually client won't receive any notification of key adding, so the
              --   key won't be consume
              -- 2. It is neccessary to confirm when pushAdd will error and hanlde each error case correctly
              pushAdd cwWatchStream cwConsumerName sriSubscriptionId orderingKey stopSignal
              let newConsumerWorkload = minConsumerWorkload { cwShards = Set.insert orderingKey cwShards }
              let newSet = Set.insert newConsumerWorkload leftSet
              return watchCtx {wcWorkingConsumers = newSet}
    )

--------------------------------------------------------------------------------

pushAdd :: StreamSend WatchSubscriptionResponse -> T.Text -> T.Text -> T.Text -> MVar () -> IO ()
pushAdd streamSend consumerName subId orderingKey stopSignal = do
  Log.info $ "notify consumer " <> Log.buildText consumerName <> " to add a new orderingKey " <> Log.buildText orderingKey
  let changeAdd = WatchSubscriptionResponseChangeChangeAdd $ WatchSubscriptionResponse_SubscriptionAdd orderingKey
  let resp = WatchSubscriptionResponse
           { watchSubscriptionResponseSubscriptionId = subId
           , watchSubscriptionResponseChange = Just changeAdd
           }
  tryPush streamSend resp stopSignal

pushRemove :: StreamSend WatchSubscriptionResponse -> T.Text -> T.Text -> T.Text -> MVar () ->  IO ()
pushRemove streamSend consumerName subId orderingKey stopSignal = do
  Log.info $ "notify consumer " <> Log.buildText consumerName <> " to remove a orderingKey " <> Log.buildText orderingKey
  let changeRemove = WatchSubscriptionResponseChangeChangeRemove $ WatchSubscriptionResponse_SubscriptionRemove orderingKey
  let resp = WatchSubscriptionResponse
           { watchSubscriptionResponseSubscriptionId = subId
           , watchSubscriptionResponseChange = Just changeRemove
           }
  tryPush streamSend resp stopSignal

tryPush :: StreamSend WatchSubscriptionResponse -> WatchSubscriptionResponse -> MVar () ->  IO ()
tryPush streamSend resp stopSignal = do
  streamSend resp >>= \case
    Left _   -> do
      Log.e "push watch resp error"
      void $ tryPutMVar stopSignal ()
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
  subs <- withMVar scSubscribeRuntimeInfo return
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
      Log.debug $  "scan for sub: " <> Log.buildText sriSubscriptionId
      let path = mkPartitionKeysPath (textToCBytes sriStreamName)
      -- FIXME: should we make a strict check here? tryGetChildren will ignore the case
      -- which path xxx/{streamName}/keys not exists. But here it should be exists.
      -- IMO a strict check here is neccessary for the case: a client create stream in store
      -- successfully but failed in create zk path, but it never retry the create req later
      shards <- P.tryGetChildren zkHandle path
      Log.debug $  "get shards num: " <> Log.buildInt (length shards)
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
                if null wcWaitingConsumers || Set.null wcWorkingConsumers
                  then return (watchCtx, Nothing)
                  else do
                    let (maxConsumer@ConsumerWorkload{cwConsumerWatch = oldWatch, ..}, leftSet) = Set.deleteFindMax wcWorkingConsumers
                    let oldWatchStream = cwWatchStream oldWatch
                    let oldConsumerName = cwConsumerName oldWatch
                    if Set.size cwShards > 1
                      then do
                        let (shard, leftShards) = Set.deleteFindMin cwShards
                        let consumer@ConsumerWatch { cwWatchStream = newWatchStream , cwConsumerName = newConsumerName } = head wcWaitingConsumers
                        pushRemove oldWatchStream oldConsumerName sriSubscriptionId
                          shard (wcWatchStopSignals HM.! oldConsumerName)
                        pushAdd newWatchStream newConsumerName sriSubscriptionId
                          shard (wcWatchStopSignals HM.! newConsumerName)

                        let newMaxConsumer = maxConsumer {cwShards = leftShards}
                        let newWorkingConsumer = mkConsumerWorkload consumer (Set.singleton shard)
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
  watchCtx <- newMVar $ WatchContext
    { wcWaitingConsumers = []
    , wcWorkingConsumers = Set.empty
    , wcWatchStopSignals = HM.empty
    }
  shardCtx <- newMVar HM.empty
  streamName <- P.getObject subId zkHandle >>= \case
    Nothing                -> throwIO $ SubscriptionIdNotFound subId
    Just Subscription {..} -> return subscriptionStreamName

  return SubscribeRuntimeInfo
    { sriSubscriptionId = subId
    , sriStreamName = streamName
    , sriWatchContext = watchCtx
    , sriShardRuntimeInfo = shardCtx
    }

--------------------------------------------------------------------------------
--

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx@ServerContext{..} (ServerBiDiRequest _ streamRecv streamSend) = defaultBiDiStreamExceptionHandle $ do
  fetcher@Fetcher{..} <- initFetcher
  firstReq <- streamRecv
  subCtx <- case firstReq of
    Left _                -> throwIO GRPCStreamRecvError
    Right Nothing         -> throwIO GRPCStreamRecvCloseError
    Right (Just req@StreamingFetchRequest{..}) -> do
      context@SubscriptionCtx{..} <- initCtx ctx req
      Log.debug $ "init subscriptionCtx done for sub: " <> Log.buildText subscriptionId <> ", key: " <> Log.buildText subKey
      writeChan eventCh $ ACKEVENT streamingFetchRequestAckIds
      return context
  let ackTimeoutSeconds = subAckTimeoutSeconds subCtx

  -- background thread to run a timer
  -- FIXME: one server instance can share a common timer thread, no need to start a timer for each fetch handler
  th1 <- async $ timer eventCh
  -- background thread to receive acks from client
  th2 <- async $ waitAcks subCtx streamRecv eventCh
  -- background thread to send records to client
  th3 <- async $ sendRes subCtx streamSend eventCh responseCh
  -- background thread to fetching data from store
  th4 <- async $ try (readRecords scStatsHolder subCtx eventCh) >>= \case
    Left (e :: S.SomeHStoreException) -> do
      Log.fatal $ logSubscriptionCtx subCtx <> "read records exception: " <> Log.buildString (show e)
      writeChan eventCh . STOPEVENT . Left $ StoreError e
    Right _ -> return ()
  let cancelation = cancel th1 >> cancel th2 >> cancel th3 >> cancel th4

  let sliceWindow = SliceWindow
        { ackedRanges       = Map.empty
        , batchNumMap       = Map.empty
        , windowLowerBound  = RecordId S.LSN_MIN 0
        }
  windowRef <- newIORef sliceWindow

  inflightRecordsRef <- newIORef Map.empty
  -- mainLoop to handle event
  handleEvent subCtx fetcher windowRef inflightRecordsRef ackTimeoutSeconds >>= \case
    Right _ -> return $
      ServerBiDiResponse [] StatusUnknown . StatusDetails $ "should not reach here"
    Left (err :: SubscribeInnerError) -> cleanup subCtx cancelation >> onError err
  where
    onError :: SubscribeInnerError -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
    onError GRPCStreamRecvError = return $
      ServerBiDiResponse [] StatusCancelled . StatusDetails $ "consumer recv error"
    onError GRPCStreamRecvCloseError =
        return $ ServerBiDiResponse [] StatusCancelled . StatusDetails $ "consumer is closed"
    onError _ = return $
      ServerBiDiResponse [] StatusUnknown . StatusDetails $ ""

    cleanup :: SubscriptionCtx -> IO () -> IO ()
    cleanup SubscriptionCtx{..} cancelation = do
      -- FIXME: check if it's safe to use fromJust
      SubscribeRuntimeInfo {..} <- withMVar scSubscribeRuntimeInfo
        (return . fromJust . HM.lookup subscriptionId)

      modifyMVar_ sriWatchContext
        (
          \watchCtx@WatchContext {..} -> do
             let workingSet = Set.map
                   (
                     \consumer@ConsumerWorkload{cwConsumerWatch = ConsumerWatch {..}, ..} ->
                       if cwConsumerName == subConsumerName
                       then
                         let shards = Set.delete subKey cwShards
                          in consumer {cwShards = shards}
                       else consumer
                   )
                   wcWorkingConsumers
             -- FIXME: if a consumer has an empty cwShards, it will be removed from watchCtx, does this means
             -- this consumer can not receive any new keys in the future? is it right?
             -- IMO, consumer with an empty cwShards should move to waitingList so that server can dispatch
             -- new keys to it later.
             let newWorkingSet = Set.filter (\ConsumerWorkload {..} -> not . Set.null $ cwShards) workingSet
             return watchCtx {wcWorkingConsumers = newWorkingSet}
        )
      cancelation

handleEvent
  :: SubscriptionCtx
  -> Fetcher
  -> IORef SliceWindow
  -> IORef (Map.Map Int64 (V.Vector RecordId))
  -> Int32
  -> IO (Either SubscribeInnerError ())
handleEvent subCtx fetcher@Fetcher{..} windowRef inflightRecordsRef ackTimeoutSeconds = do
  startFetcher fetcher >>= loop
  where
    loop mailBox = readChan mailBox >>= \case
      ACKEVENT acks -> do
        unless (V.null acks) $
          doAck subCtx windowRef acks
        loop mailBox
      DATAREADYEVENT datas -> do
        Log.debug $ logSubscriptionCtx subCtx <> "receive DATAREADYEVENT"
        window <- readIORef windowRef
        let newWindow = insertDataToWindow window datas
        writeIORef windowRef newWindow
        case datas of
          GapData _ -> return ()
          NormalData records -> do
            let responsRecords = fetchResult records
            writeChan responseCh responsRecords
            inflightRecords <- readIORef inflightRecordsRef
            let rids = V.map (fromJust . receivedRecordRecordId) responsRecords
            timeout <- currentTimestampMS <&> (+ fromIntegral ackTimeoutSeconds)
            writeIORef inflightRecordsRef $ Map.insert timeout rids inflightRecords
        loop mailBox
      ACKTIMEOUTEVENT -> do
        inflightRecords <- readIORef inflightRecordsRef
        current <- currentTimestampMS
        let (timeoutRecords, remainRecords) = Map.spanAntitone (<= current) inflightRecords
        unless (Map.null timeoutRecords) $ do
          let timeoutRecordIds = V.concat $ Map.elems timeoutRecords
          SliceWindow{..} <- readIORef windowRef
          let unackedRecordIds = filterUnackedRecordIds timeoutRecordIds ackedRanges windowLowerBound
          Log.debug $ logSubscriptionCtx subCtx <> "unackedRecordIds={" <> Log.buildString (show unackedRecordIds) <> "}"
          if V.null unackedRecordIds
            then writeIORef inflightRecordsRef remainRecords
            else do
              void . forkIO $ readRetransRecords subCtx unackedRecordIds responseCh batchNumMap
              let newInflightRecords = Map.insert (current + fromIntegral ackTimeoutSeconds) unackedRecordIds remainRecords
              writeIORef inflightRecordsRef newInflightRecords
        loop mailBox
      STOPEVENT res -> return res

-- spawn two background threads, one thread will forward new event to streamingFetch main loop,
-- another thread will waiting a stop signal, and if the signal received, it will write a stop
-- event to main loop, and stop the both spawned threads.
startFetcher :: Fetcher -> IO (Chan FetchEvent)
startFetcher Fetcher{..} = do
  mailBox <- newChan
  void . forkIO $ receiveEvent mailBox stopCh
  return mailBox
  where
    receiveEvent mailBox stopChannel = race_ (waitEvent mailBox) (stop stopChannel mailBox)
    stop ch mailBox = readChan ch >>= writeChan mailBox . STOPEVENT
    waitEvent mailBox = forever $ readChan eventCh >>= writeChan mailBox

waitAcks :: SubscriptionCtx -> StreamRecv StreamingFetchRequest -> Chan FetchEvent -> IO ()
waitAcks ctx streamRecv ch =
  waitLoop
  where
    waitLoop = do
      streamRecv >>= \case
        Left (err :: grpcIOError) -> do
          Log.fatal $ logSubscriptionCtx ctx <> "streamRecv error: " <> Log.buildString (show err)
          writeChan ch . STOPEVENT $ Left GRPCStreamRecvError
        Right Nothing -> do
          -- -- This means that the consumer finished sending acks actively.
          Log.info $ logSubscriptionCtx ctx <> "consumer closed."
          writeChan ch . STOPEVENT $ Left GRPCStreamRecvCloseError
        Right (Just StreamingFetchRequest {..}) -> do
          writeChan ch $ ACKEVENT streamingFetchRequestAckIds
          waitLoop

sendRes
  :: SubscriptionCtx
  -> StreamSend StreamingFetchResponse
  -> Chan FetchEvent
  -> Chan (V.Vector ReceivedRecord)
  -> IO ()
sendRes ctx@SubscriptionCtx{..} streamSend eventCh dataCh = do
  sendLoop
  where
    sendLoop = do
      records <- readChan dataCh
      Log.info $ logSubscriptionCtx ctx <> "send " <> Log.buildInt (V.length records)
               <> " records to consumer " <> Log.buildText subConsumerName
      let response = StreamingFetchResponse{streamingFetchResponseReceivedRecords = records}
      streamSend response >>= \case
        Left err -> do
          -- if send record error, throw exception
          Log.fatal $ logSubscriptionCtx ctx <> "send error, will remove a consumer: " <> Log.buildString (show err)
          writeChan eventCh . STOPEVENT $ Left GRPCStreamSendError
        Right _ -> sendLoop

readRecords :: Stats.StatsHolder -> SubscriptionCtx -> Chan FetchEvent -> IO ()
readRecords statsHolder ctx@SubscriptionCtx{..} ch = forever $ do
  S.ckpReaderReadAllowGap subLdCkpReader 1000 >>= \case
    Left gap -> do
     Log.debug $ logSubscriptionCtx ctx <> "reader meet a gapRecord: " <> Log.buildString (show gap)
     writeChan ch . DATAREADYEVENT . GapData $ gap
    Right dataRecords
      | null dataRecords -> do
          Log.debug $ logSubscriptionCtx ctx <> "reader read empty dataRecords"
          -- FIXME: avoid hard code, find another way all make it a config
          threadDelay 1000000
      | otherwise -> do
          -- XXX: Should we add a server option to toggle Stats?
          -- WARNING: we assume there is one stream name in all dataRecords.
          --
          -- Make sure you have read only ONE stream(log), otherwise you should
          -- group dataRecords by stream name.
          let len_bs = sum $ map (ZV.length . S.recordPayload) dataRecords
          Stats.stream_time_series_add_record_bytes statsHolder (textToCBytes subStreamName) (fromIntegral len_bs)

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

          -- `ckpReaderReadAllowGap` will return a specific number of records, which may cause the last LSN's
          -- records to be truncated, so we need to do another point read to get all the records of the last LSN.
          lastBatchRecords <- fetchLastLSN subLogId lastLSN subLdCkpReader maxReadSize
          newGroups <- if | null batch && null lastBatchRecords -> do
                             Log.debug $ logSubscriptionCtx ctx
                                      <> "doRead: both batch and lastBatchRecords are empty, lastLSN = " <> Log.buildInt lastLSN
                             return [[]]
                          | null batch -> return [lastBatchRecords]
                          | null lastBatchRecords -> do
                              Log.debug $ logSubscriptionCtx ctx
                                       <> "doRead: read lastBatchRecords return empty, lastLSN = " <> Log.buildInt lastLSN
                              return batch
                          | otherwise -> return (batch ++ [lastBatchRecords])
          let finalLastLSN = case lastBatchRecords of
               -- In this case, newGroups = batch and finalLastLSN should be the lsn of the last record in batch.
               [] -> S.recordLSN . head . last $ batch
               -- In this case, newGroups = [lastBatchRecords] or batch ++ [lastBatchRecords],
               -- in both cases finalLastLSN should be lastLSN
               _  -> lastLSN
          void $ S.ckpReaderSetTimeout subLdCkpReader 0
          S.ckpReaderStartReading subLdCkpReader subLogId (finalLastLSN + 1) S.LSN_MAX
          writeChan ch . DATAREADYEVENT $ NormalData newGroups

initCtx :: ServerContext -> StreamingFetchRequest -> IO SubscriptionCtx
initCtx ServerContext{..} StreamingFetchRequest{..} = do
  P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
    Nothing -> do
      Log.debug $ "streamingFetch error because subscription " <> Log.buildText streamingFetchRequestSubscriptionId <> " not exist."
      throwIO $ SubscriptionIdNotFound streamingFetchRequestSubscriptionId
    Just Subscription {..} -> do
      -- create a ldCkpReader for reading new records
      let readerName = textToCBytes (streamingFetchRequestSubscriptionId <> "-" <> streamingFetchRequestOrderingKey)
      ldCkpReader <-
        S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 1 Nothing 10
      -- seek ldCkpReader to start offset
      let streamID = S.mkStreamId S.StreamTypeStream (textToCBytes subscriptionStreamName)
      let key = orderingKeyToStoreKey streamingFetchRequestOrderingKey
      logId <- S.getUnderlyingLogId scLDClient streamID key
      S.startReadingFromCheckpointOrStart ldCkpReader logId (Just S.LSN_MIN) S.LSN_MAX
      -- set ldCkpReader timeout to 0
      _ <- S.ckpReaderSetTimeout ldCkpReader 0
      -- create a ldReader for rereading unacked records
      ldReader <- S.newLDReader scLDClient 1 Nothing
      Log.debug $ "created ldCkpReader and ldReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"
               <> ", partition {" <> Log.buildText streamingFetchRequestOrderingKey <> "}"
      return SubscriptionCtx
        { subscriptionId       = streamingFetchRequestSubscriptionId
        , subConsumerName      = streamingFetchRequestConsumerName
        , subKey               = streamingFetchRequestOrderingKey
        , subStreamName        = subscriptionStreamName
        , subLogId             = logId
        , subAckTimeoutSeconds = subscriptionAckTimeoutSeconds * 10
        , subLdCkpReader       = ldCkpReader
        , subLdReader          = ldReader
        }

readRetransRecords
  :: SubscriptionCtx
  -> V.Vector RecordId
  -> Chan (V.Vector ReceivedRecord) -- ^ responseCh
  -> Map.Map Word64 Word32          -- ^ batchNumMap
  -> IO ()
readRetransRecords ctx@SubscriptionCtx{..} unackedRecordIds responseCh batchNumMap = do
  Log.info $ logSubscriptionCtx ctx <> Log.buildInt (V.length unackedRecordIds) <> " records need to be resend"
  cache <- newIORef Map.empty
  lastResendLSN <- newIORef 0
  V.forM_ unackedRecordIds $ \RecordId {..} -> do
    dataRecords <- getDataRecords subLdReader subLogId cache lastResendLSN recordIdBatchId
    if null dataRecords
      then do
        -- TODO: retry or error
        Log.fatal $ logSubscriptionCtx ctx <> "can't read log " <> Log.buildString (show subLogId)
                 <> " at " <> Log.buildString (show recordIdBatchId)
      else do
        let rr = mkReceivedRecord (fromIntegral recordIdBatchIndex) (dataRecords !! fromIntegral recordIdBatchIndex)
        writeChan responseCh $ V.singleton rr
  where
    getDataRecords ldreader logId cache lastResendLSN recordIdBatchId = do
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

timer :: Chan FetchEvent -> IO ()
timer ch = forever $ do
  tv <- registerDelay 1000000
  atomically $ readTVar tv >>= check
  writeChan ch ACKTIMEOUTEVENT

--------------------------------------------------------------------------------

fetchResult :: [[S.DataRecord Bytes]] -> V.Vector ReceivedRecord
fetchResult groups = V.fromList $ concatMap (zipWith mkReceivedRecord [0 ..]) groups

commitCheckPoint :: S.C_LogID -> S.LDSyncCkpReader -> RecordId -> IO ()
commitCheckPoint logId reader RecordId {..} = do
  S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
mkReceivedRecord index record =
  let recordId = RecordId (S.recordLSN record) (fromIntegral index)
   in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

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

filterUnackedRecordIds :: V.Vector RecordId -> Map.Map RecordId RecordIdRange -> RecordId -> V.Vector RecordId
filterUnackedRecordIds recordIds ackedRanges windowLowerBound =
  flip V.filter recordIds $ \recordId ->
    (recordId >= windowLowerBound)
      && case Map.lookupLE recordId ackedRanges of
        Nothing                               -> True
        Just (_, RecordIdRange _ endRecordId) -> recordId > endRecordId

doAck
  :: SubscriptionCtx
  -> IORef SliceWindow
  -> V.Vector RecordId
  -> IO ()
doAck ctx@SubscriptionCtx{..} windowRef ackRecordIds = do
  window@SliceWindow{..} <- readIORef windowRef
  let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b windowLowerBound a batchNumMap) ackedRanges ackRecordIds
  let commitLSN = getCommitRecordId newAckedRanges batchNumMap

  case tryUpdateWindowLowerBound newAckedRanges windowLowerBound batchNumMap commitLSN of
    Just (ranges, newLowerBound) -> do
      Log.info $ logSubscriptionCtx ctx
              <> "update window lower bound, from {"
              <> Log.buildString (show windowLowerBound)
              <> "} to {"
              <> Log.buildString (show newLowerBound)
              <> "}"

      commitCheckPoint subLogId subLdCkpReader (fromJust commitLSN)
      Log.info $ logSubscriptionCtx ctx <> "commit checkpoint = " <> Log.buildString (show . fromJust $ commitLSN)
      Log.debug $ logSubscriptionCtx ctx
               <> "after commitCheckPoint, length of ackedRanges is: " <> Log.buildInt (Map.size ranges)
               <> ", 10 smallest ackedRanges: " <> Log.buildString (printAckedRanges $ Map.take 10 ranges)

      -- after a checkpoint is committed, informations of records less then and equal to checkpoint are no need to be retained, so just clear them
      let newBatchNumMap = updateBatchNumMap (fromJust commitLSN) batchNumMap
      Log.debug $ logSubscriptionCtx ctx
               <> "update batchNumMap, 10 smallest batchNumMap: " <> Log.buildString (show $ Map.take 10 newBatchNumMap)
      writeIORef windowRef $ window {ackedRanges = ranges, windowLowerBound = newLowerBound, batchNumMap = newBatchNumMap}
    Nothing ->
      writeIORef windowRef $ window {ackedRanges = newAckedRanges}
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
