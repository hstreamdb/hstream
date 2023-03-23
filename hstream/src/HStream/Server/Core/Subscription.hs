{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE MultiWayIf       #-}
{-# LANGUAGE TupleSections    #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}

module HStream.Server.Core.Subscription where

import           Control.Concurrent
import           Control.Concurrent.Async      (async, wait, withAsync)
import           Control.Concurrent.STM
import           Control.Exception             (catch, finally, handle,
                                                onException, throwIO)
import           Control.Monad
import qualified Data.ByteString               as BS
import           Data.Foldable                 (foldl')
import           Data.Functor                  ((<&>))
import qualified Data.HashMap.Strict           as HM
import           Data.IORef                    (newIORef, readIORef, writeIORef)
import           Data.Kind                     (Type)
import qualified Data.List                     as L
import qualified Data.Map.Strict               as Map
import           Data.Maybe                    (fromJust, fromMaybe, isNothing)
import qualified Data.Set                      as Set
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import qualified Data.Vector                   as V
import           Data.Word                     (Word32, Word64)
import           GHC.Stack                     (HasCallStack)
import           Network.GRPC.HighLevel        (StreamRecv, StreamSend)
import           Proto3.Suite                  (Enumerated (Enumerated), def)

import qualified HStream.Exception             as HE
import qualified HStream.Logger                as Log
import qualified HStream.MetaStore.Types       as M
import           HStream.Server.ConnectorTypes (getCurrentTimestamp)
import           HStream.Server.Core.Common    as CC (decodeRecordBatch,
                                                      getCommitRecordId,
                                                      getSuccessor,
                                                      insertAckedRecordId,
                                                      listSubscriptions)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import qualified HStream.Stats                 as Stats
import qualified HStream.Store                 as S
import           HStream.Utils                 (decompressBatchedRecord,
                                                getProtoTimestamp,
                                                mkBatchedRecord, textToCBytes)

--------------------------------------------------------------------------------

listSubscriptions :: ServerContext -> IO (V.Vector Subscription)
listSubscriptions sc = CC.listSubscriptions sc Nothing

listSubscriptionsWithPrefix :: ServerContext -> Text -> IO (V.Vector Subscription)
listSubscriptionsWithPrefix sc prefix = V.filter (T.isPrefixOf prefix . subscriptionSubscriptionId) <$> CC.listSubscriptions sc Nothing

listConsumers :: ServerContext -> ListConsumersRequest -> IO ListConsumersResponse
listConsumers sc@ServerContext{..} ListConsumersRequest{listConsumersRequestSubscriptionId = sid} = do
  subCtxMap <- readTVarIO scSubscribeContexts
  case HM.lookup sid subCtxMap of
    Nothing -> throwIO $ HE.SubscriptionNotFound sid
    Just subCtx@SubscribeContextNewWrapper{..} -> atomically $ do
      ctx@SubscribeContext{..} <- readTMVar scnwContext
      consumerMap <- readTVar subConsumerContexts
      return . ListConsumersResponse . V.fromList $ makeRpcConsumer <$> HM.elems consumerMap
  where
    -- FIXME: Set "" to Server uri, because these are consumers created by queries
    makeRpcConsumer ConsumerContext{..} = def {consumerName = ccConsumerName, consumerUri = fromMaybe "" ccConsumerUri, consumerUserAgent = fromMaybe "" ccConsumerAgent}

getSubscription :: ServerContext -> GetSubscriptionRequest -> IO GetSubscriptionResponse
getSubscription ServerContext{ ..} GetSubscriptionRequest{ getSubscriptionRequestId = subId} = do
  shardIds <- fmap HM.keys $ atomically $
    readTVar scSubscribeContexts
    >>= maybe (return mempty) (readTMVar . scnwContext
                           >=> readTVar . subShardContexts)
     .  HM.lookup subId
  offsets <- forM shardIds (\x -> do
    lsn <- handle (\(e::S.SomeHStoreException) -> return 0) $
      S.ckpStoreGetLSN scCkpStore (textToCBytes subId) x
    return $ SubscriptionOffset {
      subscriptionOffsetShardId = x,
      subscriptionOffsetBatchId = lsn
      })
  mMeta <- M.getMeta subId metaHandle
  case mMeta of
   Nothing  -> throwIO $ HE.SubscriptionNotFound subId
   Just sub -> pure $ GetSubscriptionResponse {
    getSubscriptionResponseSubscription = Just $ originSub sub,
    getSubscriptionResponseOffsets = V.fromList offsets}

createSubscription :: HasCallStack => ServerContext -> Subscription -> IO Subscription
createSubscription ServerContext {..} sub@Subscription{..} = do
  let streamName = transToStreamName subscriptionStreamName
  streamExists <- S.doesStreamExist scLDClient streamName
  unless streamExists $ do
    Log.debug $ "Try to create a subscription to a nonexistent stream. Stream Name: "
              <> Log.buildString' streamName
    throwIO $ HE.EmptyStream subscriptionStreamName
  shards <- getShards scLDClient subscriptionStreamName
  startOffsets <- case subscriptionOffset of
    (Enumerated (Right SpecialOffsetEARLIEST)) -> return $ foldl' (\acc logId -> HM.insert logId S.LSN_MIN acc) HM.empty shards
    (Enumerated (Right SpecialOffsetLATEST))   -> foldM gatherTailLSN HM.empty shards
    _                                          -> throwIO HE.InvalidSubscriptionOffset

  createTime <- getProtoTimestamp
  let newSub = sub {subscriptionCreationTime = Just createTime}
  let subWrap = SubscriptionWrap
        { originSub  = newSub
        , subOffsets = startOffsets
        }
  -- FIXME: SubscriptionExists
  subExists <- M.checkMetaExists @SubscriptionWrap subscriptionSubscriptionId metaHandle
  when subExists $ throwIO (HE.SubscriptionExists subscriptionSubscriptionId)
  M.insertMeta subscriptionSubscriptionId subWrap metaHandle
  return newSub
 where
   gatherTailLSN acc shard = do
     lsn <- (+1) <$> S.getTailLSN scLDClient shard
     return $ HM.insert shard lsn acc

deleteSubscription :: ServerContext -> DeleteSubscriptionRequest -> IO ()
deleteSubscription ServerContext{..} DeleteSubscriptionRequest { deleteSubscriptionRequestSubscriptionId = subId, deleteSubscriptionRequestForce = force} = do
  subscription <- M.getMeta @SubscriptionWrap subId metaHandle
  when (isNothing subscription) $ throwIO (HE.SubscriptionNotFound subId)

  (status, msub) <- atomically $ do
    res <- getSubState
    case res of
      Nothing -> pure (NotExist, Nothing)
      Just (subCtx, stateVar) -> do
        state <- readTVar stateVar
        case state of
          SubscribeStateNew -> retry
          SubscribeStateRunning -> do
            isActive <- hasValidConsumers subCtx
            if isActive
            then if force
                 then do
                   writeTVar stateVar SubscribeStateStopping
                   pure (CanDelete, Just (subCtx, stateVar))
                 else pure (CanNotDelete, Just (subCtx, stateVar))
            else do
              writeTVar stateVar SubscribeStateStopping
              pure (CanDelete, Just (subCtx, stateVar))
          SubscribeStateStopping -> pure (Signaled, Just (subCtx, stateVar))
          SubscribeStateStopped  -> pure (Signaled, Just (subCtx, stateVar))
  Log.debug $ "Subscription deletion has state " <> Log.buildString' status
  case status of
    NotExist  -> doRemove
    CanDelete -> do
      let (subCtx@SubscribeContext{..}, subState) = fromJust msub
      atomically $ waitingStopped subCtx subState
      Log.info "Subscription stopped, start deleting "
      atomically removeSubFromCtx
      S.removeAllCheckpoints subLdCkpReader
      doRemove
    CanNotDelete -> throwIO $ HE.FoundActiveConsumers "Subscription still has active consumers"
    Signaled     -> throwIO $ HE.SubscriptionIsDeleting "Subscription is being deleted, please wait a while"
  where
    -- FIXME: Concurrency Issue
    doRemove :: IO ()
    doRemove = M.deleteMeta @SubscriptionWrap subId Nothing metaHandle

    getSubState :: STM (Maybe (SubscribeContext, TVar SubscribeState))
    getSubState = do
      scs <- readTVar scSubscribeContexts
      case HM.lookup subId scs of
        Nothing -> return Nothing
        Just SubscribeContextNewWrapper {..}  -> do
          subState <- readTVar scnwState
          case subState of
            SubscribeStateNew -> retry
            _ -> do
              subCtx <- readTMVar scnwContext
              return $ Just (subCtx, scnwState)

    hasValidConsumers :: SubscribeContext -> STM Bool
    hasValidConsumers SubscribeContext {..} = do
      consumers <- readTVar subConsumerContexts
      pure $ not $ HM.null consumers

    waitingStopped :: SubscribeContext -> TVar SubscribeState -> STM ()
    waitingStopped SubscribeContext {..} subState = do
      consumers <- readTVar subConsumerContexts
      if HM.null consumers
      then pure()
      else retry
      writeTVar subState SubscribeStateStopped

    removeSubFromCtx :: STM ()
    removeSubFromCtx =  do
      scs <- readTVar scSubscribeContexts
      writeTVar scSubscribeContexts (HM.delete subId scs)

data DeleteSubStatus = NotExist | CanDelete | CanNotDelete | Signaled
  deriving (Show)

--------------------------------------------------------------------------------
-- streaming fetch

data FetchCoreMode = FetchCoreInteractive
                   | FetchCoreDirect
data SFetchCoreMode (mode :: FetchCoreMode) where
  SFetchCoreInteractive :: SFetchCoreMode 'FetchCoreInteractive
  SFetchCoreDirect      :: SFetchCoreMode 'FetchCoreDirect

type family FetchCoreType (mode :: FetchCoreMode) (sendTyp :: Type) (recvTyp :: Type)
type instance FetchCoreType 'FetchCoreInteractive a b
-- TODO: use a datatype instead of tuple
  = (StreamSend a, StreamRecv b, Text, Text) -> IO ()
type instance FetchCoreType 'FetchCoreDirect a b
  = StreamingFetchRequest -> (Maybe ReceivedRecord -> IO (IO (), IO ())) -> IO ()

streamingFetchCore :: ServerContext
                   -> SFetchCoreMode mode
                   -> FetchCoreType mode StreamingFetchResponse StreamingFetchRequest
streamingFetchCore ctx SFetchCoreDirect = \initReq callbacksGen -> do
  mockAckPool <- newTChanIO
  Stats.subscription_time_series_add_request_messages (scStatsHolder ctx) (textToCBytes (streamingFetchRequestSubscriptionId initReq)) 1
  Stats.subscription_stat_add_request_messages (scStatsHolder ctx) (textToCBytes (streamingFetchRequestSubscriptionId initReq)) 1
  Log.debug "pass first recv"
  (SubscribeContextWrapper {..}, tid_m) <- initSub ctx (streamingFetchRequestSubscriptionId initReq)
  Log.debug "pass initSub"
  let streamSend resp = do
        let req = StreamingFetchRequest
                  { streamingFetchRequestSubscriptionId = streamingFetchRequestSubscriptionId initReq
                  , streamingFetchRequestConsumerName = streamingFetchRequestConsumerName initReq
                  , streamingFetchRequestAckIds = maybe V.empty receivedRecordRecordIds $ streamingFetchResponseReceivedRecords resp
                  }
        (callback,beforeAck) <- callbacksGen (streamingFetchResponseReceivedRecords resp)
        callback
        async (beforeAck >> (atomically $ writeTChan mockAckPool req))
        return $ Right ()
  consumerCtx <- initConsumer scwContext (streamingFetchRequestConsumerName initReq) Nothing Nothing streamSend
  Log.debug "pass initConsumer"
  let streamRecv = do
        req <- atomically $ readTChan mockAckPool
        return $ Right (Just req)
  withAsync (recvAcks ctx scwState scwContext consumerCtx streamRecv) wait `onException` do
    case tid_m of
      Just tid -> killThread tid
      Nothing  -> return ()
  Log.debug "pass recvAcks"
streamingFetchCore ctx SFetchCoreInteractive = \(streamSend, streamRecv, requestUri, userAgent) -> do
  StreamingFetchRequest {..} <- firstRecv streamRecv
  Log.debug "pass first recv"
  (SubscribeContextWrapper {..}, _) <- initSub ctx streamingFetchRequestSubscriptionId
  Log.debug "pass initSub"
  consumerCtx <- initConsumer scwContext streamingFetchRequestConsumerName (Just requestUri) (Just userAgent) streamSend
  Log.debug "pass initConsumer"
  async (recvAcks ctx scwState scwContext consumerCtx streamRecv) >>= wait
  Log.debug "pass recvAcks"
  where
    firstRecv :: StreamRecv StreamingFetchRequest -> IO StreamingFetchRequest
    firstRecv streamRecv =
      streamRecv >>= \case
        Right (Just firstReq) -> do
          Stats.subscription_time_series_add_request_messages (scStatsHolder ctx) (textToCBytes (streamingFetchRequestSubscriptionId firstReq)) 1
          Stats.subscription_stat_add_request_messages (scStatsHolder ctx) (textToCBytes (streamingFetchRequestSubscriptionId firstReq)) 1
          return firstReq
        Left _        -> throwIO $ HE.StreamReadError "Consumer recv error"
        Right Nothing -> throwIO $ HE.StreamReadClose "Consumer is closed"

initSub :: ServerContext -> SubscriptionId -> IO (SubscribeContextWrapper, Maybe ThreadId)
initSub serverCtx@ServerContext {..} subId = M.getMeta subId metaHandle >>= \case
  Nothing -> do
    Log.fatal $ "subscription " <> Log.build subId <> " not exist."
    throwIO $ HE.SubscriptionNotFound subId
  Just SubscriptionWrap{} -> do
    (needInit, SubscribeContextNewWrapper {..}) <- atomically $ do
      subMap <- readTVar scSubscribeContexts
      case HM.lookup subId subMap of
        Nothing -> do
          wrapper <- SubscribeContextNewWrapper <$> newTVar SubscribeStateNew <*> newEmptyTMVar
          let newSubMap = HM.insert subId wrapper subMap
          writeTVar scSubscribeContexts newSubMap
          return (True, wrapper)
        Just wrapper@SubscribeContextNewWrapper {..} -> do
          readTVar scnwState >>= \case
            SubscribeStateNew     -> retry
            SubscribeStateRunning -> return (False, wrapper)
            _                     -> throwSTM $ HE.SubscriptionInvalidError "Invalid Subscription"
    if needInit
      then do
        subCtx <- doSubInit serverCtx subId
        wrapper@SubscribeContextWrapper{..} <- atomically $ do
          putTMVar scnwContext subCtx
          writeTVar scnwState SubscribeStateRunning
          return SubscribeContextWrapper {scwState = scnwState, scwContext = subCtx}
        tid <- myThreadId
        let errHandler = \case
              Left e  -> throwTo tid e
              Right _ -> pure ()
        tid <- forkFinally (sendRecords serverCtx scwState scwContext) errHandler
        return (wrapper, Just tid)
      else do
        mctx <- atomically $ readTMVar scnwContext
        return (SubscribeContextWrapper {scwState = scnwState, scwContext = mctx}, Nothing)

-- For each subscriptionId, create ldCkpReader and ldReader, then
-- add all shards of target stream to SubscribeContext
doSubInit :: ServerContext -> SubscriptionId -> IO SubscribeContext
doSubInit ServerContext{..} subId = do
  M.getMeta subId metaHandle >>= \case
    Nothing -> do
      Log.fatal $ "unexpected error: subscription " <> Log.build subId <> " not exist."
      throwIO $ HE.SubscriptionNotFound subId
    Just SubscriptionWrap {originSub=oSub@Subscription{..}, ..} -> do
      Log.debug $ "get subscriptionInfo from persistence: \n"
               <> "subscription = " <> Log.buildString' (show oSub) <> "\n"
               <> "startOffsets = " <> Log.buildString' (show subOffsets)
      let readerName = textToCBytes subId
      -- Notice: doc this. shard count can not larger than this.
      let maxReadLogs = 1000
      -- see: https://logdevice.io/api/classfacebook_1_1logdevice_1_1_client.html#a797d6ebcb95ace4b95a198a293215103
      let ldReaderBufferSize = 10
      -- create a ldCkpReader for reading new records
      ldCkpReader <-
        S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 maxReadLogs (Just ldReaderBufferSize)
      S.ckpReaderSetTimeout ldCkpReader 10  -- 10 milliseconds
      -- create a ldReader for rereading unacked records
      reader <- S.newLDReader scLDClient maxReadLogs (Just ldReaderBufferSize)
      S.readerSetTimeout reader 10 -- 10 milliseconds
      ldReader <- newMVar reader
      Log.debug $ "created a ldReader for subscription {" <> Log.build subId <> "}"

      unackedRecords <- newTVarIO 0
      consumerContexts <- newTVarIO HM.empty
      shardContexts <- newTVarIO HM.empty
      assignment <- mkEmptyAssignment
      curTime <- getCurrentTimestamp >>= (newTVarIO . fromIntegral)
      checkList <- newTVarIO []
      checkListIndex <- newTVarIO Map.empty
      let emptySubCtx =
            SubscribeContext
              { subSubscriptionId = subId,
                subStreamName = subscriptionStreamName ,
                subAckTimeoutSeconds = subscriptionAckTimeoutSeconds,
                subMaxUnackedRecords = fromIntegral subscriptionMaxUnackedRecords,
                subLdCkpReader = ldCkpReader,
                subLdReader = ldReader,
                subUnackedRecords = unackedRecords,
                subConsumerContexts = consumerContexts,
                subShardContexts = shardContexts,
                subAssignment = assignment,
                subCurrentTime = curTime,
                subWaitingCheckedRecordIds = checkList,
                subWaitingCheckedRecordIdsIndex = checkListIndex,
                subStartOffsets = subOffsets,
                subStartOffset = subscriptionOffset
              }
      addNewShardsToSubCtx emptySubCtx (HM.toList subOffsets)
      return emptySubCtx
  where
    mkEmptyAssignment :: IO Assignment
    mkEmptyAssignment = do
      total <- newTVarIO Set.empty
      unassign <- newTVarIO []
      waitingRead <- newTVarIO []
      waitingReassign <- newTVarIO []
      wc <- newTVarIO []
      s2c <- newTVarIO HM.empty
      c2s <- newTVarIO HM.empty
      cws <- newTVarIO Set.empty

      return
        Assignment
          { totalShards = total,
            unassignedShards = unassign,
            waitingReadShards = waitingRead,
            waitingReassignedShards = waitingReassign,
            waitingConsumers = wc,
            shard2Consumer = s2c,
            consumer2Shards = c2s,
            consumerWorkloads = cws
          }


-- get all partitions of the specified stream
getShards :: S.LDClient -> T.Text -> IO [S.C_LogID]
getShards client streamName = do
  Map.elems <$> S.listStreamPartitions client (transToStreamName streamName)

addNewShardsToSubCtx :: SubscribeContext -> [(S.C_LogID, S.LSN)] -> IO ()
addNewShardsToSubCtx SubscribeContext {subAssignment = Assignment{..}, ..} shards = atomically $ do
  oldTotal <- readTVar totalShards
  oldUnassign <- readTVar unassignedShards
  oldShardCtxs <- readTVar subShardContexts
  (newTotal, newUnassign, newShardCtxs)
    <- foldM addShards (oldTotal, oldUnassign, oldShardCtxs) shards
  writeTVar totalShards newTotal
  writeTVar unassignedShards newUnassign
  writeTVar subShardContexts newShardCtxs
  where
    addShards old@(total, unassign, ctx) (logId, lsn)
      | Set.member logId total = return old
      | otherwise = do
          lowerBound <- newTVar $ ShardRecordId lsn 0
          upperBound <- newTVar maxBound
          range <- newTVar Map.empty
          batchMp <- newTVar Map.empty
          let ackWindow = AckWindow
                { awWindowLowerBound = lowerBound,
                  awWindowUpperBound = upperBound,
                  awAckedRanges = range,
                  awBatchNumMap = batchMp
                }
              subShardCtx = SubscribeShardContext {sscAckWindow = ackWindow, sscLogId = logId}
          return (Set.insert logId total, unassign ++ [logId], HM.insert logId subShardCtx ctx)

-- Add consumer and sender to the waitlist and consumerCtx
initConsumer
  :: SubscribeContext -> ConsumerName -> Maybe Text -> Maybe Text -> StreamSend StreamingFetchResponse
  -> IO ConsumerContext
initConsumer SubscribeContext {subAssignment = Assignment{..}, ..} consumerName uri agent streamSend = do
  sender <- newMVar streamSend
  atomically $ do
    cMap <- readTVar subConsumerContexts
    when (HM.member consumerName cMap) $ throwSTM (HE.ConsumerExists $ T.unpack consumerName)
    modifyTVar' waitingConsumers (\consumers -> consumers ++ [consumerName])

    isValid <- newTVar True
    let cc = ConsumerContext
              { ccConsumerName = consumerName,
                ccConsumerUri = uri,
                ccConsumerAgent = agent,
                ccIsValid = isValid,
                ccStreamSend = sender
              }
    writeTVar subConsumerContexts (HM.insert consumerName cc cMap)
    return cc

sendRecords :: ServerContext -> TVar SubscribeState -> SubscribeContext -> IO ()
sendRecords ServerContext{..} subState subCtx@SubscribeContext {..} = do
  threadDelay 10000
  isFirstSendRef <- newIORef True
  loop isFirstSendRef
  where
    loop isFirstSendRef = do
      -- Log.debug "enter sendRecords loop"
      state <- readTVarIO subState
      if state == SubscribeStateRunning
        then do
          newShards <- getNewShards
          unless (L.null newShards) $ do
            addNewShardsToSubCtx subCtx newShards
            Log.info $ "add shards " <> Log.buildString (show newShards)
                    <> " to consumer " <> Log.build subSubscriptionId
          atomically $ do
            checkAvailable subShardContexts
            checkAvailable subConsumerContexts
          atomically $ do
            assignWaitingConsumers subAssignment

          addRead subLdCkpReader subAssignment subStartOffsets
          atomically checkUnackedRecords
          recordBatches <- readRecordBatches
          receivedRecordsVecs <- forM recordBatches decodeRecordBatch
          isFirstSend <- readIORef isFirstSendRef
          if isFirstSend
          then do
            writeIORef isFirstSendRef False
            tid1 <- forkIO . forever $ do
                     threadDelay (100 * 1000)
                     updateClockAndDoResend

            tid2 <- forkIO . forever $ do
                      threadDelay (100 * 1000)
                      atomically $ assignShards subAssignment

            -- FIXME: the same code
            successSendRecords <- sendReceivedRecordsVecs receivedRecordsVecs
            atomically $ addUnackedRecords subCtx successSendRecords
            -- Note: automically kill child threads when the parent thread is killed
            loop isFirstSendRef `onException` (killThread tid1 >> killThread tid2)
          else do
            -- FIXME: the same code
            successSendRecords <- sendReceivedRecordsVecs receivedRecordsVecs
            atomically $ addUnackedRecords subCtx successSendRecords
            loop isFirstSendRef
        else
          when (state == SubscribeStateStopping) $ do
            throwIO $ HE.SubscriptionInvalidError "Invalid Subscription"

    updateClockAndDoResend :: IO ()
    updateClockAndDoResend = do
      -- Note: non-strict behaviour in STM!
      --       Please refer to https://github.com/haskell/stm/issues/30
      newTime <- getCurrentTimestamp <&> fromIntegral
      doneList <- atomically $ do
        ct <- readTVar subCurrentTime
        checkList <- readTVar subWaitingCheckedRecordIds
        let (!doneList, !leftList) = span ( \CheckedRecordIds {..} -> crDeadline <= newTime) checkList
        writeTVar subCurrentTime newTime
        writeTVar subWaitingCheckedRecordIds leftList
        return doneList
      forM_ doneList (\r@CheckedRecordIds {..} -> buildShardRecordIds r >>= resendTimeoutRecords crLogId crBatchId )
      where
        buildShardRecordIds  CheckedRecordIds {..} = atomically $ do
          batchIndexes <- readTVar crBatchIndexes
          pure $ V.fromList $ fmap (\i -> ShardRecordId {sriBatchId = crBatchId, sriBatchIndex= i}) (Set.toList batchIndexes)

    checkAvailable :: TVar (HM.HashMap k v) -> STM()
    checkAvailable tv = readTVar tv >>= check . not . HM.null

    checkUnackedRecords :: STM ()
    checkUnackedRecords = do
      unackedRecords <- readTVar subUnackedRecords
      if unackedRecords >= subMaxUnackedRecords
      then do
        -- traceM $ "block on unackedrecords: " <> show unackedRecords
        retry
      else pure ()

    getNewShards :: IO [(S.C_LogID, S.LSN)]
    getNewShards = do
      shards <- catch (getShards scLDClient subStreamName) (\(_::S.NOTFOUND)-> pure mempty)
      if L.null shards then return [] else do
        shardSet <- readTVarIO (totalShards subAssignment)
        let newShards = filter (not . flip Set.member shardSet) shards
        case subStartOffset of
          Enumerated (Right SpecialOffsetEARLIEST) ->
            return $ (, S.LSN_MIN) <$> newShards
          Enumerated (Right SpecialOffsetLATEST)   ->
            mapM (\x -> S.getTailLSN scLDClient x <&> (x,) . (+1)) newShards
          _                                        ->
            throwIO $ HE.InvalidSubscriptionOffset

    addRead :: S.LDSyncCkpReader -> Assignment -> HM.HashMap S.C_LogID S.LSN -> IO ()
    addRead ldCkpReader Assignment {..} startOffsets = do
      shards <- atomically $ swapTVar waitingReadShards []
      forM_ shards $ \shard -> do
        offset <- case HM.lookup shard startOffsets of
          Nothing -> do
            Log.fatal $ "can't find startOffsets for shard "
                     <> Log.build shard
                     <> ", startOffsets="
                     <> Log.buildString' (show startOffsets)
            throwIO . HE.UnexpectedError $ "can't find startOffsets for shard " <> show shard
          Just s -> return s
        Log.debug $ "Start reading " <> Log.buildString' shard <> " from "
                 <> Log.buildString' offset
        S.startReadingFromCheckpointOrStart ldCkpReader shard (Just offset) S.LSN_MAX

    readRecordBatches :: IO [S.DataRecord BS.ByteString]
    readRecordBatches = do
      S.ckpReaderReadAllowGap subLdCkpReader 100 >>= \case
        Left gap@S.GapRecord{..} -> do
          Log.debug $ "reader meet gap: " <> Log.buildString (show gap)
          atomically $ do
            scs <- readTVar subShardContexts
            let SubscribeShardContext {sscAckWindow=AckWindow{..}} = scs HM.! gapLogID
            ranges <- readTVar awAckedRanges
            batchNumMap <- readTVar awBatchNumMap
            -- insert gap range to ackedRanges
            let gapLoRecordId = ShardRecordId gapLoLSN minBound
                gapHiRecordId = ShardRecordId gapHiLSN maxBound
                newRanges = Map.insert gapLoRecordId (ShardRecordIdRange gapLoRecordId gapHiRecordId) ranges
                -- also need to insert lo_lsn record and hi_lsn record to batchNumMap
                -- because we need to use these info in `tryUpdateWindowLowerBound` function later.
                groupNums = map (, 0) [gapLoLSN, gapHiLSN]
                newBatchNumMap = Map.union batchNumMap (Map.fromList groupNums)
            writeTVar awAckedRanges newRanges
            writeTVar awBatchNumMap newBatchNumMap
          return []
        Right dataRecords -> return dataRecords

    sendReceivedRecordsVecs :: [(S.C_LogID, Word64, V.Vector ShardRecordId, ReceivedRecord)] -> IO Int
    sendReceivedRecordsVecs vecs = do
      (_, successRecords) <- foldM
        (
          \ (skipSet, successRecords) (logId, batchId, shardRecordIdVec, vec)->
            if Set.member logId skipSet
            then return (skipSet, successRecords)
            else do
              ok <- sendReceivedRecords logId batchId shardRecordIdVec vec False
              if ok
              then return (skipSet, successRecords + V.length shardRecordIdVec)
              else return (Set.insert logId skipSet, successRecords)
        )
        (Set.empty, 0)
        vecs
      pure successRecords

    sendReceivedRecords :: S.C_LogID -> Word64 -> V.Vector ShardRecordId -> ReceivedRecord -> Bool -> IO Bool
    sendReceivedRecords logId batchId shardRecordIds records@ReceivedRecord{..} isResent = do
      let Assignment {..} = subAssignment
      -- if current send is not a resent, insert record related info into AckWindow
      mres <- atomically $ do
        if not isResent
        then do
          scs <- readTVar subShardContexts
          let SubscribeShardContext {sscAckWindow = AckWindow{..}} = scs HM.! logId
          batchNumMap <- readTVar awBatchNumMap
          let newBatchNumMap = Map.insert batchId (fromIntegral $ V.length shardRecordIds) batchNumMap
          writeTVar awBatchNumMap newBatchNumMap
        else pure ()

        s2c <- readTVar shard2Consumer
        case HM.lookup logId s2c of
          Nothing -> return Nothing
          Just consumer -> do
            ccs <- readTVar subConsumerContexts
            case HM.lookup consumer ccs of
              Nothing -> return Nothing
              Just ConsumerContext {..} -> do
                iv <- readTVar ccIsValid
                if iv
                then return $ Just (ccConsumerName, ccStreamSend)
                else return Nothing

      deliveryRes <- recordsDelivery mres
      let subId = textToCBytes subSubscriptionId
          recordSize = fromIntegral . V.length $ shardRecordIds
          byteSize = fromIntegral $ maybe 0 (BS.length . batchedRecordPayload) receivedRecordRecord
      if deliveryRes
        then do
          Stats.subscription_time_series_add_response_messages scStatsHolder subId 1
          Stats.subscription_stat_add_response_messages scStatsHolder subId 1
          if isResent
             then do
               Stats.subscription_stat_add_resend_records scStatsHolder subId (fromIntegral $ V.length shardRecordIds)
             else do
               Stats.subscription_stat_add_send_out_bytes scStatsHolder subId byteSize
               Stats.subscription_stat_add_send_out_records scStatsHolder subId recordSize
               Stats.subscription_time_series_add_send_out_bytes scStatsHolder subId byteSize
               Stats.subscription_time_series_add_send_out_records scStatsHolder subId recordSize
        else do
          if isResent
            then Stats.subscription_stat_add_resend_records_failed scStatsHolder subId recordSize
            else Stats.subscription_stat_add_send_out_records_failed scStatsHolder subId recordSize

      return deliveryRes
     where
       recordsDelivery :: Maybe (ConsumerName, MVar (StreamSend StreamingFetchResponse)) -> IO Bool
       recordsDelivery Nothing = do
         if isResent
         then registerResend logId batchId shardRecordIds
         else resetReadingOffset logId batchId
         return False
       recordsDelivery (Just(consumerName, streamSend)) = do
         withMVar streamSend (\ss -> ss (StreamingFetchResponse $ Just records)) >>= \case
           Left err -> do
             Log.fatal $ "sendReceivedRecords failed: logId=" <> Log.build logId <> ", batchId=" <> Log.build batchId
                      <> ", num of records=" <> Log.build (V.length shardRecordIds) <> "\n"
                      <> "will remove the consumer " <> Log.build consumerName <> ": " <> Log.buildString (show err)
             atomically $ invalidConsumer subCtx consumerName
             if isResent
             then registerResend logId batchId shardRecordIds
             else resetReadingOffset logId batchId
             return False
           Right _ -> do
             Log.debug $ "send records from " <> Log.build logId <> " to consumer " <> Log.build consumerName
                      <> ", batchId=" <> Log.build batchId <> ", num of records=" <> Log.build (V.length shardRecordIds)
             registerResend logId batchId shardRecordIds
             return True

    registerResend logId batchId recordIds = atomically $ do
      batchIndexes <- newTVar $ Set.fromList $ fmap sriBatchIndex (V.toList recordIds)
      currentTime <- readTVar subCurrentTime
      checkList <- readTVar subWaitingCheckedRecordIds
      checkListIndex <- readTVar subWaitingCheckedRecordIdsIndex
      let checkedRecordIds = CheckedRecordIds {
                              crDeadline =  currentTime + fromIntegral (subAckTimeoutSeconds * 1000),
                              crLogId = logId,
                              crBatchId = batchId,
                              crBatchIndexes = batchIndexes
                            }
      let checkedRecordIdsKey = CheckedRecordIdsKey {
                              crkLogId = logId,
                              crkBatchId = batchId
                            }
      let newCheckList = checkList ++ [checkedRecordIds]
      let newCheckListIndex = Map.insert checkedRecordIdsKey checkedRecordIds checkListIndex
      writeTVar subWaitingCheckedRecordIds newCheckList
      writeTVar subWaitingCheckedRecordIdsIndex newCheckListIndex

    resendTimeoutRecords logId batchId recordIds = do
      resendRecordIds <- atomically $ do
        scs <- readTVar subShardContexts
        let SubscribeShardContext {sscAckWindow=AckWindow{..}} = scs HM.! logId
        ranges <- readTVar awAckedRanges
        lb <- readTVar awWindowLowerBound
        let res = filterUnackedRecordIds recordIds ranges lb
        -- unless (V.null res) $ do
        --   traceM $ "There are " <> show (V.length res) <> " records need to resent"
        --         <> ", batchId=" <> show batchId
        --   traceM $ "windowLowerBound=" <> show lb
        --   traceM $ "ackedRanges=" <> show ranges
        --   mp <- readTVar awBatchNumMap
        --   traceM $ "batchNumMap=" <> show mp
        return res

      unless (V.null resendRecordIds) $ do
        Log.debug $ "There are " <> Log.build (V.length resendRecordIds) <> " records need to resent"
                 <> ", logId=" <> Log.build logId  <> ", batchId=" <> Log.build batchId
        dataRecord <- withMVar subLdReader $ \reader -> do
          S.readerStartReading reader logId batchId batchId
          readLoop reader 3
        if length dataRecord /= 1
        then do
          Log.fatal $ "read error on `readerRead`. Expect 1 record but got " <> Log.build (length dataRecord)
                   <> ", logId " <> Log.build logId <> ", batchId " <> Log.build batchId
        else do
          (_, _, _, ReceivedRecord{..}) <- decodeRecordBatch $ head dataRecord
          let batchRecords@BatchedRecord{..} = fromJust receivedRecordRecord
              uncompressedRecords = decompressBatchedRecord batchRecords
              (ids, records) =
                V.foldl'
                  (\(rids, r) ShardRecordId {..} ->
                      let newRecords = V.snoc r (uncompressedRecords V.! fromIntegral sriBatchIndex)
                          newIds = V.snoc rids (receivedRecordRecordIds V.! fromIntegral sriBatchIndex)
                        in (newIds, newRecords)
                  )
                  (V.empty, V.empty)
                  resendRecordIds
              resendBatch = mkBatchedRecord batchedRecordCompressionType batchedRecordPublishTime (fromIntegral $ V.length records) records
              resendRecords = ReceivedRecord ids (Just resendBatch)
          void $ sendReceivedRecords logId batchId resendRecordIds resendRecords True
     where
      readLoop reader n
        | n == 0 = return []
        | otherwise = do
          res <- S.readerRead reader 1
          if null res
             then do
               Log.warning $ "reader read got empty result, logId " <> Log.build logId <> ", batchId " <> Log.build batchId <> ", retry."
               readLoop reader (n - 1)
             else return res

    filterUnackedRecordIds recordIds ackedRanges windowLowerBound =
      flip V.filter recordIds $ \recordId ->
        (recordId >= windowLowerBound)
          && case Map.lookupLE recordId ackedRanges of
            Nothing                                    -> True
            Just (_, ShardRecordIdRange _ endRecordId) -> recordId > endRecordId

    resetReadingOffset :: S.C_LogID -> S.LSN -> IO ()
    resetReadingOffset logId startOffset = do
      S.ckpReaderStartReading subLdCkpReader logId startOffset S.LSN_MAX

assignShards :: Assignment -> STM ()
assignShards assignment@Assignment {..} = do
  unassign <- readTVar unassignedShards
  successCount <- tryAssignShards unassign True
  writeTVar unassignedShards (drop successCount unassign)

  reassign <- readTVar waitingReassignedShards
  reassignSuccessCount <- tryAssignShards reassign False
  writeTVar waitingReassignedShards (drop reassignSuccessCount reassign)
  where
    tryAssignShards :: [S.C_LogID] -> Bool -> STM Int
    tryAssignShards logs needStartReading = do
      (_, successCount) <- foldM
        ( \(goOn, n) shard ->
            if goOn
              then do
                ok <- tryAssignShard shard needStartReading
                if ok
                then return (True, n + 1)
                else return (False, n)
              else return (goOn, n)
        )
        (True, 0)
        logs
      return successCount

    tryAssignShard :: S.C_LogID -> Bool -> STM Bool
    tryAssignShard logId needStartReading = do
      waiters <- readTVar waitingConsumers
      if null waiters
        then do
          workSet <- readTVar consumerWorkloads
          if Set.null workSet
            then return False
            else do
              let consumer = cwConsumerName $ fromJust $ Set.lookupMin workSet
              doAssign assignment consumer logId needStartReading
              return True
        else do
          let waiter = head waiters
          doAssign assignment waiter logId needStartReading
          writeTVar waitingConsumers (tail waiters)
          return True

doAssign :: Assignment -> ConsumerName -> S.C_LogID -> Bool -> STM ()
doAssign Assignment {..} consumerName logId needStartReading = do
  when needStartReading $ do
    modifyTVar' waitingReadShards (\shards -> shards ++ [logId])

  modifyTVar' shard2Consumer (HM.insert logId consumerName)
  c2s <- readTVar consumer2Shards
  workSet <- readTVar consumerWorkloads
  case HM.lookup consumerName c2s of
    Nothing -> do
      set <- newTVar (Set.singleton logId)
      writeTVar consumer2Shards (HM.insert consumerName set c2s)
      writeTVar consumerWorkloads (Set.insert (ConsumerWorkload {cwConsumerName = consumerName, cwShardCount = 1}) workSet)
    Just ts -> do
      set <- readTVar ts
      writeTVar ts (Set.insert logId set)
      let old = ConsumerWorkload {cwConsumerName = consumerName, cwShardCount = Set.size set}
      let new = old {cwShardCount = Set.size set + 1}
      writeTVar consumerWorkloads (Set.insert new (Set.delete old workSet))

assignWaitingConsumers :: Assignment -> STM ()
assignWaitingConsumers assignment@Assignment {..} = do
  consumers <- readTVar waitingConsumers
  (_, successCount)<- foldM
    ( \(goOn, successCount) consumer ->
        if goOn
          then do
            success <- tryAssignConsumer consumer
            if success
            then return (True, successCount + 1)
            else return (False, successCount)
          else return (goOn, successCount)
    )
    (True, 0)
    consumers
  writeTVar waitingConsumers (L.drop successCount consumers)
  where
    tryAssignConsumer :: ConsumerName -> STM Bool
    tryAssignConsumer consumerName = do
      workloads <- readTVar consumerWorkloads
      case Set.lookupMax workloads of
        Nothing -> return False
        Just ConsumerWorkload {..} -> do
          if cwShardCount > 1
            then do
              res <- removeOneShardFromConsumer cwConsumerName
              case res of
                Nothing -> return False
                Just shard -> do
                  doAssign assignment consumerName shard False
                  return True
            else do
              return False

    removeOneShardFromConsumer :: ConsumerName -> STM (Maybe S.C_LogID)
    removeOneShardFromConsumer consumerName = do
      c2s <- readTVar consumer2Shards
      case HM.lookup consumerName c2s of
        Nothing -> pure Nothing
        Just shardsTVar -> do
          shards <- readTVar shardsTVar
          let (shard, newShards) = Set.deleteFindMax shards
          writeTVar shardsTVar newShards
          workloads <- readTVar consumerWorkloads
          let oldCount = Set.size shards
              target = ConsumerWorkload {cwConsumerName = consumerName, cwShardCount = oldCount}
              tempWorkloads = Set.delete target workloads
              newWorkloads = Set.insert target {cwShardCount = oldCount - 1} tempWorkloads
          writeTVar consumerWorkloads newWorkloads

          s2c <- readTVar shard2Consumer
          let newS2c = HM.delete shard s2c
          writeTVar shard2Consumer newS2c
          return $ Just shard

recvAcks
  :: ServerContext
  -> TVar SubscribeState
  -> SubscribeContext
  -> ConsumerContext
  -> StreamRecv StreamingFetchRequest
  -> IO ()
recvAcks ServerContext {..} subState subCtx ConsumerContext {..} streamRecv = loop
  where
    loop = do
      checkSubRunning
      Log.debug "Waiting for acks from client"
      streamRecv >>= \case
        Left (err :: grpcIOError) -> do
          Log.fatal . Log.buildString $ "streamRecv error: " <> show err
          -- invalid consumer
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO $ HE.StreamReadError "Consumer recv error"
        Right Nothing -> do
          -- This means that the consumer finished sending acks actively.
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO $ HE.StreamReadClose "Consumer is closed"
        Right (Just StreamingFetchRequest {..}) -> do
          Log.debug $ "received acks:" <> Log.build (V.length streamingFetchRequestAckIds)
            <> " from consumer:" <> Log.build ccConsumerName
          let cSubscriptionId = textToCBytes (subSubscriptionId subCtx)
          Stats.subscription_time_series_add_request_messages scStatsHolder cSubscriptionId 1
          Stats.subscription_stat_add_request_messages scStatsHolder cSubscriptionId 1
          unless (V.null streamingFetchRequestAckIds) $ do
            Stats.subscription_stat_add_received_acks scStatsHolder cSubscriptionId (fromIntegral $ V.length streamingFetchRequestAckIds)
            Stats.subscription_time_series_add_acks scStatsHolder cSubscriptionId (fromIntegral $ V.length streamingFetchRequestAckIds)
            doAcks scLDClient subCtx streamingFetchRequestAckIds
          loop

    -- throw error when check can not pass
    checkSubRunning :: IO ()
    checkSubRunning = do
      state <- readTVarIO subState
      if state /= SubscribeStateRunning
      then do
        Log.warning "Invalid Subscrtipion: Subscription is not running"
        atomically $ invalidConsumer subCtx ccConsumerName
        throwIO $ HE.SubscriptionInvalidError "Invalid Subscription"
      else do
        isValid <- readTVarIO ccIsValid
        if isValid
        then return ()
        else do
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO $ HE.ConsumerInvalidError "Invalid Consumer"

doAcks
  :: S.LDClient
  -> SubscribeContext
  -> V.Vector RecordId
  -> IO ()
doAcks ldclient subCtx@SubscribeContext{..} ackRecordIds = do
  atomically $ do
    removeAckedRecordIdsFromCheckList ackRecordIds
  let group = HM.toList $ groupRecordIds ackRecordIds
  forM_ group (\(logId, recordIds) -> doAck ldclient subCtx logId recordIds)
  where
    groupRecordIds :: V.Vector RecordId -> HM.HashMap S.C_LogID (V.Vector RecordId)
    groupRecordIds recordIds =
      V.foldl'
        (
          \ g r@RecordId{..} ->
            if HM.member recordIdShardId g
            then
              let ov = g HM.! recordIdShardId
              in  HM.insert recordIdShardId (V.snoc ov r) g
            else
              HM.insert recordIdShardId (V.singleton r) g
        )
        HM.empty
        recordIds

    removeAckedRecordIdsFromCheckList :: V.Vector RecordId -> STM ()
    removeAckedRecordIdsFromCheckList recordIds = do
      checkList <- readTVar subWaitingCheckedRecordIds
      checkListIndex <- readTVar subWaitingCheckedRecordIdsIndex
      mapM_
        (
          \ RecordId {..}  -> do
            let k = CheckedRecordIdsKey {
                      crkLogId = recordIdShardId,
                      crkBatchId = recordIdBatchId
                    }
            case Map.lookup k checkListIndex of
              Nothing -> pure ()
              Just CheckedRecordIds {..} -> modifyTVar crBatchIndexes (Set.delete recordIdBatchIndex)
        )
        recordIds
      (newCheckList, newCheckListIndex) <- foldM
        ( \(r, i) l@CheckedRecordIds{..} -> do
            batchIndexes <- readTVar crBatchIndexes
            let k = CheckedRecordIdsKey {
                      crkLogId =  crLogId,
                      crkBatchId = crBatchId
                    }
            if Set.null batchIndexes
            then pure (r, Map.delete k i)
            else pure (r ++ [l], i)
        )
        ([], checkListIndex)
        checkList
      writeTVar subWaitingCheckedRecordIds newCheckList
      writeTVar subWaitingCheckedRecordIdsIndex newCheckListIndex

doAck
  :: S.LDClient
  -> SubscribeContext
  -> S.C_LogID
  -> V.Vector RecordId
  -> IO ()
doAck ldclient subCtx@SubscribeContext {..} logId recordIds= do
  res <- atomically $ do
    scs <- readTVar subShardContexts
    let SubscribeShardContext {sscAckWindow = AckWindow{..}} = scs HM.! logId
    lb <- readTVar awWindowLowerBound
    ub <- readTVar awWindowUpperBound
    ars <- readTVar awAckedRanges
    bnm <- readTVar awBatchNumMap
    let shardRecordIds = recordIds2ShardRecordIds recordIds
    let (newAckedRanges, updated :: Word32) = V.foldl' (\(a, n) b -> maybe (a, n) (, n + 1) $ insertAckedRecordId b lb a bnm) (ars, 0) shardRecordIds
    when (updated > 0) $ modifyTVar subUnackedRecords (subtract updated)
    let commitShardRecordId = getCommitRecordId newAckedRanges bnm
    case tryUpdateWindowLowerBound newAckedRanges lb bnm commitShardRecordId of
      Just (ranges, newLowerBound) -> do
        -- traceM $ "[stream " <> show logId <> "] update window lower bound, from {"
        --       <> show lb <> "} to {"
        --       <> show newLowerBound <> "}"
        let batchId = sriBatchId $ fromJust commitShardRecordId
        let newBatchNumMap = Map.dropWhileAntitone (<= batchId) bnm
        -- traceM $ "[stream " <> show logId <> "] has a new ckp " <> show batchId <> ", after commit, length of ackRanges is: "
        --       <> show (Map.size newAckedRanges) <> ", 10 smallest ackedRanges: " <> printAckedRanges (Map.take 10 newAckedRanges)
        -- traceM $ "[stream " <> show logId <> "] update batchNumMap, 10 smallest batchNumMap: " <> show (Map.take 10 newBatchNumMap)
        writeTVar awAckedRanges ranges
        writeTVar awWindowLowerBound newLowerBound
        writeTVar awBatchNumMap newBatchNumMap
        return (Just batchId)
      Nothing -> do
        writeTVar awAckedRanges newAckedRanges
        return Nothing
  case res of
    Just lsn -> do
        Log.debug $ "[stream " <> Log.build logId <> "] commit checkpoint = " <> Log.buildString (show lsn)
        S.writeCheckpoints subLdCkpReader (Map.singleton logId lsn) 10{-retries-}
    Nothing  -> return ()

invalidConsumer :: SubscribeContext -> ConsumerName -> STM ()
invalidConsumer SubscribeContext{subAssignment = Assignment{..}, ..} consumer = do
  ccs <- readTVar subConsumerContexts
  case HM.lookup consumer ccs of
    Nothing -> pure ()
    Just ConsumerContext {..} -> do
      consumerValid <- readTVar ccIsValid
      if consumerValid
      then do
        writeTVar ccIsValid False
        c2s <- readTVar consumer2Shards
        case HM.lookup consumer c2s of
          Nothing        -> pure ()
          Just worksTVar -> do
            works <- swapTVar worksTVar Set.empty
            let nc2s = HM.delete consumer c2s
            writeTVar consumer2Shards nc2s
            idleShards <- readTVar waitingReassignedShards
            shardMap <- readTVar shard2Consumer
            (shardsNeedAssign, newShardMap)
              <- foldM unbindShardWithConsumer (idleShards, shardMap) works
            writeTVar waitingReassignedShards shardsNeedAssign
            writeTVar shard2Consumer newShardMap
            modifyTVar consumerWorkloads
              (Set.filter (\ConsumerWorkload{..} ->
                              cwConsumerName /= consumer))
        let newConsumerCtx = HM.delete consumer ccs
        writeTVar subConsumerContexts newConsumerCtx
      else pure ()
  where
    unbindShardWithConsumer (shards, mp) logId = return (shards ++ [logId], HM.delete logId mp)

tryUpdateWindowLowerBound
  :: Map.Map ShardRecordId ShardRecordIdRange -- ^ ackedRanges
  -> ShardRecordId                       -- ^ lower bound record of current window
  -> Map.Map Word64 Word32          -- ^ batchNumMap
  -> Maybe ShardRecordId          -- ^ commitPoint
  -> Maybe (Map.Map ShardRecordId ShardRecordIdRange, ShardRecordId)
tryUpdateWindowLowerBound ackedRanges lowerBoundRecordId batchNumMap (Just commitPoint) =
  Map.lookupMin ackedRanges >>= \(_, ShardRecordIdRange minStartRecordId minEndRecordId) ->
    if | minStartRecordId == lowerBoundRecordId && (sriBatchId minEndRecordId) == (sriBatchId commitPoint) ->
            -- The current ackedRange [minStartRecordId, minEndRecordId] contains the complete batch record and can be committed directly,
            -- so remove the hole range [minStartRecordId, minEndRecordId], update windowLowerBound to successor of minEndRecordId
           Just (Map.delete minStartRecordId ackedRanges, getSuccessor minEndRecordId batchNumMap)
       | minStartRecordId == lowerBoundRecordId ->
           -- The ackedRange [minStartRecordId, commitPoint] contains the complete batch record and will be committed,
           -- update ackedRange to [successor of commitPoint, minEndRecordId]
           let newAckedRanges = Map.delete minStartRecordId ackedRanges
               startRecordId = getSuccessor commitPoint batchNumMap
               newAckedRanges' = Map.insert startRecordId (ShardRecordIdRange startRecordId minEndRecordId) newAckedRanges
            in Just(newAckedRanges', startRecordId)
       | otherwise -> Nothing
tryUpdateWindowLowerBound _ _ _ Nothing = Nothing

recordIds2ShardRecordIds :: V.Vector RecordId -> V.Vector ShardRecordId
recordIds2ShardRecordIds =
  V.map (\RecordId {..} -> ShardRecordId {sriBatchId = recordIdBatchId, sriBatchIndex = recordIdBatchIndex})

addUnackedRecords :: SubscribeContext -> Int -> STM ()
addUnackedRecords SubscribeContext {..} count = do
  -- traceM $ "addUnackedRecords: " <> show count
  unackedRecords <- readTVar subUnackedRecords
  writeTVar subUnackedRecords (unackedRecords + fromIntegral count)
