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
import           Control.Exception                (catch, onException, throwIO,
                                                   try)
import           Control.Monad                    (forM_, unless, when)
import           Data.Function                    (on)
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (modifyIORef', newIORef,
                                                   readIORef)
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
import           Z.IO.LowResTimer                 (registerLowResTimer)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception         (StreamNotExist (..),
                                                   SubscribeInnerError (..),
                                                   SubscriptionIdNotFound (..),
                                                   SubscriptionWatchOnDifferentNode (..),
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
import           HStream.Utils                    (cBytesToText, returnResp,
                                                   textToCBytes)

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
--------------------------------------------------------------------------------

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
  unless (getAllocatedNodeId hr subId == serverID) $
    throwIO SubscriptionWatchOnDifferentNode

  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req
  subscription <- P.getObject @ZHandle @'SubRep subId zkHandle
  when (isNothing subscription) $ throwIO (SubscriptionIdNotFound subId)
  Core.deleteSubscription ctx (fromJust subscription)
  returnResp Empty
--------------------------------------------------------------------------------

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = checkSubscriptionExistRequestSubscriptionId
  res <- P.checkIfExist @ZHandle @'SubRep sid zkHandle
  returnResp . CheckSubscriptionExistResponse $ res
--------------------------------------------------------------------------------

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
    handleException SubscribeInValidError = return $
      ServerBiDiResponse [] StatusAborted . StatusDetails $ "subscription is invalid"
    handleException ConsumerInValidError = return $
      ServerBiDiResponse [] StatusAborted . StatusDetails $ "consumer is invalid"

streamingFetchInternal
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO ()
streamingFetchInternal ctx@ServerContext {..} (ServerBiDiRequest _ streamRecv streamSend) = do
  StreamingFetchRequest {..} <- firstRecv 
  wrapper@SubscribeContextWrapper {..} <- initSub ctx streamingFetchRequestSubscriptionId 
  consumerCtx <- initConsumer scwContext consumerName streamSend
  recvAcks scwState scwContext consumerCtx streamRecv
  where
    firstRecv :: IO StreamingFetchRequest 
    firstRecv = 
      streamRecv >>= \case
        Left _                -> throwIO GRPCStreamRecvError
        Right Nothing         -> throwIO GRPCStreamRecvCloseError
        Right (Just firstReq) -> return firstReq 

initSub :: ServerContext -> SubscriptionId -> IO SubscribeContextWrapper
initSub serverCtx@ServerContext {..} subId = do
  (needInit, SubscribeContextNewWrapper {..}) <- atomically $ do
    subMap <- readTVar scSubscribeContexts 
    case HM.lookup subId subMap of
      Nothing -> do
        state <- newTVar SubscribeStateNew
        ctx <- newTVar Nothing
        let wrapper = SubscribeContextNewWrapper {scnwState = state, scnwContext = ctx}
        let newSubMap = HM.insert subId wrapper subMap
        writeTVar subContexts newSubMap
        return (True, wrapper)
      Just wrapper@SubscribeContextNewWrapper {..} -> do
        state <- readTVar scnwState
        case state of
          SubscribeStateNew -> retry
          SubscribeStateRunning -> return (False, wrapper)
          _ -> throwSTM SubscribeInValidError 
  if needInit
    then do
      subCtx <- doSubInit subId
      wrapper <- atomically $ do
        writeTVar scnwContext (Just subCtx)
        writeTVar scnwState SubscribeStateRunning
        return SubscribeContextWrapper {scwState = scnwState, scwContext = subCtx}
      forkIO $ sendRecords serverCtx wrapper 
      return wrapper
    else do
      mctx <- readTVarIO scnwContext
      return $ SubscribeContextWrapper {scwState = scnwState, scwContext = fromJust mctx}

doSubInit :: ServerContext -> SubscriptionId -> IO SubscribeContext
doSubInit ServerContext{..} subId = do
  P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
    Nothing -> do
      Log.error $ "unexpected error: subscription " <> Log.buildText streamingFetchRequestSubscriptionId <> " not exist."
      throwIO $ SubscriptionIdNotFound streamingFetchRequestSubscriptionId
    Just Subscription {..} -> do
      -- create a ldCkpReader for reading new records
      let readerName = textToCBytes streamingFetchRequestSubscriptionId
      ldCkpReader <-
        --TODO: check this
        S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 1 Nothing 10
      Log.debug $ "created a ldCkpReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

      -- create a ldReader for rereading unacked records
      --TODO: check this
      ldReader <- S.newLDReader scLDClient 5000 Nothing
      Log.debug $ "created a ldReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

      consumerContexts <- newTVar HM.empty 
      shardContexts <- newTVar HM.empty 
      assignment <- mkEmptyAssignment
      let emptySubCtx =
            SubscribeContext
              { subSubscriptionId = subId,
                subStreamName = subscriptionStreamName ,
                --TODO: check unit
                subAckTimeoutSeconds = subscriptionAckTimeoutSeconds,
                subLdCkpReader = ldCkpReader, 
                subLdReader = ldReader,
                subConsumerContexts = consumerContexts,
                subShardContexts = shardContexts,
                subAssignment = assignment
              }
      shards <- getShards subscriptionStreamName 
      addNewShardsToSubCtx emptySubCtx shards
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

    getShards :: T.Text -> IO [HS.C_LogID]
    getShards streamName = undefined

addNewShardsToSubCtx :: SubscribeContext -> [HS.C_LogID] -> IO ()  
addNewShardsToSubCtx SubscribeContext {..} shards = atomically $ do  
  let Assignment {..} = subAssignment
  oldTotal <- readTVar totalShards
  oldUnassign <- readTVar unassignedShards 
  oldShardCtxs <- readTVar subShardContexts 
  let (newTotal, newUnassign, newShardCtxs) = foldM 
                  (
                    \ (ot, ou, os) l -> 
                      if Set.member l ot 
                      then return (ot, ou, os)      
                      else
                        lb <- newTVar $ RecordId S.LSN_MIN 0 
                        ub <- newTVar maxBound 
                        ar <- newTVar $ Map.empty
                        bn <- newTVar $ Map.empty
                        let ackWindow = AckWindow
                              { awWindowLowerBound = lb,
                                awWindowUpperBound = ub,
                                awAckedRanges = ar,
                                awBatchNumMap = bn 
                              }
                            subShardCtx = SubscribeShardContext {sscAckWindow = ackWindow, sscLogId = l} 
                        return (Set.insert l ot, ou ++ [l], HM.insert l subShardCtx os) 
                  )
                  (oldTotal, oldUnassign, oldShardCtxs)
                  (pure shards)
  writeTVar totalShards newTotal
  writeTVar unassignedShards newUnassign 
  writeTVar subShardContexts newShardCtxs 

initConsumer :: SubscribeContext -> ConsumerName -> StreamSend StreamingFetchResponse -> IO ConsumerContext 
initConsumer SubscribeContext {..} consumerName streamSend = atomically $ do 
  let Assignment {..} = subAssignment
  oldWcs <- readTVar waitingConsumers
  writeTVar waitingConsumers (oldWcs ++ [consumerName])

  iv <- newTVar True
  ss <- newTVar streamSend
  let cc = ConsumerContext
            { ccConsumerName = consumerName,
              ccIsValid = iv,
              ccStreamSend = ss 
            }
  oldCcs <- readTVar subConsumerContexts
  writeTVar subConsumerContexts (HM.insert consumerName cc odlCcs)
  return cc

sendRecords :: ServerContext -> SubscribeContextWrapper -> IO ()
sendRecords ServerContext {..} SubscribeContextWrapper {..} =
  threadDelay 10000
  let subCtx@SubscribeContext {..} = scwContext 
  loop
  where
    loop = do
      state <- readTVarIO scwState
      if state == SubscribeStateRunning
        then do
          atomically $ do
            assignShards subAssignment
            assignWaitingConsumers subAssignment
          addRead subAssignment
          recordBatches <- readRecordBatches
          let receivedRecordsVecs = fmap decodeRecordBatch recordBatches 
          sendReceivedRecordsVecs receivedRecordsVecs  
          -- TODO: resend
          loop
        else 
          return ()

    addRead :: S.LdCkpReader -> Assignment -> IO ()
    addRead ldCkpReader Assignment {..} = do 
      shards <- atomically $ do
        shards <- readTVar waitingReadShards
        writeTVar waitingReadShards []
        return shards
      forM_ 
        shards 
        (\shard -> S.startReadingFromCheckpointOrStart ldCkpReader shard (Just S.LSN_MIN) S.LSN_MAX)

    readRecordBatches :: IO [S.DataRecord]
    readRecordBatches = 
      S.ckpReaderReadAllowGap ssriLdCkpReader 1000 >>= \case
        Left gap@S.GapRecord {..} -> do
          atomically $ do
            scs <- readTVar subShardContexts
            let SubscribeShardContext {..} = scs HM.! gapLogID
                AckWindow {..} = sscAckWindow
            ranges <- readTVar awAckedRanges
            batchNumMap <- readTVar awBatchNumMap
            -- insert gap range to ackedRanges
            let gapLoRecordId = RecordId gapLoLSN minBound
                gapHiRecordId = RecordId gapHiLSN maxBound
                newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) ranges 
                -- also need to insert lo_lsn record and hi_lsn record to batchNumMap
                -- because we need to use these info in `tryUpdateWindowLowerBound` function later.
                groupNums = map (, 0) [gapLoLSN, gapHiLSN]
                newBatchNumMap = Map.union batchNumMap (Map.fromList groupNums)
            writeTVar awAckedRanges newRanges
            writeTVar awBatchNumMap newBatchNumMap
          return []
        Right dataRecords -> return dataRecords

    decodeRecordBatch :: S.DataRecord Bytes -> (S.C_LogID, Word64, V.Vector ReceivedRecord)
    decodeRecordBatch dataRecord = 
      let payload = recordPayload dataRecord
          logId = recordLogId dataRecord 
          batchId = recordLSN dataRecord
          recordBatch = decodeBatch payload
          batch = hstreamRecordBatchBatch recordBatch         
          receivedRecords = mkReceivedRecords batchId batch 
      in
          (logId, batchId, receivedRecords)
        

    mkReceivedRecords :: Word64 -> V.Vector ByteString -> V.Vector ReceivedRecord
    mkReceivedRecords batchId records =
      V.imap (\ i a -> ReceivedRecord (Just $ RecordId batchId (fromIntegral index)) a) records   

    sendReceivedRecordsVecs :: [(S.C_LogID, Word64, V.Vector ReceivedRecord)] -> IO ()
    sendReceivedRecordsVecs vecs =
      foldM
        (
          \ skipSet (logId, batchId, vec)->
            if Set.member logId skipSet
            then return skipSet
            else do
              ok <- sendReceivedRecords logId batchId vec
              if ok
              then return skipSet
              else return $ Set.insert logId skipSet
        )
        Set.empty
        vecs

    sendReceivedRecords :: S.C_LogID -> Word64 -> V.Vector ReceivedRecord -> Bool -> IO Bool 
    sendReceivedRecords logId batchId records isResent = do 
      let Assignment {..} = subAssignment
      mres <- atomically $ do
        if not isResent 
        then do
          scs <- readTVar subShardContexts
          let SubscribeShardContext {..} = scs HM.! logId 
              AckWindow {..} = sscAckWindow
          batchNumMap <- readTVar awBatchNumMap
          let newBatchNumMap = HM.insert batchId (v.length records) batchNumMap
          writeTVar awBatchNumMap newBatchNumMap
        else pure ()

        s2c <- readTVar shard2Consumer
        let consumer = s2c HM.! logId 
        ccs <- readTVar subConsumerContexts 
        let ConsumerContext {..} = ccs HM.! consumerName 
        if ccIsValid
        then return $ Just (ccConsumerName, ccStreamSend)
        else Nothing 
      case mres of
        Nothing -> return False 
        Just (consumerName, streamSend) = do 
          streamSend (StreamingFetchResponse records) >>= \case
            Left err -> do
              Log.error $ "send records error, will remove the consumer: " <> show err
              atomically $ invalidConsumer subCtx consumerName 
              if not isResent
              then do 
                let ReceivedRecord {..} = V.head records
                    lsn = recordIdBatchId receivedRecordRecordId
                resetReadingOffset logId lsn
              else pure ()
              return False
            Right _ -> do
              let recordIds = V.map (fromJust . receivedRecordRecordId) records
              registerResend logId batchId recordIds
              return True 

    registerResend logId batchId recordIds =
      void $ registerLowResTimer
           (fromIntegral (subAckTimeoutSeconds * 10))
           (void $ forkIO $ resendTimeoutRecords logId batchId recordIds)

    resendTimeoutRecords logId batchId recordIds = do
      resendRecordIds <- atomically $ do
        scs <- readTVar subShardContexts
        let SubscribeShardContext {..} = scs HM.! logId 
            AckWindow {..} = sscAckWindow 
        return $ filterUnackedRecordIds recordIds awAckedRanges awWindowLowerBound 

      if V.null resendRecordIds
      then return ()
      else do
        S.readerStartReading subLdReader logId batchId batchId 
        dataRecord <- S.readerRead ldreader 1 
        let (_, _, records) = decodeRecordBatch dataRecord 
        if length records /= V.length recordIds
          then do
            Log.fatal $ "unexpected error: read records error"
          else do
            let 
              resendRecords = 
                V.foldl' 
                  (\a RecordId {..} -> 
                    V.snoc a (records ! recordIdBatchIndex) 
                  ) 
                  V.empty
                  resendRecordIds
            sendReceivedRecords logId batchId resendRecords
      
    filterUnackedRecordIds recordIds ackedRanges windowLowerBound =
      flip V.filter recordIds $ \recordId ->
        (recordId >= windowLowerBound)
          && case Map.lookupLE recordId ackedRanges of
            Nothing                               -> True
            Just (_, RecordIdRange _ endRecordId) -> recordId > endRecordId

    resetReadingOffset :: S.C_LogID -> S.LSN -> IO ()
    resetReadingOffset logId startOffset = do 
      S.ckpReaderStartReading subLdCkpReader logId startOffset S.LSN_MAX 

assignShards :: Assignment -> STM ()
assignShards assignment@Assignment {..} = do
  unassign <- readTVar unassignedShards
  tryAssignShards unassign True

  reassign <- readTVar waitingReassignedShards
  tryAssignShards reassign False
  where
    tryAssignShards :: [S.C_LogID] -> Bool -> STM ()
    tryAssignShards logs needStartReading =
      foldM_
        ( \goOn shard ->
            if goOn
              then tryAssignShard shard needStartReading
              else return goOn
        )
        True
        logs

    tryAssignShard :: LogId -> Bool -> STM Bool
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
          return True

doAssign :: Assignment -> ConsumerName -> LogId -> Bool -> STM ()
doAssign Assignment {..} consumerName logId needStartReading = do
  if needStartReading
    then do
      waitShards <- readTVar waitingReadShards
      writeTVar waitingReadShards (waitShards ++ [logId])
    else return ()

  s2c <- readTVar shard2Consumer
  writeTVar shard2Consumer (HM.insert logId consumerName s2c)
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
      let new = old {cwShardCount = (Set.size set) + 1}
      writeTVar consumerWorkloads (Set.insert new (Set.delete old workSet))

assignWaitingConsumers :: Assignment -> STM ()
assignWaitingConsumers assignment@Assignment {..} = do
  consumers <- readTVar waitingConsumers
  foldM_
    ( \goOn consumer ->
        if goOn
          then tryAssignConsumer consumer
          else return goOn
    )
    True
    consumers
  where
    tryAssignConsumer :: ConsumerName -> STM Bool
    tryAssignConsumer consumerName = do
      workloads <- readTVar consumerWorkloads
      case Set.lookupMax workloads of
        Nothing -> return False
        Just ConsumerWorkload {..} -> do
          if cwShardCount > 1
            then do
              shard <- removeOneShardFromConsumer cwConsumerName
              doAssign assignment consumerName shard False
              return True
            else do
              return False

    removeOneShardFromConsumer :: ConsumerName -> STM LogId
    removeOneShardFromConsumer consumerName = do
      c2s <- readTVar consumer2Shards
      let shardsTVar = c2s HM.! consumerName
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
      return shard

recvAcks :: ServerContext -> TVar SubscribeState -> SubscribeContext -> ConsumerContext ->  (StreamRecv StreamingFetchRequest) -> IO () 
recvAcks ServerContext {..} subState subCtx@SubscribeContext {..} ConsumerContext {..} streamRecv = loop 
  where
    loop = do 
      check
      streamRecv >>= \case
        Left (err :: grpcIOError) -> do
          Log.error . Log.buildString $ "streamRecv error: " <> show err
          -- invalid consumer
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO GRPCStreamRecvError
        Right Nothing -> do
          -- This means that the consumer finished sending acks actively.
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO GRPCStreamRecvCloseError
        Right (Just StreamingFetchRequest {..}) ->
          if V.null streamingFetchRequestAckIds
            then loop 
            else do
              doAcks scLDClient subCtx streamingFetchRequestAckIds
              loop

    -- throw error when check can not pass
    check :: IO () 
    check = do 
      ss <- readTVarIO subState
      if ss /= SubscribeStateRunning 
      then throwIO SubscribeInValidError
      else do 
        cv <- readTVarIO ccIsValid
        if cv
        then return ()
        else throwIO ConsumerInValidError 
      

doAcks
  :: S.LDClient
  -> SubscribeContext  
  -> V.Vector RecordId
  -> IO ()
doAcks ldclient subCtx ackRecordIds = do
  let group = HM.toList $ groupRecordIds ackRecordIds
  forM_ group (\(logId, recordIds) -> doAck ldclient subCtx logId recordIds)
  where
    groupRecordIds :: V.Vector RecordId -> HM.HashMap C_LogID (V.Vector RecordId)
    groupRecordIds recordIds =  
      V.foldl'
        (
          \ g r -> 
            let shardId = recordIdShardId r 
            in 
                if HM.member shardId g 
                then 
                  let ov = g HM.! shardId
                  in  HM.insert shardId (V.snoc ov r) g 
                else
                  HM.insert shardId (V.singleton r) g 
        )
        HM.empty
        recordIds

doAck
  :: S.LDClient
  -> SubscribeContext  
  -> HS.C_LogID
  -> V.Vector RecordId
  -> IO ()
doAck ldclient SubscribeContext {..} logId recordIds= do
  res <- atomically $ do
    scs <- readTVar subShardContexts
    SubscribeShardContext {..} <- scs HM.！logId
    let AckWindow {..} = sscAckWindow
    lb <- readTVar awWindowLowerBound
    ub <- readTVar awWindowUpperBound
    ars <- readTVar awAckedRanges
    bnm <- readTVar awBatchNumMap
    let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b lb a bnm) ars recordIds 
    let commitLSN = recordIdBatchId $ fromJust $ getCommitRecordId newAckedRanges bnm 
    case tryUpdateWindowLowerBound newAckedRanges lb bnm commitLSN of
      Just (ranges, newLowerBound) -> do 
        let newBatchNumMap = Map.dropWhileAntitone (<= commitLSN) bnm 
        writeTVar awAckedRanges ranges
        writeTVar awWindowLowerBound newLowerBound
        writeTVar awBatchNumMap newBatchNumMap 
        return (Just commitLSN)
      Nothing -> 
        writeTVar awAckedRanges newAckedRanges 
        return Nothing
  case res of
    Just lsn -> S.writeCheckpoints subLdCkpReader (Map.singleton logId lsn)
    Nothin -> return ()
    
invalidConsumer :: SubscribeContext -> ConsumerName -> STM ()
invalidConsumer SubscribeContext{..} consumer = do 
  ccs <- readTVar subConsumerContexts
  let cc@ConsumerContext {..} = ccs HM.! consumer
  iv <- readTVar ccIsValid
  if iv
  then do
    writeTVar ccIsValid False

    let Assignment {..} = subAssignment
    c2s <- readTVar consumer2Shards
    let worksTVar = c2s HM.! consumer
    works <- readTVar worksTVar
    writeTVar worksTVar Set.empty
    let nc2s = HM.delete consmer c2s
    writeTVar consumer2Shards nc2s

    rs <- readTVar waitingReassignedShards 
    s2c <- readTVar shard2Consumer 
    (nrs, ns2c) <- foldM
      (
        \ (nrs, ns2c) s ->
          (nrs ++ [s], HM.delete s ns2c)
      )
      (rs, s2c)
      works
    writeTVar waitingReassignedShards nrs 
    writeTVar shard2Consumer ns2c 
    
    let nccs = HM.delete conusmer ccs
    writeTVar subConsumerContexts nccs 
  else pure () 

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

data SubscribeInnerError = GRPCStreamRecvError
                             | GRPCStreamRecvCloseError
                             | GRPCStreamSendError
                             | ConsumerInValidError
                             | SubscribeInValidError 
  deriving (Show)
instance Exception SubscribeInnerError

