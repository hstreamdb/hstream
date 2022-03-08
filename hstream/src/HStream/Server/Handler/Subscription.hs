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
streamingFetchHandler ctx bidiRequest = undefined


streamingFetchInternal
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO ()
streamingFetchInternal ctx@ServerContext {..} (ServerBiDiRequest _ streamRecv streamSend) = do
  StreamingFetchRequest {..} <- firstRecv 
  wrapper@SubscribeContextWrapper {..} <- initSub ctx subId
  initConsumer scwContext consumerName streamSend
  where
    firstRecv :: IO StreamingFetchRequest 
    firstRecv = undefined

initSub :: ServerContext -> SubscriptionId -> IO SubscribeContextWrapper
initSub ServerContext {..} subId = do
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
          -- TODO: how to deal with other state ?
  if needInit
    then do
      subCtx <- doSubInit subId
      atomically $ do
        writeTVar scnwContext (Just subCtx)
        writeTVar scnwState SubscribeStateRunning
        return SubscribeContextWrapper {scwState = scnwState, scwContext = subCtx}
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

initConsumer :: SubscribeContext -> ConsumerName -> StreamSend StreamingFetchResponse -> IO ()
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

sendRecords :: SubscribeContext -> SubscribeContextWrapper -> IO ()
sendRecords SubscribeContext {..} SubscribeContextWrapper {..} =
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
          recordBatches <- readRecords
          let receivedRecords = recordBatchesToReceivedRecords recordBatches
          sendReceivedRecords receivedRecords
          -- TODO: resend
        else 
          return ()

    addRead :: Assignment -> IO ()
    addRead = undefined

    readRecords :: IO [RecordBatch]
    readRecords = undefined

    recordBatchesToReceivedRecords :: [RecordBatch] -> [ReceivedRecord]
    recordBatchesToReceivedRecords = undefined

    sendReceivedRecords :: [ReceivedRecord] -> IO ()
    sendReceivedRecords = undefined

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
