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
import           Control.Concurrent.STM
import           Control.Exception                (Exception, Handler (Handler),
                                                   catch, throwIO)
import           Control.Monad                    (foldM, foldM_, forM_,
                                                   forever, unless, when)
import qualified Data.ByteString                  as B
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (modifyIORef', newIORef,
                                                   readIORef, writeIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isNothing)
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Debug.Trace
import           Network.GRPC.HighLevel           (StreamRecv, StreamSend)
import           Network.GRPC.HighLevel.Generated
import           Z.Data.Vector                    (Bytes)
import           Z.IO.LowResTimer                 (registerLowResTimer)
import           ZooKeeper.Types                  (ZHandle)

import           Data.Text.Encoding               (encodeUtf8)
import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception         (ExceptionHandle, Handlers,
                                                   StreamNotExist (..),
                                                   SubscriptionIdNotFound (..),
                                                   defaultHandlers,
                                                   mkExceptionHandle,
                                                   setRespType)
import           HStream.Server.Handler.Common    (bindSubToStreamPath,
                                                   getCommitRecordId,
                                                   getSuccessor,
                                                   insertAckedRecordId,
                                                   removeSubFromStreamPath)
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence       (ObjRepType (..))
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (decodeBatch, mkServerErrResp,
                                                   returnResp, textToCBytes)

--------------------------------------------------------------------------------

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata sub@Subscription{..}) = subExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  bindSubToStreamPath zkHandle streamName subName
  catch (Core.createSubscription ctx sub) $
    \(e :: StreamNotExist) -> removeSubFromStreamPath zkHandle streamName subName >> throwIO e
  returnResp sub
  where
    streamName = textToCBytes subscriptionStreamName
    subName = textToCBytes subscriptionSubscriptionId
--------------------------------------------------------------------------------

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest
  { deleteSubscriptionRequestSubscriptionId = subId }) = subExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req

  hr <- readMVar loadBalanceHashRing
  unless (getAllocatedNodeId hr subId == serverID) $
    throwIO SubscriptionOnDifferentNode

  subscription <- P.getObject @ZHandle @'SubRep subId zkHandle
  when (isNothing subscription) $ throwIO (SubscriptionIdNotFound subId)
  Core.deleteSubscription ctx (fromJust subscription)
  returnResp Empty
-- --------------------------------------------------------------------------------

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = checkSubscriptionExistRequestSubscriptionId
  res <- P.checkIfExist @ZHandle @'SubRep sid zkHandle
  returnResp . CheckSubscriptionExistResponse $ res
-- --------------------------------------------------------------------------------

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler sc (ServerNormalRequest _metadata ListSubscriptionsRequest) = subExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse <$> Core.listSubscriptions sc
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res
-- --------------------------------------------------------------------------------

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx bidiRequest = subStreamingExceptionHandle do
  Log.debug "recv server call: streamingFetch"
  streamingFetchInternal ctx bidiRequest
  return $ ServerBiDiResponse mempty StatusUnknown "should not reach here"

streamingFetchInternal
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO ()
streamingFetchInternal ctx (ServerBiDiRequest _ streamRecv streamSend) = do
  StreamingFetchRequest {..} <- firstRecv
  Log.debug "pass first recv"
  SubscribeContextWrapper {..} <- initSub ctx streamingFetchRequestSubscriptionId
  Log.debug "pass initSub"
  consumerCtx <- initConsumer scwContext streamingFetchRequestConsumerName streamSend
  Log.debug "pass initConsumer"
  recvAcks ctx scwState scwContext consumerCtx streamRecv
  Log.debug "pass recvAcks"
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
        wrapper <- SubscribeContextNewWrapper <$> newTVar SubscribeStateNew <*> newEmptyTMVar
        let newSubMap = HM.insert subId wrapper subMap
        writeTVar scSubscribeContexts newSubMap
        return (True, wrapper)
      Just wrapper@SubscribeContextNewWrapper {..} -> do
        readTVar scnwState >>= \case
          SubscribeStateNew     -> retry
          SubscribeStateRunning -> return (False, wrapper)
          _                     -> throwSTM SubscribeInValidError
  if needInit
    then do
      subCtx <- doSubInit serverCtx subId
      wrapper@SubscribeContextWrapper{..} <- atomically $ do
        putTMVar scnwContext subCtx
        writeTVar scnwState SubscribeStateRunning
        return SubscribeContextWrapper {scwState = scnwState, scwContext = subCtx}
      Log.debug $ "ready to forkIO run sendRecords for sub " <> Log.buildText subId
      void . forkIO $ sendRecords serverCtx scwState scwContext
      return wrapper
    else do
      mctx <- atomically $ readTMVar scnwContext
      return $ SubscribeContextWrapper {scwState = scnwState, scwContext = mctx}

-- For each subscriptionId, create ldCkpReader and ldReader, then
-- add all shards of target stream to SubscribeContext
doSubInit :: ServerContext -> SubscriptionId -> IO SubscribeContext
doSubInit ctx@ServerContext{..} subId = do
  P.getObject subId zkHandle >>= \case
    Nothing -> do
      Log.fatal $ "unexpected error: subscription " <> Log.buildText subId <> " not exist."
      throwIO $ SubscriptionIdNotFound subId
    Just Subscription {..} -> do
      let readerName = textToCBytes subId
      -- Notice: doc this. shard count can not larger than this.
      let maxReadLogs = 1000
      -- see: https://logdevice.io/api/classfacebook_1_1logdevice_1_1_client.html#a797d6ebcb95ace4b95a198a293215103
      let ldReaderBufferSize = 10
      -- create a ldCkpReader for reading new records
      ldCkpReader <-
        S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 maxReadLogs (Just ldReaderBufferSize) 5
      S.ckpReaderSetTimeout ldCkpReader 10  -- 10 milliseconds
      Log.debug $ "created a ldCkpReader for subscription {" <> Log.buildText subId <> "}"

      -- create a ldReader for rereading unacked records
      ldReader <- newMVar =<< S.newLDReader scLDClient maxReadLogs (Just ldReaderBufferSize)
      Log.debug $ "created a ldReader for subscription {" <> Log.buildText subId <> "}"

      unackedRecords <- newTVarIO 0
      consumerContexts <- newTVarIO HM.empty
      shardContexts <- newTVarIO HM.empty
      assignment <- mkEmptyAssignment
      curTime <- newTVarIO 0
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
                subWaitingCheckedRecordIdsIndex = checkListIndex
              }
      shards <- getShards ctx subscriptionStreamName
      Log.debug $ "get shards: " <> Log.buildString (show shards)
      addNewShardsToSubCtx emptySubCtx shards
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
getShards :: ServerContext -> T.Text -> IO [S.C_LogID]
getShards ServerContext{..} streamName = do
  let streamId = transToStreamName streamName
  res <- S.listStreamPartitions scLDClient streamId
  return $ Map.elems res

addNewShardsToSubCtx :: SubscribeContext -> [S.C_LogID] -> IO ()
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
    addShards old@(total, unassign, ctx) logId
      | Set.member logId total = return old
      | otherwise = do
          lowerBound <- newTVar $ ShardRecordId S.LSN_MIN 0
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
initConsumer :: SubscribeContext -> ConsumerName -> StreamSend StreamingFetchResponse -> IO ConsumerContext
initConsumer SubscribeContext {subAssignment = Assignment{..}, ..} consumerName streamSend = do
  sender <- newMVar streamSend
  atomically $ do
    modifyTVar' waitingConsumers (\consumers -> consumers ++ [consumerName])

    isValid <- newTVar True
    let cc = ConsumerContext
              { ccConsumerName = consumerName,
                ccIsValid = isValid,
                ccStreamSend = sender
              }
    modifyTVar' subConsumerContexts (HM.insert consumerName cc)
    return cc

sendRecords :: ServerContext -> TVar SubscribeState -> SubscribeContext -> IO ()
sendRecords ctx subState subCtx@SubscribeContext {..} = do
  Log.debug "enter sendRecords"
  threadDelay 10000
  isFirstSendRef <- newIORef True
  loop isFirstSendRef
  where
    loop isFirstSendRef = do
      Log.debug "enter sendRecords loop"
      state <- readTVarIO subState
      if state == SubscribeStateRunning
        then do
          newShards <- getNewShards
          unless (L.null newShards) $ do
            addNewShardsToSubCtx subCtx newShards
            Log.info $ "add shards " <> Log.buildString (show newShards)
                    <> " to consumer " <> Log.buildText subSubscriptionId
          atomically $ do
            checkAvailable subShardContexts
            checkAvailable subConsumerContexts
          atomically $ do
            assignShards subAssignment
            assignWaitingConsumers subAssignment
          addRead subLdCkpReader subAssignment
          atomically checkUnackedRecords
          recordBatches <- readRecordBatches
          Log.debug $ "readBatches size " <> Log.buildInt (length recordBatches)
          let receivedRecordsVecs = fmap decodeRecordBatch recordBatches
          isFirstSend <- readIORef isFirstSendRef
          if isFirstSend
          then do
            writeIORef isFirstSendRef False
            void $ forkIO $ forever $ do
              threadDelay (100 * 1000)
              updateClockAndDoResend
          else pure ()
          successSendRecords <- sendReceivedRecordsVecs receivedRecordsVecs
          Log.debug "pass sendReceivedRecordsVecs"
          atomically $ addUnackedRecords subCtx successSendRecords
          loop isFirstSendRef
        else
          return ()

    updateClockAndDoResend :: IO ()
    updateClockAndDoResend = do
      doneList <- atomically $ do
        ct <- readTVar subCurrentTime
        let newTime = ct + 1
        checkList <- readTVar subWaitingCheckedRecordIds
        let (doneList, leftList) = span ( \CheckedRecordIds {..} -> crDeadline <= newTime) checkList
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


    getNewShards :: IO [S.C_LogID]
    getNewShards = do
      shards <- getShards ctx subStreamName
      if L.null shards
        then return []
        else do
          atomically $ do
            let Assignment {..} = subAssignment
            shardSet <- readTVar totalShards
            foldM
              (\a b ->
                if Set.member b shardSet
                then return a
                else return $ a ++ [b]
              )
              []
              shards

    addRead :: S.LDSyncCkpReader -> Assignment -> IO ()
    addRead ldCkpReader Assignment {..} = do
      shards <- atomically $ swapTVar waitingReadShards []
      forM_ shards $ \shard -> do
        Log.debug $ "start reading " <> Log.buildString (show shard)
        S.startReadingFromCheckpointOrStart ldCkpReader shard (Just S.LSN_MIN) S.LSN_MAX

    readRecordBatches :: IO [S.DataRecord Bytes]
    readRecordBatches = do
      S.ckpReaderReadAllowGap subLdCkpReader 100 >>= \case
        Left gap@S.GapRecord {..} -> do
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

    decodeRecordBatch :: S.DataRecord Bytes -> (S.C_LogID, Word64, V.Vector ShardRecordId, V.Vector ReceivedRecord)
    decodeRecordBatch dataRecord =
      let payload = S.recordPayload dataRecord
          logId = S.recordLogID dataRecord
          batchId = S.recordLSN dataRecord
          recordBatch = decodeBatch payload
          batch = hstreamRecordBatchBatch recordBatch
          (shardRecordIds, receivedRecords) = mkReceivedRecords logId batchId batch
      in
          (logId, batchId, shardRecordIds, receivedRecords)

    mkReceivedRecords :: S.C_LogID -> Word64 -> V.Vector B.ByteString -> (V.Vector ShardRecordId, V.Vector ReceivedRecord)
    mkReceivedRecords logId batchId records =
      let
          shardRecordIds = V.imap (\ i _ -> ShardRecordId batchId (fromIntegral i)) records
          receivedRecords = V.imap (\ i a -> ReceivedRecord (Just $ RecordId logId batchId (fromIntegral i)) a) records
      in  (shardRecordIds, receivedRecords)

    sendReceivedRecordsVecs :: [(S.C_LogID, Word64, V.Vector ShardRecordId, V.Vector ReceivedRecord)] -> IO Int
    sendReceivedRecordsVecs vecs = do
      (_, successRecords) <- foldM
        (
          \ (skipSet, successRecords) (logId, batchId, shardRecordIdVec, vec)->
            if Set.member logId skipSet
            then return (skipSet, successRecords)
            else do
              ok <- sendReceivedRecords logId batchId shardRecordIdVec vec False
              if ok
              then return (skipSet, successRecords + V.length vec)
              else return (Set.insert logId skipSet, successRecords)
        )
        (Set.empty, 0)
        vecs
      pure successRecords

    sendReceivedRecords :: S.C_LogID -> Word64 -> V.Vector ShardRecordId -> V.Vector ReceivedRecord -> Bool -> IO Bool
    sendReceivedRecords logId batchId shardRecordIds records isResent = do
      let Assignment {..} = subAssignment
      -- if current send is not a resent, insert record related info into AckWindow
      mres <- atomically $ do
        if not isResent
        then do
          scs <- readTVar subShardContexts
          let SubscribeShardContext {sscAckWindow = AckWindow{..}} = scs HM.! logId
          batchNumMap <- readTVar awBatchNumMap
          let newBatchNumMap = Map.insert batchId (fromIntegral $ V.length records) batchNumMap
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
      case mres of
        Nothing -> do
          unless isResent $
            resetReadingOffset logId batchId
          return False
        Just (consumerName, streamSend) ->
          withMVar streamSend (\ss -> ss (StreamingFetchResponse records)) >>= \case
            Left err -> do
              Log.fatal $ "sendReceivedRecords failed: logId=" <> Log.buildInt logId <> ", batchId=" <> Log.buildInt batchId
                       <> ", num of records=" <> Log.buildInt (V.length shardRecordIds) <> "\n"
                       <> "will remove the consumer " <> Log.buildText consumerName <> ": " <> Log.buildString (show err)
              atomically $ invalidConsumer subCtx consumerName
              unless isResent $ do
                resetReadingOffset logId batchId
              return False
            Right _ -> do
              Log.debug $ "send records from " <> Log.buildInt logId <> " to consumer " <> Log.buildText consumerName
                       <> ", batchId=" <> Log.buildInt batchId <> ", num of records=" <> Log.buildInt (V.length shardRecordIds)
              registerResend logId batchId shardRecordIds
              return True

    registerResend logId batchId recordIds = atomically $ do
      batchIndexes <- newTVar $ Set.fromList $ fmap sriBatchIndex (V.toList recordIds)
      currentTime <- readTVar subCurrentTime
      checkList <- readTVar subWaitingCheckedRecordIds
      checkListIndex <- readTVar subWaitingCheckedRecordIdsIndex
      let checkedRecordIds = CheckedRecordIds {
                              crDeadline =  currentTime + fromIntegral (subAckTimeoutSeconds * 10),
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
        Log.debug $ "There are " <> Log.buildInt (V.length resendRecordIds) <> " records need to resent"
                 <> ", batchId=" <> Log.buildInt batchId
        dataRecord <- withMVar subLdReader $ \reader -> do
          S.readerStartReading reader logId batchId batchId
          S.readerRead reader 1
        if length dataRecord /= 1
        then do
          -- TODO: handle error here
          Log.fatal . Log.buildString $ "read error"
        else do
          let (_, _, _, records) = decodeRecordBatch $ head dataRecord
              resendRecords =
                V.foldl'
                  (\a ShardRecordId {..} ->
                    V.snoc a (records V.! fromIntegral sriBatchIndex)
                  )
                  V.empty
                  resendRecordIds
          void $ sendReceivedRecords logId batchId resendRecordIds resendRecords True

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
      streamRecv >>= \case
        Left (err :: grpcIOError) -> do
          Log.fatal . Log.buildString $ "streamRecv error: " <> show err
          -- invalid consumer
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO GRPCStreamRecvError
        Right Nothing -> do
          -- This means that the consumer finished sending acks actively.
          atomically $ invalidConsumer subCtx ccConsumerName
          throwIO GRPCStreamRecvCloseError
        Right (Just StreamingFetchRequest {..}) -> do
          unless (V.null streamingFetchRequestAckIds) $
            doAcks scLDClient subCtx streamingFetchRequestAckIds
          loop

    -- throw error when check can not pass
    checkSubRunning :: IO ()
    checkSubRunning = do
      state <- readTVarIO subState
      if state /= SubscribeStateRunning
      then throwIO SubscribeInValidError
      else do
        isValid <- readTVarIO ccIsValid
        if isValid
        then return ()
        else throwIO ConsumerInValidError

doAcks
  :: S.LDClient
  -> SubscribeContext
  -> V.Vector RecordId
  -> IO ()
doAcks ldclient subCtx@SubscribeContext{..} ackRecordIds = do
  atomically $ do
    addUnackedRecords subCtx (- V.length ackRecordIds)
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
            let CheckedRecordIds {..}  = checkListIndex Map.! k
            batchIndexes <- readTVar crBatchIndexes
            let newBatchIndexes = Set.delete recordIdBatchIndex batchIndexes
            writeTVar crBatchIndexes newBatchIndexes
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
doAck ldclient SubscribeContext {..} logId recordIds= do
  res <- atomically $ do
    scs <- readTVar subShardContexts
    let SubscribeShardContext {sscAckWindow = AckWindow{..}} = scs HM.! logId
    lb <- readTVar awWindowLowerBound
    ub <- readTVar awWindowUpperBound
    ars <- readTVar awAckedRanges
    bnm <- readTVar awBatchNumMap
    let shardRecordIds = recordIds2ShardRecordIds recordIds
    let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b lb a bnm) ars shardRecordIds
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
        Log.info $ "[stream " <> Log.buildInt logId <> "] commit checkpoint = " <> Log.buildString (show lsn)
        S.writeCheckpoints subLdCkpReader (Map.singleton logId lsn)
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
--------------------------------------------------------------------------------
-- Exception and Exception Handlers
data SubscribeInnerError = GRPCStreamRecvError
                         | GRPCStreamRecvCloseError
                         | GRPCStreamSendError
                         | ConsumerInValidError
                         | SubscribeInValidError
  deriving (Show)
instance Exception SubscribeInnerError

newtype ConsumerExists = ConsumerExists T.Text
  deriving (Show)
instance Exception ConsumerExists

data SubscriptionOnDifferentNode = SubscriptionOnDifferentNode
  deriving (Show)
instance Exception SubscriptionOnDifferentNode

subscriptionExceptionHandler :: Handlers (StatusCode, StatusDetails)
subscriptionExceptionHandler = [
  Handler (\(err :: SubscriptionOnDifferentNode) -> do
    Log.warning $ Log.buildString' err
    return (StatusAborted, "Subscription is bound to a different node")),
  Handler (\(err :: Core.FoundActiveConsumers) -> do
    Log.warning $ Log.buildString' err
    return (StatusFailedPrecondition, "Subscription still has active consumers")),
  Handler(\(err@(ConsumerExists name) :: ConsumerExists) -> do
    Log.warning $ Log.buildString' err
    return (StatusInvalidArgument, StatusDetails ("Consumer " <> encodeUtf8 name <> " exist")))
  ]

subExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
subExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  subscriptionExceptionHandler ++ defaultHandlers

subStreamingExceptionHandle :: ExceptionHandle (ServerResponse 'BiDiStreaming a)
subStreamingExceptionHandle = mkExceptionHandle . setRespType (ServerBiDiResponse mempty) $
  innerErrorHandlers ++ subscriptionExceptionHandler ++ defaultHandlers

innerErrorHandlers :: Handlers (StatusCode, StatusDetails)
innerErrorHandlers = [Handler $ \(err :: SubscribeInnerError) -> case err of
  GRPCStreamRecvError      -> return (StatusCancelled, "Consumer recv error")
  GRPCStreamRecvCloseError -> return (StatusCancelled, "Consumer is closed")
  GRPCStreamSendError      -> return (StatusCancelled, "Consumer send request error")
  SubscribeInValidError    -> return (StatusAborted,   "Invalid Subscription")
  ConsumerInValidError     -> return (StatusAborted,   "Invalid Consumer")
  ]
