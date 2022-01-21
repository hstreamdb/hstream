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
import Control.Concurrent.Async (concurrently_)
import           Control.Exception                (displayException, throwIO, try, SomeException, Exception)
import           Control.Monad                    (when, zipWithM, unless)
import qualified Data.ByteString.Char8            as BS
import           Data.Function                    (on)
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (modifyIORef', newIORef,
                                                   readIORef, writeIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import qualified Data.Set as Set
import           Data.Maybe                       (catMaybes, fromJust)
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
                                                   getStartRecordId,
                                                   getSuccessor,
                                                   insertAckedRecordId)
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
      Log.debug $ "Try to create a subscription to a nonexistent stream"
               <> "Stream Name: "
               <> Log.buildString (show streamName)
      returnErrResp StatusFailedPrecondition . StatusDetails $ "stream " <> encodeUtf8 subscriptionStreamName <> " not exist"
    else do
      logId <- S.getUnderlyingLogId scLDClient (transToStreamName subscriptionStreamName) Nothing
      offset <- convertOffsetToRecordId logId

      P.checkIfExist @ZHandle @'SubRep
        subscriptionSubscriptionId zkHandle >>= \case
        True  -> returnErrResp StatusAlreadyExists . StatusDetails $ "Subsctiption "
                                                                  <> encodeUtf8 subscriptionSubscriptionId
                                                                  <> " already exists"
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
  (subInfoMVar, isInited) <- modifyMVar scSubscribeRuntimeInfo 
    ( \infoMap -> do 
        case HM.lookup watchSubscriptionRequestSubscriptionId infoMap of
          Nothing -> do
            let subInfo = newSubscriptionRuntimeInfo watchSubscriptionRequestSubscriptionId 
            subInfoMVar <- newMVar subInfo
            return (HM.insert watchSubscriptionRequestSubscriptionId subInfoMVar infoMap, (subInfoMVar, False))
          Just subInfoMVar -> return (infoMap, (subInfoMVar, True)) 
    ) 

  unless isInited 
    (watchStreamShardsForSubscription ctx watchSubscriptionRequestSubscriptionId)

  modifyMVar_ subInfoMVar
    (
      \subInfo@SubscribeRuntimeInfo{..} -> 
        let
          consumerWatch = ConsumerWatch {cwConsumerName = watchSubscriptionRequestConsumerName, cwWatchStream = streamSend}
        in
          return subInfo {sriWaitingConsumers = sriWaitingConsumers ++ [consumerWatch]} 
    )

  blockUntilConsumerDown
  where
    blockUntilConsumerDown :: IO (ServerResponse 'ServerStreaming WatchSubscriptionResponse)
    blockUntilConsumerDown = undefined

--------------------------------------------------------------------------------
-- find the stream according to the subscriptionId,
-- and watch stream shards in zk.
-- when there is a new shard, assign it for Reading.
watchStreamShardsForSubscription :: ServerContext -> T.Text -> IO () 
watchStreamShardsForSubscription = undefined 
--------------------------------------------------------------------------------

-- find a consumer for the orderingKey and push the notification to the consumer to
-- ask it to do streamingFetch. 
assignShardForReading :: MVar SubscribeRuntimeInfo -> OrderingKey -> IO ()     
assignShardForReading infoMVar orderingKey = 
  modifyMVar_ infoMVar
    (
      \info@SubscribeRuntimeInfo{..} -> 
        if null sriWaitingConsumers 
        then
          if Set.null sriWorkingConsumers
          then return info 
          else do
            let (minConsumerWorkload@ConsumerWorkload{..}, leftSet) = Set.deleteFindMin sriWorkingConsumers
            -- push SubscriptionAdd to the choosed consumer
            let ConsumerWatch {..} = cwConsumerWatch
            pushAdd cwWatchStream
            let newSet = Set.insert (minConsumerWorkload {cwShardCount = cwShardCount + 1}) leftSet
            return info {sriWorkingConsumers = newSet}
        else do 
          -- 1. choose the first consumer in waiting list
          let consumer@ConsumerWatch{..} = head sriWaitingConsumers
          -- 2. push SubscriptionAdd to the choosed consumer
          pushAdd cwWatchStream
          cwWatchStream WatchSubscriptionResponse {} 
          -- 3. remove the consumer from the waiting list and add it to the workingList 
          let newWaitingList = drop 1 sriWaitingConsumers
          let newWorkingSet = Set.insert (ConsumerWorkload {cwConsumerWatch = consumer, cwShardCount = 1}) sriWorkingConsumers 
          return info {sriWaitingConsumers = newWaitingList, sriWorkingConsumers = newWorkingSet}
    )
    where
      pushAdd :: StreamSend WatchSubscriptionResponse -> IO ()
      pushAdd = undefined
    
--------------------------------------------------------------------------------

-- for each subscription In serverContext, 
-- check if there are shards which is not assigned,
-- then run assingShardForReading.
--
-- this should be run in a backgroud thread and start before grpc server
-- started.
scanSubscriptionsForAssignment :: IO ()
scanSubscriptionsForAssignment = undefined 


--------------------------------------------------------------------------------
newSubscriptionRuntimeInfo :: T.Text -> SubscribeRuntimeInfo 
newSubscriptionRuntimeInfo subId = 
  SubscribeRuntimeInfo {
    sriSubscriptionId = subId
  , sriWaitingConsumers = []
  , sriAssignments = HM.empty
  , sriShardRuntimeInfo = HM.empty 
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
  where
    getSubInfoMap :: IO (MVar SubscribeRuntimeInfo)
    getSubInfoMap = undefined 

    dumbShardInfo :: MVar ShardSubscribeRuntimeInfo
    dumbShardInfo = undefined

    initShardRuntimeInfo :: StreamingFetchRequest -> IO (MVar ShardSubscribeRuntimeInfo) 
    initShardRuntimeInfo StreamingFetchRequest{..} = do
      -- subInfoMVar <- withMVar scSubscribeRuntimeInfo
      --   (
      --     \ infoMap -> return (HM.! infoMap streamingFetchRequestSubscriptionId)
      --   )
      subInfoMVar <- getSubInfoMap 

      modifyMVar subInfoMVar
        (
          \subInfo@SubscribeRuntimeInfo{..} -> 
            if HM.member streamingFetchRequestOrderingKey sriShardRuntimeInfo 
            --then return (subInfo, (HM.! sriShardRuntimeInfo streamingFetchRequestOrderingKey)) 
            then return (subInfo, dumbShardInfo) 
            else do
              -- get subscription info from zk
              P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
                Nothing -> do
                  Log.debug $ "streamingFetch error because subscription " <> Log.buildText streamingFetchRequestSubscriptionId <> " not exist."
                  throwIO $ SubscriptionIdNotFound streamingFetchRequestSubscriptionId
                Just sub@Subscription {..} -> do
                  let startRecordId = getStartRecordId sub 
                  -- create a ldCkpReader for reading new records
                  let readerName = textToCBytes (streamingFetchRequestSubscriptionId `T.append` "-" `T.append` streamingFetchRequestOrderingKey)
                  ldCkpReader <-
                    S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 1 Nothing 10
                  -- seek ldCkpReader to start offset
                  logId <- S.getUnderlyingLogId scLDClient (transToStreamName subscriptionStreamName) Nothing
                  let startLSN = recordIdBatchId startRecordId
                  S.startReadingFromCheckpointOrStart ldCkpReader logId (Just startLSN) S.LSN_MAX
                  -- set ldCkpReader timeout to 0
                  _ <- S.ckpReaderSetTimeout ldCkpReader 0
                  Log.debug $ "created a ldCkpReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "} with startLSN {" <> Log.buildInt startLSN <> "}"

                  -- create a ldReader for rereading unacked records
                  ldReader <- S.newLDReader scLDClient 1 Nothing
                  Log.debug $ "created a ldReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

                  -- init SubscribeRuntimeInfo
                  let shardInfo =
                        ShardSubscribeRuntimeInfo {
                            ssriStreamName = subscriptionStreamName 
                          , ssriLogId = logId 
                          , ssriAckTimeoutSeconds = subscriptionAckTimeoutSeconds  
                          , ssriLdCkpReader       = ldCkpReader 
                          , ssriLdReader          = ldReader 
                          , ssriWindowLowerBound  = startRecordId 
                          , ssriWindowUpperBound  = maxBound  
                          , ssriAckedRanges       = Map.empty  
                          , ssriBatchNumMap       = Map.empty  
                          , ssriConsumerName      = streamingFetchRequestConsumerName 
                          , ssriStreamSend        = streamSend  
                          , ssriValid             = True 
                        }
                  shardInfoMVar <- newMVar shardInfo 
                  let newShardMap = HM.insert streamingFetchRequestOrderingKey shardInfoMVar sriShardRuntimeInfo 
                  return (subInfo {sriShardRuntimeInfo = newShardMap}, shardInfoMVar)
        )

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
    Nothing -> genRecordStream ctx shardInfoMVar
    Just records -> do
      doSend records
      let recordIds = V.map (fromJust . receivedRecordRecordId) records 
      registerResend recordIds 
      genRecordStream ctx shardInfoMVar
  where
    check = 
      withMVar shardInfoMVar
        (
          \ ShardSubscribeRuntimeInfo {..} -> 
            if ssriValid
            then return ()
            else throwIO ConsumerInValidError
        )
      
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
        if ssriValid
          then do
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
          else throwIO ConsumerInValidError
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













