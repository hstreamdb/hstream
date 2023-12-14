{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module HStream.Server.Core.ShardReader
  ( createShardReader
  , deleteShardReader
  , listShardReaders
  , readShard
  , readShardStream
  , readStream
  , readSingleShardStream
  , readStreamByKey
  )
where

import           Data.Functor                ((<&>))
import           ZooKeeper.Exception         (ZNONODE (..))

import           Control.Concurrent          (modifyMVar_, newEmptyMVar,
                                              putMVar, readMVar, takeMVar,
                                              withMVar)
import           Control.Exception           (bracket, catch, throwIO, try)
import           Control.Monad               (forM, forM_, join, unless, when)
import           Data.ByteString             (ByteString)
import qualified Data.ByteString             as BS
import           Data.Either                 (isRight)
import qualified Data.Foldable               as F
import qualified Data.HashMap.Strict         as HM
import           Data.Int                    (Int64)
import           Data.IORef                  (IORef, atomicModifyIORef',
                                              newIORef, readIORef, writeIORef)
import qualified Data.Map.Strict             as M
import           Data.Maybe                  (catMaybes, fromJust, isJust)
import qualified Data.Text                   as T
import           Data.Vector                 (Vector)
import qualified Data.Vector                 as V
import           Data.Word                   (Word64)
import           GHC.Stack                   (HasCallStack)
import           HsGrpc.Server               (whileM)
import           HStream.Common.Server.Shard (shardEndKey, shardStartKey)
import           HStream.Common.Types
import qualified HStream.Exception           as HE
import qualified HStream.Logger              as Log
import qualified HStream.MetaStore.Types     as M
import           HStream.Server.Core.Common  (decodeRecordBatch)
import           HStream.Server.HStreamApi   (CreateShardReaderRequest (..))
import qualified HStream.Server.HStreamApi   as API
import qualified HStream.Server.MetaData     as P
import           HStream.Server.Types        (BiStreamReader (..),
                                              BiStreamReaderReceiver,
                                              BiStreamReaderSender,
                                              ServerContext (..),
                                              ServerInternalOffset,
                                              ShardReader (..),
                                              StreamReader (..), ToOffset (..),
                                              getLogLSN, mkShardReader,
                                              mkStreamReader)
import qualified HStream.Stats               as Stats
import qualified HStream.Store               as S
import           HStream.Utils               (decompressBatchedRecord,
                                              getPOSIXTime, getRecordKey,
                                              msecSince, textToCBytes)

createShardReader
  :: HasCallStack
  => ServerContext
  -> API.CreateShardReaderRequest
  -> IO API.CreateShardReaderResponse
createShardReader ServerContext{..} API.CreateShardReaderRequest{createShardReaderRequestStreamName=rStreamName,
    createShardReaderRequestShardId=rShardId, createShardReaderRequestShardOffset=rOffset, createShardReaderRequestTimeout=rTimeout,
    createShardReaderRequestReaderId=rId} = do
  exist <- M.checkMetaExists @P.ShardReaderMeta rId metaHandle
  when exist $ throwIO $ HE.ShardReaderExists $ "ShardReader with id " <> rId <> " already exists."
  shardExists <- S.logIdHasGroup scLDClient rShardId
  unless shardExists $ throwIO $ HE.ShardNotFound $ "Shard with id " <> T.pack (show rShardId) <> " is not found."
  (startLSN, timestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId False . toOffset) rOffset
  let shardReaderMeta = mkShardReaderMeta timestamp startLSN
  Log.info $ "create shardReader " <> Log.buildString' (show shardReaderMeta)

  M.insertMeta rId shardReaderMeta metaHandle
  return API.CreateShardReaderResponse
    { API.createShardReaderResponseStreamName  = rStreamName
    , API.createShardReaderResponseShardId     = rShardId
    , API.createShardReaderResponseShardOffset = rOffset
    , API.createShardReaderResponseReaderId    = rId
    , API.createShardReaderResponseTimeout     = rTimeout
    }
 where
   mkShardReaderMeta startTimestamp offset = P.ShardReaderMeta rStreamName rShardId offset rId rTimeout startTimestamp

deleteShardReader
  :: HasCallStack
  => ServerContext
  -> API.DeleteShardReaderRequest
  -> IO ()
deleteShardReader ServerContext{..} API.DeleteShardReaderRequest{..} = do
  isSuccess <- catch (M.deleteMeta @P.ShardReaderMeta deleteShardReaderRequestReaderId Nothing metaHandle >> return True) $
      \ (_ :: ZNONODE) -> return False
  modifyMVar_ shardReaderMap $ \mp -> do
    case HM.lookup deleteShardReaderRequestReaderId mp of
      Nothing -> return mp
      Just _  -> return (HM.delete deleteShardReaderRequestReaderId mp)
  unless isSuccess $ throwIO $ HE.EmptyShardReader "ShardReaderNotExists"

listShardReaders :: HasCallStack => ServerContext -> API.ListShardReadersRequest -> IO (V.Vector T.Text)
listShardReaders ServerContext{..} _req = M.listMeta @P.ShardReaderMeta metaHandle <&> V.fromList . map P.readerReaderId

readShard
  :: HasCallStack
  => ServerContext
  -> API.ReadShardRequest
  -> IO (V.Vector API.ReceivedRecord)
readShard ServerContext{..} API.ReadShardRequest{..} = do
  bracket getReader putReader readRecords
 where
   ldReaderBufferSize = 10

   getReader = do
     mReader <- HM.lookup readShardRequestReaderId <$> readMVar shardReaderMap
     case mReader of
       Just readerMvar -> takeMVar readerMvar
       Nothing         -> do
         r@P.ShardReaderMeta{..} <- M.getMeta readShardRequestReaderId metaHandle >>= \case
           Nothing     -> throwIO $ HE.EmptyShardReader "ShardReaderNotExists"
           Just readerMeta -> return readerMeta
         Log.info $ "get reader from persistence: " <> Log.buildString' (show r)
         reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
         S.readerStartReading reader readerShardId readerShardOffset S.LSN_MAX
         S.readerSetTimeout reader (fromIntegral readerReadTimeout)
         readerMvar <- newEmptyMVar
         modifyMVar_ shardReaderMap $ return . HM.insert readShardRequestReaderId readerMvar
         return $ mkShardReader reader readerStreamName readerShardId Nothing startTimestamp Nothing

   putReader reader = do
    withMVar shardReaderMap $ \mp -> do
      case HM.lookup readShardRequestReaderId mp of
        Nothing         -> pure ()
        Just readerMvar -> putMVar readerMvar reader

   readRecords r@ShardReader{..} = do
     let cStreamName = textToCBytes targetStream
     !read_start <- getPOSIXTime
     -- records <- S.readerRead shardReader (fromIntegral readShardRequestMaxRecords)
     records <- readInternal r (fromIntegral readShardRequestMaxRecords)
     Stats.serverHistogramAdd scStatsHolder Stats.SHL_ReadLatency =<< msecSince read_start
     Stats.stream_stat_add_read_in_bytes scStatsHolder cStreamName (fromIntegral . sum $ map (BS.length . S.recordPayload) records)
     Stats.stream_stat_add_read_in_batches scStatsHolder cStreamName (fromIntegral $ length records)
     let (records', _) = filterRecords shardReaderStartTs shardReaderEndTs records
     receivedRecordsVecs <- forM records' decodeRecordBatch
     let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
     Log.debug $ "reader " <> Log.build readShardRequestReaderId
              <> " read " <> Log.build (V.length res) <> " batchRecords"
     return res

   readInternal r@ShardReader{..} maxRecords = do
     S.readerReadAllowGap @ByteString shardReader maxRecords >>= \case
       Left gap@S.GapRecord{..}
         | gapType == S.GapTypeAccess -> do
           Log.info $ "shardReader read stream " <> Log.build targetStream <> ", shard " <> Log.build targetShard <> " meet gap " <> Log.build (show gap)
           throwIO $ HE.AccessGapError $ "shardReader read stream " <> show targetStream <> ", shard " <> show targetShard <> " meet gap"
         | gapType == S.GapTypeNotInConfig -> do
           Log.info $ "shardReader read stream " <> Log.build targetStream <> ", shard " <> Log.build targetShard <> " meet gap " <> Log.build (show gap)
           throwIO $ HE.NotInConfigGapError $ "shardReader read stream " <> show targetStream <> ", shard " <> show targetShard <> " meet gap"
         | gapType == S.GapTypeUnknown -> do
           Log.warning $ "shardReader read stream " <> Log.build targetStream <> ", shard " <> Log.build targetShard <> " meet gap " <> Log.build (show gap)
           throwIO $ HE.UnknownGapError $ "shardReader read stream " <> show targetStream <> ", shard " <> show targetShard <> " meet gap"
         | gapType == S.GapTypeDataloss -> do
           Log.fatal $ "shardReader read stream " <> Log.build targetStream <> ", shard " <> Log.build targetShard <> " meet gap " <> Log.build (show gap)
           throwIO $ HE.DataLossGapError $ "shardReader read stream " <> show targetStream <> ", shard " <> show targetShard <> " meet gap"
         | otherwise -> readInternal r maxBound
       Right dataRecords -> return dataRecords

-----------------------------------------------------------------------------------------------------
--

readStream
  :: HasCallStack
  => ServerContext
  -> API.ReadStreamRequest
  -> (API.ReadStreamResponse -> IO (Either String ()))
  -- ^ Stream write function
  -> IO ()
readStream ServerContext{..}
                API.ReadStreamRequest{ readStreamRequestReaderId       = rReaderId
                                     , readStreamRequestStreamName     = rStreamName
                                     , readStreamRequestFrom           = rStart
                                     , readStreamRequestMaxReadBatches = rMaxBatches
                                     , readStreamRequestUntil          = rEnd
                                     }
                streamWrite = do
  bracket createReader deleteReader readRecords
 where
   ldReaderBufferSize = 10
   maxReadBatch = 10
   streamId = S.transToStreamName rStreamName

   createReader = do
     shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
     reader <- S.newLDReader scLDClient (fromIntegral . length $ shards) (Just ldReaderBufferSize)
     totalBatches <- if rMaxBatches == 0 then return Nothing else Just <$> newIORef rMaxBatches
     S.readerSetTimeout reader 60000
     S.readerSetWaitOnlyWhenNoData reader
     tsMapList <- forM shards $ \shard -> do
       try (startReadingShard scLDClient reader rReaderId shard (toOffset <$> rStart) (toOffset <$> rEnd)) >>= \case
         Right (sTimestamp, eTimestamp) -> return $ Just (shard, (sTimestamp, eTimestamp))
         Left (e :: HE.ConflictShardReaderOffset) -> do
           -- In the readStream scenario, for the same offset, some shards may not meet the requirement(startLSN <= endLSN).
           -- Therefore, when encountering the ConflictShardReaderOffset exception, skip this shard.
           Log.warning $ "skip read shard " <> Log.build (show shard) <> " for stream " <> Log.build (show rStreamName)
                      <> " because: "  <> Log.build (show e)
           return Nothing
     let mp = HM.fromList $ catMaybes tsMapList
     return $ mkStreamReader reader rStreamName totalBatches mp

   deleteReader StreamReader{..} = do
     let shards = HM.keys streamReaderTsLimits
     forM_ shards $ \shard -> do
       isReading <- S.readerIsReading streamReader shard
       when isReading $ S.readerStopReading streamReader shard
     Log.info $ "shard reader " <> Log.build rReaderId <> " stop reading"

   readRecords s@StreamReader{..} = do
     let cStreamName = textToCBytes streamReaderTargetStream
     whileM $ do
       !read_start <- getPOSIXTime
       records <- S.readerRead streamReader (fromIntegral maxReadBatch)
       Stats.serverHistogramAdd scStatsHolder Stats.SHL_ReadLatency =<< msecSince read_start
       Stats.stream_stat_add_read_in_bytes scStatsHolder cStreamName (fromIntegral . sum $ map (BS.length . S.recordPayload) records)
       Stats.stream_stat_add_read_in_batches scStatsHolder cStreamName (fromIntegral $ length records)
       if null records then S.readerIsReadingAny streamReader
                       else sendRecords s records
     Log.info $ "shard reader " <> Log.build rReaderId <> " read stream done."

   sendRecords StreamReader{..} records = do
     -- group records with same logId
     let groupRecords = F.foldr'
          (
            \r acc -> let logId = S.recordLogID r
                       in HM.insertWith (++) logId [r] acc
          )
          HM.empty
          records

     res <- V.concat . HM.elems <$> HM.traverseWithKey
       (
          \key value -> do
             case HM.lookup key streamReaderTsLimits of
               Just (startTs, endTs) -> do
                 getResponseRecords streamReader key value rReaderId startTs endTs
               Nothing -> return V.empty
       ) groupRecords

     Log.debug $ "stream reader " <> Log.build rReaderId <> " read " <> Log.build (V.length res) <> " batchRecords"
     isEnd <- S.readerIsReadingAny streamReader
     streamSend streamWrite rReaderId streamReaderTotalBatches isEnd res

readShardStream
  :: HasCallStack
  => ServerContext
  -> API.ReadShardStreamRequest
  -> (API.ReadShardStreamResponse -> IO (Either String ()))
  -- ^ Stream write function
  -> IO ()
readShardStream sc
                API.ReadShardStreamRequest{ readShardStreamRequestReaderId       = rReaderId
                                          , readShardStreamRequestStreamName     = rStreamName
                                          , readShardStreamRequestShardId        = rShardId
                                          , readShardStreamRequestFrom           = rStart
                                          , readShardStreamRequestMaxReadBatches = rMaxBatches
                                          , readShardStreamRequestUntil          = rEnd
                                          }
                streamWrite = do
    readShardStream' sc rReaderId rStreamName rShardId rStart rEnd rMaxBatches streamWrite

readSingleShardStream
  :: HasCallStack
  => ServerContext
  -> API.ReadSingleShardStreamRequest
  -> (API.ReadSingleShardStreamResponse -> IO (Either String ()))
  -- ^ Stream write function
  -> IO ()
readSingleShardStream sc@ServerContext{..}
                API.ReadSingleShardStreamRequest{ readSingleShardStreamRequestReaderId       = rReaderId
                                                , readSingleShardStreamRequestStreamName     = rStreamName
                                                , readSingleShardStreamRequestFrom           = rStart
                                                , readSingleShardStreamRequestMaxReadBatches = rMaxBatches
                                                , readSingleShardStreamRequestUntil          = rEnd
                                                }
                streamWrite = do
    let streamId = S.transToStreamName rStreamName
    shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
    when (length shards /= 1) $ throwIO $ HE.TooManyShardCount $ "Stream " <> show rStreamName <> " has more than one shard"
    readShardStream' sc rReaderId rStreamName (head shards) rStart rEnd rMaxBatches streamWrite

readStreamByKey
  :: HasCallStack
  => ServerContext
  -> BiStreamReaderSender
  -> BiStreamReaderReceiver
  -> IO ()
readStreamByKey ServerContext{..} streamWriter streamReader =
  bracket createReader deleteReader readRecords
 where
   ldReaderBufferSize = 10
   maxReadBatch = 5

   createReader = do
     streamReader >>= \case
       Right (Just req@API.ReadStreamByKeyRequest{..}) -> do
         Log.info $ "BiStreamReader receive first request from client " <> Log.build (show req)
         let streamId = S.transToStreamName readStreamByKeyRequestStreamName
         streamExist <- S.doesStreamExist scLDClient streamId
         unless streamExist $ do
           Log.info $ "Create biStreamReader error because stream " <> Log.build readStreamByKeyRequestStreamName <> "is not exist"
           throwIO $ HE.StreamNotFound $ "Stream " <> T.pack (show readStreamByKeyRequestStreamName) <> " is not exist."

         shardId <- getShardId scLDClient streamId readStreamByKeyRequestKey
         reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
         Log.info $ "Create shardReader " <> Log.build readStreamByKeyRequestReaderId
         -- Logdevice reader will blocked 2s when no data returned by store
         S.readerSetTimeout reader 2000
         S.readerSetWaitOnlyWhenNoData reader
         (sTimestamp, eTimestamp) <- startReading reader readStreamByKeyRequestReaderId shardId
                                      (toOffset <$> readStreamByKeyRequestFrom) (toOffset <$> readStreamByKeyRequestUntil)

         recordBuffer <- newIORef V.empty
         let biReader = BiStreamReader { biStreamReader             = reader
                                       , biStreamReaderId           = readStreamByKeyRequestReaderId
                                       , biStreamReaderTargetStream = readStreamByKeyRequestStreamName
                                       , bistreamReaderTargetShard  = shardId
                                       , biStreamReaderTargetKey    = readStreamByKeyRequestKey
                                       , biStreamReaderStartTs      = sTimestamp
                                       , biStreamReaderEndTs        = eTimestamp
                                       , biStreamReaderSender       = streamWriter
                                       , biStreamReaderReceiver     = streamReader
                                       , biStreamRecordBuffer       = recordBuffer
                                       }
         return (biReader, Just readStreamByKeyRequestReadRecordCount)
       Left _        -> throwIO $ HE.StreamReadError "BiStreamReader recv error"
       Right Nothing -> throwIO $ HE.StreamReadClose "Client is closed for bistreamReader"

   deleteReader (BiStreamReader{..}, _) = do
     isReading <- S.readerIsReadingAny biStreamReader
     when isReading $ S.readerStopReading biStreamReader bistreamReaderTargetShard
     Log.info $ "biStreamReader " <> Log.build biStreamReaderId <> " stop reading, destroy reader"

   readRecords :: (BiStreamReader, Maybe Word64) -> IO ()
   {- First round read, read from hstore directly -}
   readRecords (reader@BiStreamReader{..}, Just cnt) = do
     isSuccess <- readLoopInternal reader (fromIntegral cnt)
     when isSuccess $ do
       Log.info $ "BiStreamReader " <> Log.build biStreamReaderId
               <> " finish reading " <> Log.build (show cnt)
               <> " records from stream " <> Log.build biStreamReaderTargetStream
               <> ", shard " <> Log.build bistreamReaderTargetShard
       readRecords (reader, Nothing)
   {-
       - Later read loop. Waiting for the client to send the total number of messages for the next round of reads,
         then cyclically reading data from the store to deliver to the client.
       - Records that exceeds the total number of client requests is cached and prioritized for delivery in the
         next round of delivery.
       - Any failure to send causes the read loop to exit, and the biStreamReader will be destroyed
   -}
   readRecords (reader@BiStreamReader{..}, Nothing) = do
     whileM $ do
       -- check if until offset is reached before holding on next client request
       isReading <- S.readerIsReadingAny biStreamReader
       -- Note that all buffered records need to be delivered to client
       bufferNotEmpty <- not . V.null <$> readIORef biStreamRecordBuffer
       if isReading || bufferNotEmpty
         then readLoop reader
         else do
            Log.info $ "BiStreamReader " <> Log.build biStreamReaderId
                    <> " reached until offset for stream " <> Log.build biStreamReaderTargetStream
                    <> ", shard " <> Log.build bistreamReaderTargetShard
            return False
     Log.info $ "BiStreamReader " <> Log.build biStreamReaderId
             <> " read stream " <> Log.build biStreamReaderTargetStream
             <> ", shard " <> Log.build bistreamReaderTargetShard
             <> " done."

   readLoop :: BiStreamReader -> IO Bool
   readLoop reader@BiStreamReader{..} = do
     nextRoundReads <- handleClientRequest reader
     case nextRoundReads of
       Just cnt -> do
         sends <- sendCachedRecords reader cnt
         if V.all isJust sends
           then do
             let cnt' = fromIntegral cnt - (V.sum . V.catMaybes $ sends)
             isSuccess <- readLoopInternal reader cnt'
             when isSuccess $
               Log.info $ "BiStreamReader " <> Log.build biStreamReaderId
                       <> " finish reading " <> Log.build (show cnt)
                       <> " records from stream " <> Log.build biStreamReaderTargetStream
                       <> ", shard " <> Log.build bistreamReaderTargetShard
             return isSuccess
           else do
             Log.info $ "BiStreamReader " <> Log.build biStreamReaderId <> " exit read because send records to client failed."
             return False
       Nothing  -> do
         Log.info $ "BiStreamReader " <> Log.build biStreamReaderId <> " exit read because no more data request by client."
         return False

   readLoopInternal :: BiStreamReader -> Int -> IO Bool
   readLoopInternal reader@BiStreamReader{..} cnt
     | cnt == 0 = return True
     | otherwise = do
         !read_start <- getPOSIXTime
         records <- S.readerRead biStreamReader maxReadBatch
         Stats.serverHistogramAdd scStatsHolder Stats.SHL_ReadLatency =<< msecSince read_start
         let cStreamName = textToCBytes biStreamReaderTargetStream
         Stats.stream_stat_add_read_in_bytes scStatsHolder cStreamName (fromIntegral . sum $ map (BS.length . S.recordPayload) records)
         Stats.stream_stat_add_read_in_batches scStatsHolder cStreamName (fromIntegral $ length records)
         if null records
           then do
             -- check if until offset is reached.
             isReading <- S.readerIsReadingAny biStreamReader
             if isReading then readLoopInternal reader cnt
                          else do Log.info $ "BiStreamReader " <> Log.build biStreamReaderId
                                          <> " reached until offset for stream " <> Log.build biStreamReaderTargetStream
                                          <> ", shard " <> Log.build bistreamReaderTargetShard
                                  return False
           else do
             res <- getResponseRecords biStreamReader bistreamReaderTargetShard records biStreamReaderId biStreamReaderStartTs biStreamReaderEndTs
             let (res', remains) = V.splitAt cnt . join $ V.map (filterReceivedRecordByKey biStreamReaderTargetKey) res
             when (V.null res' && (not . V.null $ remains)) $ do
               Log.fatal "when res' == null, remains should be null too"
               throwIO $ HE.SomeServerInternal "Server internal error caused by biStreamReader"
             _ <- atomicModifyIORef' biStreamRecordBuffer $ \buffer -> (buffer <> remains, V.empty)
             successSends <- sendRecords reader res'
             if V.all isJust successSends
               then readLoopInternal reader (cnt - (V.sum . V.catMaybes $ successSends))
               else do
                 Log.info $ "BiStreamReader " <> Log.build biStreamReaderId <> " exit read because send records to client failed."
                 return False

   sendCachedRecords reader@BiStreamReader{..} cnt = do
     bufferedRecords <- atomicModifyIORef' biStreamRecordBuffer $ \buffer -> do
       let (sends, remains) = V.splitAt (fromIntegral cnt) buffer
        in (remains, sends)
     Log.debug $ "BiStreamReader " <> Log.build biStreamReaderId <> " will send " <> Log.build (show . V.length $ bufferedRecords) <> " records from cache."
     sendRecords reader bufferedRecords

   sendRecords BiStreamReader{..} records
     | V.null records = return $ V.singleton (Just 0)
     | otherwise = do
         -- gather all records with same shardId togather
         let records' = V.unzip <$> V.groupBy (\(id1, _) (id2, _) -> API.recordIdShardId id1 == API.recordIdShardId id2) records
         res <- forM records' $ \(readStreamByKeyResponseRecordIds, readStreamByKeyResponseReceivedRecords) -> do
            biStreamReaderSender API.ReadStreamByKeyResponse{..} >>= \case
              Left err -> do
                Log.fatal $ "BiStreamReader " <> Log.build biStreamReaderId <> " send records failed: \n\trecords="
                         <> Log.build (show readStreamByKeyResponseRecordIds)
                         <> "\n\tnum of records=" <> Log.build (V.length readStreamByKeyResponseRecordIds)
                         <> "\n\terror: " <> Log.build (show err)
                return Nothing
              Right _ -> do
                Log.debug $ "BiStreamReader " <> Log.build biStreamReaderId <> " send "
                         <> Log.build (show . V.length $ readStreamByKeyResponseRecordIds) <> " records."
                return $ Just (V.length readStreamByKeyResponseRecordIds)
         return $ V.fromList res

   handleClientRequest :: BiStreamReader -> IO (Maybe Word64)
   handleClientRequest BiStreamReader{..} =
     biStreamReaderReceiver >>= \case
       Left (err :: grpcIOError) -> do
         Log.fatal $ "BiStreamReader " <> Log.build biStreamReaderId <> " receive client error: " <> Log.build (show err)
         return Nothing
       Right Nothing -> do
         Log.info $ "No more data asked by client for biStreamReader: " <> Log.build biStreamReaderId
         return Nothing
       Right (Just API.ReadStreamByKeyRequest{..}) -> do
         Log.debug $ "Client ask " <> Log.build (show readStreamByKeyRequestReadRecordCount)
                  <> " records for bistreamReader " <> Log.build biStreamReaderId
         return $ Just readStreamByKeyRequestReadRecordCount

   filterReceivedRecordByKey :: T.Text -> API.ReceivedRecord -> V.Vector (API.RecordId, API.HStreamRecord)
   filterReceivedRecordByKey key API.ReceivedRecord{..} =
     let batchedRecord = fromJust receivedRecordRecord
      in V.filter (\(_, record) -> filterHStreamRecords key record) $ V.zip receivedRecordRecordIds (decompressBatchedRecord batchedRecord)

   filterHStreamRecords :: T.Text -> API.HStreamRecord -> Bool
   filterHStreamRecords key record = getRecordKey record == key

   startReading
     :: S.LDReader
     -> T.Text                      -- readerId, use for logging
     -> S.C_LogID
     -> Maybe ServerInternalOffset  -- startOffset
     -> Maybe ServerInternalOffset  -- endOffset
     -> IO (Maybe Int64, Maybe Int64)
   startReading reader readerId rShardId rStart rEnd = do
     (startLSN, sTimestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId False) rStart
     -- set default until LSN to tailLSN
     (endLSN,   eTimestamp) <- maybe ((, Nothing) <$> S.getTailLSN scLDClient rShardId) (getLogLSN scLDClient rShardId True) rEnd
     when (endLSN < startLSN) $
       throwIO . HE.ConflictShardReaderOffset $ "startLSN(" <> show startLSN <>") should less than and equal to endLSN(" <> show endLSN <> ")"
     -- Since the LSN obtained by timestamp is not accurate, for scenarios where the endLSN is determined using a timestamp,
     -- set the endLSN to LSN_MAX and do not rely on the underlying reader mechanism to determine the end of the read
     let endLSN' = if isJust eTimestamp then S.LSN_MAX else endLSN
     S.readerStartReading reader rShardId startLSN endLSN'
     Log.info $ "ShardReader " <> Log.build readerId <> " start reading shard " <> Log.build rShardId
             <> " from = " <> Log.build (show startLSN) <> ", to = " <> Log.build (show endLSN')
     return (sTimestamp, eTimestamp)

----------------------------------------------------------------------------------------------------------------------------------
-- helper

readShardStream'
  :: (HasCallStack, StreamSend s)
  => ServerContext
  -> T.Text
  -> T.Text
  -> S.C_LogID
  -> Maybe API.ShardOffset
  -> Maybe API.ShardOffset
  -> Word64
  -> (s -> IO (Either String ()))
  -- ^ Stream write function
  -> IO ()
readShardStream' ServerContext{..} rStreamName rReaderId rShardId rStart rEnd rMaxBatches streamWrite = do
  bracket createReader deleteReader readRecords
 where
   ldReaderBufferSize = 10
   maxReadBatch = 10

   createReader = do
     shardExists <- S.logIdHasGroup scLDClient rShardId
     unless shardExists $ throwIO $ HE.ShardNotFound $ "Shard with id " <> T.pack (show rShardId) <> " is not found."
     reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
     Log.info $ "Create shardReader " <> Log.build rReaderId
     -- When there is data, reader read will return immediately, the maximum number of returned data is maxReadBatch,
     -- If there is no data, it will wait up to 1min and return 0.
     -- Setting the timeout to 1min instead of infinite is to give us some information on whether
     -- the current reader is still alive or not.
     S.readerSetTimeout reader 60000
     S.readerSetWaitOnlyWhenNoData reader
     (sTimestamp, eTimestamp) <- startReadingShard scLDClient reader rReaderId rShardId (toOffset <$> rStart) (toOffset <$> rEnd)
     totalBatches <- if rMaxBatches == 0 then return Nothing else Just <$> newIORef rMaxBatches
     return $ mkShardReader reader rStreamName rShardId totalBatches sTimestamp eTimestamp

   deleteReader ShardReader{..} = do
     Log.info $ "shard reader " <> Log.build rReaderId <> " stop reading"
     isReading <- S.readerIsReadingAny shardReader
     when isReading $ S.readerStopReading shardReader rShardId

   readRecords s@ShardReader{..} = do
     let cStreamName = textToCBytes targetStream
     whileM $ do
       !read_start <- getPOSIXTime
       records <- S.readerRead shardReader (fromIntegral maxReadBatch)
       Stats.serverHistogramAdd scStatsHolder Stats.SHL_ReadLatency =<< msecSince read_start
       Stats.stream_stat_add_read_in_bytes scStatsHolder cStreamName (fromIntegral . sum $ map (BS.length . S.recordPayload) records)
       Stats.stream_stat_add_read_in_batches scStatsHolder cStreamName (fromIntegral $ length records)
       if null records then S.readerIsReadingAny shardReader
                       else sendRecords s records
     Log.info $ "shard reader " <> Log.build rReaderId <> " read stream done."

   sendRecords ShardReader{..} records = do
     res <- getResponseRecords shardReader targetShard records rReaderId shardReaderStartTs shardReaderEndTs
     isReading <- S.readerIsReadingAny shardReader
     streamSend streamWrite rReaderId shardReaderTotalBatches isReading res

class StreamSend s where
  convert :: Vector API.ReceivedRecord -> s
  streamSend :: (s -> IO (Either String ())) -> T.Text -> Maybe (IORef Word64) -> Bool -> Vector API.ReceivedRecord -> IO Bool
  streamSend streamWrite readerId totalCnt isReading res = case totalCnt of
    Just bs -> do
      remains <- fromIntegral <$> readIORef bs
      let diff = remains - V.length res
      if diff > 0
        then do
          writeIORef bs $ fromIntegral diff
          streamWrite (convert res) >>= return <$> (isReading &&) . isRight
        else do
          let res' = V.take remains res
          _ <- streamWrite (convert res')
          Log.info $ "shard reader " <> Log.build readerId <> " finish read max batches."
          return False
    Nothing -> streamWrite (convert res) >>= return <$> (isReading &&) . isRight

instance StreamSend API.ReadShardStreamResponse where
  convert = API.ReadShardStreamResponse

instance StreamSend API.ReadStreamResponse where
  convert = API.ReadStreamResponse

instance StreamSend API.ReadSingleShardStreamResponse where
  convert = API.ReadSingleShardStreamResponse

-- Removes data that is not in the specified timestamp range.
-- If endTimestamp is set, also check if endTimestamp has been reached
filterRecords :: Maybe Int64 -> Maybe Int64 -> [S.DataRecord ByteString] -> ([S.DataRecord ByteString], Bool)
filterRecords startTs endTs records =
  let
      records' = case startTs of
        Just startOffset -> filterRecordBeforeTimestamp records startOffset
        Nothing          -> records
      res = case endTs of
        Just endOffset -> filterRecordAfterTimestamp records' endOffset
        Nothing        -> (records, False)
   in res
 where
  -- Remove all values with timestamps less than the given timestamp
  filterRecordBeforeTimestamp :: [S.DataRecord ByteString] -> Int64 -> [S.DataRecord ByteString]
  filterRecordBeforeTimestamp [] _ = []
  filterRecordBeforeTimestamp rds timestamp =
    if (S.recordTimestamp . head $ records) > timestamp then records else dropWhile (\r -> S.recordTimestamp r < timestamp) rds

  -- Remove all values with timestamps greater than the given timestamp, return true if the recordTimestamp of the
  -- last records after filtering is equal to given timestamp
  filterRecordAfterTimestamp :: [S.DataRecord ByteString] -> Int64 -> ([S.DataRecord ByteString], Bool)
  filterRecordAfterTimestamp [] _ = ([], False)
  filterRecordAfterTimestamp rds timestamp =
     F.foldr'
       (
          \r (acc', isEnd) -> let tmp = S.recordTimestamp r
                               in if tmp <= timestamp then (r : acc', isEnd || tmp == timestamp) else (acc', True)
       )
       ([], False)
       rds

startReadingShard
  :: S.LDClient
  -> S.LDReader
  -> T.Text                      -- readerId, use for logging
  -> S.C_LogID
  -> Maybe ServerInternalOffset  -- startOffset
  -> Maybe ServerInternalOffset  -- endOffset
  -> IO (Maybe Int64, Maybe Int64)
startReadingShard scLDClient reader readerId rShardId rStart rEnd = do
  (startLSN, sTimestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId False) rStart
  (endLSN,   eTimestamp) <- maybe (return (S.LSN_MAX, Nothing)) (getLogLSN scLDClient rShardId True) rEnd
  when (endLSN < startLSN) $
    throwIO . HE.ConflictShardReaderOffset $ "startLSN(" <> show startLSN <>") should less than and equal to endLSN(" <> show endLSN <> ")"
  -- Since the LSN obtained by timestamp is not accurate, for scenarios where the endLSN is determined using a timestamp,
  -- set the endLSN to LSN_MAX and do not rely on the underlying reader mechanism to determine the end of the read
  let endLSN' = if isJust eTimestamp then S.LSN_MAX else endLSN
  S.readerStartReading reader rShardId startLSN endLSN'
  Log.info $ "ShardReader " <> Log.build readerId <> " start reading shard " <> Log.build rShardId
          <> " from = " <> Log.build (show startLSN) <> ", to = " <> Log.build (show endLSN')
  return (sTimestamp, eTimestamp)

getResponseRecords
  :: S.LDReader
  -> S.C_LogID
  -> [S.DataRecord ByteString]
  -> T.Text
  -> Maybe Int64
  -> Maybe Int64
  -> IO (Vector API.ReceivedRecord)
getResponseRecords reader shard records readerId startTs endTs = do
  let (records', isEnd) = filterRecords startTs endTs records
  receivedRecordsVecs <- forM records' decodeRecordBatch
  let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
  Log.debug $ "reader " <> Log.build readerId
           <> " read " <> Log.build (V.length res) <> " batchRecords"
  when isEnd $ do
    Log.info $ "stream reader "
            <> Log.build readerId <> " stop reading shard "
            <> Log.build shard <> " since reach end timestamp."
    S.readerStopReading reader shard
  return res

getShardId :: S.LDClient -> S.StreamId -> T.Text -> IO S.C_LogID
getShardId scLDClient streamId key =
  S.listStreamPartitions scLDClient streamId >>= lookupShard . M.elems
 where
   shardKey = hashShardKey key

   lookupShard []             = do
     throwIO $ HE.UnexpectedError $ "Can't find shard for key " <> show key <> " within streamId " <> show streamId
   lookupShard (shard:shards) = do
     attrs <- S.getStreamPartitionExtraAttrs scLDClient shard
     -- FIXME: Under the new shard model, each partition created should have an extrAttr attribute,
     -- except for the default partition created by default for each stream. After the default
     -- partition is subsequently removed, an error ShardKeyNotFound should be returned here.
     let startKey = maybe minBound cBytesToKey $ M.lookup shardStartKey attrs
     let endKey = maybe maxBound cBytesToKey $ M.lookup shardEndKey attrs
     if shardKey >= startKey && shardKey <= endKey
       then do
         Log.info $ "Find shard for key " <> Log.build key
                 <> ", hashedKey=" <> Log.build (show shardKey)
                 <> ", streamId=" <> Log.build (show streamId)
                 <> ", shardId=" <> Log.build (show shard)
                 <> ", shard startKey=" <> Log.build (show startKey)
                 <> ", shard endKey=" <> Log.build (show endKey)
         return shard
       else lookupShard shards
