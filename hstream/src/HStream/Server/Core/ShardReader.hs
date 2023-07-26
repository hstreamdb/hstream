{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module HStream.Server.Core.ShardReader
  ( createShardReader
  , deleteShardReader
  , listShardReaders
  , readShard
  , readShardStream
  , readStream
  , readSingleShardStream
  )
where

import           Data.Functor               ((<&>))
import           ZooKeeper.Exception        (ZNONODE (..), throwIO)

import           Control.Concurrent         (modifyMVar_, newEmptyMVar, putMVar,
                                             readMVar, takeMVar, withMVar)
import           Control.Exception          (bracket, catch)
import           Control.Monad              (forM, forM_, unless, when, foldM)
import           Data.ByteString            (ByteString)
import           Data.Either                (isRight)
import qualified Data.Foldable              as F
import qualified Data.HashMap.Strict        as HM
import Data.Set (Set)
import qualified Data.Set as Set
import           Data.Int                   (Int64)
import           Data.IORef                 (IORef, newIORef, readIORef,
                                             writeIORef)
import qualified Data.Map.Strict            as M
import           Data.Maybe                 (isJust, fromJust, isNothing)
import qualified Data.Text                  as T
import           Data.Vector                (Vector)
import qualified Data.Vector                as V
import           Data.Word                  (Word64)
import           GHC.Stack                  (HasCallStack)
import           HsGrpc.Server              (whileM)
import qualified HStream.Exception          as HE
import qualified HStream.Logger             as Log
import qualified HStream.MetaStore.Types    as M
import           HStream.Server.Core.Common (decodeRecordBatch)
import           HStream.Server.HStreamApi  (CreateShardReaderRequest (..))
import qualified HStream.Server.HStreamApi  as API
import qualified HStream.Server.MetaData    as P
import           HStream.Server.Types       (ServerContext (..),
                                             ServerInternalOffset,
                                             ShardReader (..),
                                             StreamReader (..), ToOffset (..),
                                             getLogLSN, mkShardReader,
                                             mkStreamReader, transToStreamName, BiStreamReaderSender, BiStreamReaderReceiver, BiStreamReader (..))
import qualified HStream.Store              as S
import Control.Concurrent.STM (atomically, TBQueue, isEmptyTBQueue, newTBQueueIO, writeTBQueue, readTBQueue)
import HStream.Server.Shard (shardStartKey, cBytesToKey, hashShardKey)
import HStream.Utils (getRecordKey, decompressBatchedRecord)
import Control.Concurrent.Async (async, poll, cancel)

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
         return $ mkShardReader reader readerShardId Nothing startTimestamp Nothing

   putReader reader = do
    withMVar shardReaderMap $ \mp -> do
      case HM.lookup readShardRequestReaderId mp of
        Nothing         -> pure ()
        Just readerMvar -> putMVar readerMvar reader

   readRecords ShardReader{..} = do
     records <- S.readerRead shardReader (fromIntegral readShardRequestMaxRecords)
     let (records', _) = filterRecords shardReaderStartTs shardReaderEndTs records
     receivedRecordsVecs <- forM records' decodeRecordBatch
     let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
     Log.debug $ "reader " <> Log.build readShardRequestReaderId
              <> " read " <> Log.build (V.length res) <> " batchRecords"
     return res

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
   streamId = transToStreamName rStreamName

   createReader = do
     shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
     reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
     totalBatches <- if rMaxBatches == 0 then return Nothing else Just <$> newIORef rMaxBatches
     S.readerSetTimeout reader 60000
     S.readerSetWaitOnlyWhenNoData reader
     tsMapList <- forM shards $ \shard -> do
       (sTimestamp, eTimestamp) <- startReadingShard scLDClient reader rReaderId shard (toOffset <$> rStart) (toOffset <$> rEnd)
       return (shard, (sTimestamp, eTimestamp))
     let mp = HM.fromList tsMapList
     return $ mkStreamReader reader totalBatches mp

   deleteReader StreamReader{..} = do
     let shards = HM.keys streamReaderTsLimits
     forM_ shards $ \shard -> do
       isReading <- S.readerIsReading streamReader shard
       when isReading $ S.readerStopReading streamReader shard
     Log.info $ "shard reader " <> Log.build rReaderId <> " stop reading"

   readRecords s@StreamReader{..} = do
     whileM $ do
       records <- S.readerRead streamReader (fromIntegral maxReadBatch)
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
                                          , readShardStreamRequestShardId        = rShardId
                                          , readShardStreamRequestFrom           = rStart
                                          , readShardStreamRequestMaxReadBatches = rMaxBatches
                                          , readShardStreamRequestUntil          = rEnd
                                          }
                streamWrite = do
    readShardStream' sc rReaderId rShardId rStart rEnd rMaxBatches streamWrite

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
    let streamId = transToStreamName rStreamName
    shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
    when (length shards /= 1) $ throwIO $ HE.TooManyShardCount $ "Stream " <> show rStreamName <> " has more than one shard"
    readShardStream' sc rReaderId (head shards) rStart rEnd rMaxBatches streamWrite

readStreamWithKey
  :: HasCallStack
  => ServerContext
  -> BiStreamReaderSender
  -> BiStreamReaderReceiver
  -> IO ()
readStreamWithKey ServerContext{..} streamWriter streamReader = 
  bracket createReader deleteReader readRecords
 where
   ldReaderBufferSize = 10
   maxReadBatch = 10

   createReader = do
     streamReader >>= \case
       Right (Just API.ReadStreamWithKeyRequest{..}) -> do
         let streamId = transToStreamName readStreamWithKeyRequestStreamName
         streamExist <- S.doesStreamExist scLDClient streamId
         unless streamExist $ throwIO $ HE.StreamNotFound $ "Stream " <> T.pack (show readStreamWithKeyRequestStreamName) <> " is not exist."

         shardId <- getShardId scLDClient streamId readStreamWithKeyRequestKey
         reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
         Log.info $ "Create shardReader " <> Log.build readStreamWithKeyRequestReaderId
         -- Logdevice reader block at most 30s even when no more data to read
         S.readerSetTimeout reader 30000
         S.readerSetWaitOnlyWhenNoData reader
         (sTimestamp, eTimestamp) <- startReadingShard scLDClient reader readStreamWithKeyRequestReaderId shardId (toOffset <$> readStreamWithKeyRequestFrom) (toOffset <$> readStreamWithKeyRequestUntil)

         fetchChan <- newTBQueueIO 1
         atomically $ writeTBQueue fetchChan $ Just readStreamWithKeyRequestReadRecordCount
         return $ BiStreamReader { biStreamReader             = reader
                                 , biStreamReaderId           = readStreamWithKeyRequestReaderId
                                 , biStreamReaderTargetStream = readStreamWithKeyRequestStreamName
                                 , bistreamReaderTargetShard  = shardId
                                 , biStreamReaderTargetKey    = readStreamWithKeyRequestKey
                                 , biStreamReaderStartTs      = sTimestamp
                                 , biStreamReaderEndTs        = eTimestamp
                                 , fetchChan                  = fetchChan
                                 , biStreamReaderSender       = streamWriter
                                 , biStreamReaderReceiver     = streamReader
                                 }
       Left _        -> throwIO $ HE.StreamReadError "Consumer recv error"
       Right Nothing -> throwIO $ HE.StreamReadClose "Consumer is closed"

   deleteReader BiStreamReader{..} = do
     Log.info $ "shard reader " <> Log.build biStreamReaderId <> " stop reading"
     isReading <- S.readerIsReadingAny biStreamReader
     when isReading $ S.readerStopReading biStreamReader bistreamReaderTargetShard

   readRecords :: BiStreamReader -> IO ()
   readRecords s@BiStreamReader{..} = do
     clientThread <- async $ handleClientRequest biStreamReaderReceiver fetchChan
     whileM $ do
       atomically (readTBQueue fetchChan) >>= \case
         Nothing -> do Log.info $ "BiStreamReader " <> Log.build biStreamReaderId <> " stop read process"
                       return False
         Just cnt -> readLoop s cnt
     clientThreadRunning <- isNothing <$> poll clientThread
     when clientThreadRunning $ cancel clientThread

   readLoop :: BiStreamReader -> Word64 -> IO Bool
   readLoop s@BiStreamReader{..} cnt = do
     records <- S.readerRead biStreamReader maxReadBatch
     if null records 
       then do
         isReading <- S.readerIsReadingAny biStreamReader
         if isReading then readLoop s cnt 
                      else do Log.fatal $ "BiStreamReader " <> Log.build biStreamReaderId 
                                       <> " stop reading stream " <> Log.build biStreamReaderTargetStream
                                       <> ", shard " <> Log.build bistreamReaderTargetShard
                                       <> " unexpectedly" 
                              return False
       else do 
         successSends <- sendRecords s records cnt
         if V.all id successSends
           then readLoop s (cnt - (fromIntegral . V.length $ successSends))
           else return False

   sendRecords BiStreamReader{..} records cnt = do
     res <- getResponseRecords biStreamReader bistreamReaderTargetShard records biStreamReaderId biStreamReaderStartTs biStreamReaderEndTs
     let res' = V.take (fromIntegral cnt) $ V.map (filterReceivedRecordWithKey biStreamReaderTargetKey) res
     V.forM res' $ \(readStreamWithKeyResponseRecordIds, readStreamWithKeyResponseReceivedRecords) -> do
       biStreamReaderSender API.ReadStreamWithKeyResponse{..} >>= \case
         Left err -> do
           Log.fatal $ "BiStreamReader " <> Log.build biStreamReaderId <> " send records failed: \n\trecords="
                    <> Log.build (show readStreamWithKeyResponseRecordIds)
                    <> "\n\tnum of records=" <> Log.build (V.length readStreamWithKeyResponseRecordIds)
                    <> "\n\terror: " <> Log.build (show err)
           return False
         Right _ -> do
           Log.debug $ "BiStreamReader " <> Log.build biStreamReaderId <> " send " 
                    <> Log.build (show . V.length $ readStreamWithKeyResponseRecordIds) <> " records."
           return True

   handleClientRequest :: BiStreamReaderReceiver -> TBQueue (Maybe Word64) -> IO ()
   handleClientRequest streamRecv chan = whileM $ do
     streamRecv >>= \case
       Left (err :: grpcIOError) -> do
         -- Log.fatal $ "Consumer " <> Log.build ccConsumerName <> " for sub " <> Log.build subSubscriptionId
         --          <> " trigger a streamRecv error: " <> Log.build (show err)
         -- Log.warning $ "Sub " <> Log.build subSubscriptionId <> " invalided consumer " <> Log.build ccConsumerName
         atomically $ writeTBQueue chan Nothing
         return False
         -- throwIO $ HE.StreamReadError "Consumer recv error"
       Right Nothing -> do
         -- Log.info $ "Consumer " <> Log.build ccConsumerName <> " finished ack-sending stream to sub " <> Log.build subSubscriptionId
         -- -- This means that the consumer finished sending acks actively.
         -- Log.warning $ "Sub " <> Log.build subSubscriptionId <> " invalided consumer " <> Log.build ccConsumerName
         atomically $ writeTBQueue chan Nothing
         return False
         -- throwIO $ HE.StreamReadClose "Consumer is closed"
       Right (Just API.ReadStreamWithKeyRequest{..}) -> do
         -- Log.debug $ "Sub " <> Log.build subSubscriptionId <> " receive " <> Log.build (V.length streamingFetchRequestAckIds)
         --   <> " acks from consumer:" <> Log.build ccConsumerName
         atomically $ do
           isEmpty <- isEmptyTBQueue chan
           when isEmpty $ writeTBQueue chan $ Just readStreamWithKeyRequestReadRecordCount
         return True

   filterReceivedRecordWithKey :: T.Text -> API.ReceivedRecord -> (V.Vector API.RecordId, V.Vector API.HStreamRecord)
   filterReceivedRecordWithKey key API.ReceivedRecord{..} = 
     let batchedRecord = fromJust receivedRecordRecord
      in V.unzip . V.filter (\(_, record) -> filterHStreamRecords key record) $ V.zip receivedRecordRecordIds (decompressBatchedRecord batchedRecord)

   filterHStreamRecords :: T.Text -> API.HStreamRecord -> Bool
   filterHStreamRecords key record = getRecordKey record == key

----------------------------------------------------------------------------------------------------------------------------------
-- helper

readShardStream'
  :: (HasCallStack, StreamSend s)
  => ServerContext
  -> T.Text
  -> S.C_LogID
  -> Maybe API.ShardOffset
  -> Maybe API.ShardOffset
  -> Word64
  -> (s -> IO (Either String ()))
  -- ^ Stream write function
  -> IO ()
readShardStream' ServerContext{..} rReaderId rShardId rStart rEnd rMaxBatches streamWrite = do
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
     return $ mkShardReader reader rShardId totalBatches sTimestamp eTimestamp

   deleteReader ShardReader{..} = do
     Log.info $ "shard reader " <> Log.build rReaderId <> " stop reading"
     isReading <- S.readerIsReadingAny shardReader
     when isReading $ S.readerStopReading shardReader rShardId

   readRecords s@ShardReader{..} = do
     whileM $ do
       records <- S.readerRead shardReader (fromIntegral maxReadBatch)
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
getShardId scLDClient streamId orderingKey = do
  getShardDict <&> snd . fromJust . M.lookupLE shardKey
 where
   shardKey   = hashShardKey orderingKey

   getShardDict = do
     -- loading shard infomation for stream first.
     shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
     shardDict <- foldM insertShardDict M.empty shards
     Log.debug $ "build shardDict for stream " <> Log.build (show streamId) <> ": " <> Log.buildString' (show shardDict)
     return shardDict

   insertShardDict dict shardId = do
     attrs <- S.getStreamPartitionExtraAttrs scLDClient shardId
     Log.debug $ "attrs for shard " <> Log.build shardId <> ": " <> Log.buildString' (show attrs)
     startKey <- case M.lookup shardStartKey attrs of
        -- FIXME: Under the new shard model, each partition created should have an extrAttr attribute,
        -- except for the default partition created by default for each stream. After the default
        -- partition is subsequently removed, an error ShardKeyNotFound should be returned here.
        Nothing  -> return $ cBytesToKey "0"
        Just key -> return $ cBytesToKey key
     return $ M.insert startKey shardId dict
