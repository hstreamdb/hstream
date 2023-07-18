{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}

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
import           Control.Monad              (forM, forM_, unless, when)
import           Data.ByteString            (ByteString)
import           Data.Either                (isRight)
import qualified Data.Foldable              as F
import qualified Data.HashMap.Strict        as HM
import           Data.Int                   (Int64)
import           Data.IORef                 (IORef, newIORef, readIORef,
                                             writeIORef)
import qualified Data.Map.Strict            as M
import           Data.Maybe                 (isJust)
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
                                             mkStreamReader, transToStreamName)
import qualified HStream.Store              as S

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
  (startLSN, timestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId . toOffset) rOffset
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
  (startLSN, sTimestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId) rStart
  (endLSN,   eTimestamp) <- maybe (return (S.LSN_MAX, Nothing)) (getLogLSN scLDClient rShardId) rEnd
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

