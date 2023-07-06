{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Core.ShardReader
  ( createShardReader
  , deleteShardReader
  , listShardReaders
  , readShard
  , readShardStream
  )
where

import           Data.Functor               ((<&>))
import           Proto3.Suite               (Enumerated (Enumerated))
import           ZooKeeper.Exception        (ZNONODE (..), throwIO)

import           Control.Concurrent         (modifyMVar_, newEmptyMVar, putMVar,
                                             readMVar, takeMVar, withMVar)
import           Control.Exception          (bracket, catch)
import           Control.Monad              (forM, unless, when)
import           Data.ByteString            (ByteString)
import           Data.Either                (isRight)
import qualified Data.Foldable              as F
import qualified Data.HashMap.Strict        as HM
import           Data.Int                   (Int64)
import           Data.IORef                 (newIORef, readIORef, writeIORef)
import           Data.Maybe                 (fromJust, isJust)
import qualified Data.Text                  as T
import qualified Data.Vector                as V
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
                                             ShardReader (..), mkShardReader)
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
  -- (startLSN, timestamp) <- getLogLSN scLDClient rShardId rOffset
  (startLSN, timestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId) rOffset
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
         return $ mkShardReader reader Nothing startTimestamp Nothing

   putReader reader = do
    withMVar shardReaderMap $ \mp -> do
      case HM.lookup readShardRequestReaderId mp of
        Nothing         -> pure ()
        Just readerMvar -> putMVar readerMvar reader

   readRecords ShardReader{..} = do
     records <- S.readerRead reader (fromIntegral readShardRequestMaxRecords)
     let (records', _) = filterRecords startTs endTs records
     receivedRecordsVecs <- forM records' decodeRecordBatch
     let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
     Log.debug $ "reader " <> Log.build readShardRequestReaderId
              <> " read " <> Log.build (V.length res) <> " batchRecords"
     return res

-----------------------------------------------------------------------------------------------------

readShardStream
  :: HasCallStack
  => ServerContext
  -> API.ReadShardStreamRequest
  -> (API.ReadShardStreamResponse -> IO (Either String ()))
  -- ^ Stream write function
  -> IO ()
readShardStream ServerContext{..}
                API.ReadShardStreamRequest{ readShardStreamRequestReaderId       = rReaderId
                                          , readShardStreamRequestShardId        = rShardId
                                          , readShardStreamRequestFrom           = rStart
                                          , readShardStreamRequestMaxReadBatches = rMaxBatches
                                          , readShardStreamRequestUntil          = rEnd
                                          }
                streamWrite = do
  bracket createReader deleteReader readRecords
 where
   ldReaderBufferSize = 10
   maxReadBatch = 10

   createReader = do
     shardExists <- S.logIdHasGroup scLDClient rShardId
     unless shardExists $ throwIO $ HE.ShardNotFound $ "Shard with id " <> T.pack (show rShardId) <> " is not found."
     (startLSN, sTimestamp) <- maybe (return (S.LSN_MIN, Nothing)) (getLogLSN scLDClient rShardId) rStart
     (endLSN,   eTimestamp) <- maybe (return (S.LSN_MAX, Nothing)) (getLogLSN scLDClient rShardId) rEnd
     when (endLSN < startLSN) $ throwIO . HE.ConflictShardReaderOffset $ "startLSN(" <> show startLSN <>") should less than and equal to endLSN(" <> show endLSN <> ")"
     -- Since the LSN obtained by timestamp is not accurate, for scenarios where the endLSN is determined using a timestamp,
     -- set the endLSN to LSN_MAX and do not rely on the underlying reader mechanism to determine the end of the read
     let endLSN' = if isJust eTimestamp then S.LSN_MAX else endLSN
     reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
     S.readerStartReading reader rShardId startLSN endLSN'
     -- When there is data, reader read will return immediately, the maximum number of returned data is maxReadBatch,
     -- If there is no data, it will wait up to 1min and return 0.
     -- Setting the timeout to 1min instead of infinite is to give us some information on whether
     -- the current reader is still alive or not.
     S.readerSetTimeout reader 60000
     S.readerSetWaitOnlyWhenNoData reader
     Log.info $ "create shardReader for shard " <> Log.build rShardId <> " success, from = " <> Log.build (show startLSN) <> ", to = " <> Log.build (show endLSN')
     totalBatches <- if rMaxBatches == 0 then return Nothing else Just <$> newIORef rMaxBatches
     return $ mkShardReader reader totalBatches sTimestamp eTimestamp

   deleteReader ShardReader{..} = do
     Log.info $ "shard reader " <> Log.build rReaderId <> " stop reading"
     allStoped <- S.readerIsReadingAny reader
     unless allStoped $ S.readerStopReading reader rShardId

   readRecords s@ShardReader{..} = do
     whileM $ do
       records <- S.readerRead reader (fromIntegral maxReadBatch)
       if null records then S.readerIsReadingAny reader
                       else sendRecords s records
     Log.info $ "shard reader " <> Log.build rReaderId <> " read stream done."

   sendRecords ShardReader{..} records = do
     let (records', isEnd) = filterRecords startTs endTs records
     receivedRecordsVecs <- forM records' decodeRecordBatch
     let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
     Log.debug $ "reader " <> Log.build rReaderId
              <> " read " <> Log.build (V.length res) <> " batchRecords"

     when isEnd $ Log.info $ "shard reader " <> Log.build rReaderId <> " reach end timestamp, will finish reading."

     case totalBatches of
       Just bs -> do
         remains <- fromIntegral <$> readIORef bs
         let diff = remains - V.length res
         if diff > 0
           then do
             writeIORef bs $ fromIntegral diff
             streamWrite (API.ReadShardStreamResponse res) >>= return <$> (not isEnd &&) . isRight
           else do
             let res' = V.take remains res
             _ <- streamWrite (API.ReadShardStreamResponse res')
             Log.info $ "shard reader " <> Log.build rReaderId <> " finish read max batches."
             return False
       Nothing -> streamWrite (API.ReadShardStreamResponse res) >>= return <$> (not isEnd &&) . isRight

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
          \r acc@(acc', _) -> let tmp = S.recordTimestamp r
                               in if tmp <= timestamp then (r : acc', tmp == timestamp) else acc
       )
       ([], False)
       rds

-- if the offset is timestampOffset, then return (LSN, Just timestamp)
-- , otherwise return (LSN, Nothing)
getLogLSN :: S.LDClient -> S.C_LogID -> API.ShardOffset -> IO (S.LSN, Maybe Int64)
getLogLSN scLDClient logId offset =
  case fromJust . API.shardOffsetOffset $ offset of
    API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetEARLIEST)) ->
      return (S.LSN_MIN, Nothing)
    API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetLATEST)) -> do
      startLSN <- (+ 1) <$> S.getTailLSN scLDClient logId
      return (startLSN, Nothing)
    API.ShardOffsetOffsetRecordOffset API.RecordId{..} ->
      return (recordIdBatchId, Nothing)
    API.ShardOffsetOffsetTimestampOffset API.TimestampOffset{..} -> do
      let accuracy = if timestampOffsetStrictAccuracy then S.FindKeyStrict else S.FindKeyApproximate
      startLSN <- S.findTime scLDClient logId timestampOffsetTimestampInMs accuracy
      return (startLSN, Just timestampOffsetTimestampInMs)
    _ ->
      throwIO $ HE.InvalidShardOffset "UnKnownShardOffset"
