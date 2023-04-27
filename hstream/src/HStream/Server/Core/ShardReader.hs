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
import           Data.IORef
import           Proto3.Suite               (Enumerated (Enumerated))
import           ZooKeeper.Exception        (ZNONODE (..), throwIO)

import           Control.Concurrent         (modifyMVar_, newEmptyMVar, putMVar,
                                             readMVar, takeMVar, withMVar)
import           Control.Exception          (bracket, catch)
import           Control.Monad              (forM, unless, when)
import           Data.ByteString            (ByteString)
import           Data.Either                (isRight)
import qualified Data.HashMap.Strict        as HM
import           Data.Int                   (Int64)
import           Data.Maybe                 (fromJust)
import qualified Data.Text                  as T
import qualified Data.Vector                as V
import           GHC.Stack                  (HasCallStack)
import           HsGrpc.Server              (OutStream,
                                             StreamOutput (streamWrite), whileM)
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
  (startLSN, timestamp) <- getStartLSN scLDClient rShardId rOffset
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
         return $ mkShardReader reader startTimestamp

   putReader reader = do
    withMVar shardReaderMap $ \mp -> do
      case HM.lookup readShardRequestReaderId mp of
        Nothing         -> pure ()
        Just readerMvar -> putMVar readerMvar reader

   readRecords ShardReader{..} = do
     records <- S.readerRead reader (fromIntegral readShardRequestMaxRecords)
     records' <- case timestampOffset of
       Just startOffset -> return $ filterRecordBeforeTimestamp records startOffset
       Nothing -> return records
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
  -> OutStream API.ReadShardStreamResponse
  -> IO ()
readShardStream ServerContext{..} API.ReadShardStreamRequest{readShardStreamRequestReaderId=rReaderId,
    readShardStreamRequestShardId=rShardId, readShardStreamRequestShardOffset=rOffset} stream = do
  maxReadBatch <- newIORef 10
  bracket createReader deleteReader (readRecords maxReadBatch)
 where
   ldReaderBufferSize = 10

   createReader = do
     shardExists <- S.logIdHasGroup scLDClient rShardId
     unless shardExists $ throwIO $ HE.ShardNotFound $ "Shard with id " <> T.pack (show rShardId) <> " is not found."
     (startLSN, timestamp) <- getStartLSN scLDClient rShardId rOffset
     reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
     S.readerStartReading reader rShardId startLSN S.LSN_MAX
     Log.info $ "create shardReader for shard " <> Log.build rShardId <> " success, offset = " <> Log.build (show rOffset)
     return $ mkShardReader reader timestamp

   deleteReader ShardReader{..} = do
     S.readerStopReading reader rShardId

   readRecords maxReadBatch ShardReader{..}  = do
     whileM $ do
       maxBatch <- readIORef maxReadBatch
       records <- S.readerRead reader (fromIntegral maxBatch)
       if null records
          then do
            -- set read timeout to unlimited to avoid busy loop
            S.readerSetTimeout reader (-1)
            modifyIORef' maxReadBatch (const 1)
            Log.info $ "reader " <> Log.build rReaderId
              <> " read empty records, set tiemout to unlimited."
            return True
          else do
            batch <- readIORef maxReadBatch
            when (batch == 1) $ do
              S.readerSetTimeout reader 10
              modifyIORef' maxReadBatch (const 10)
              Log.info $ "resume read timeout for reader " <> Log.build rReaderId
            records' <- case timestampOffset of
              Just startOffset -> return $ filterRecordBeforeTimestamp records startOffset
              Nothing -> return records
            receivedRecordsVecs <- forM records' decodeRecordBatch
            let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
            Log.debug $ "reader " <> Log.build rReaderId
                     <> " read " <> Log.build (V.length res) <> " batchRecords"
            isRight <$> streamWrite stream (Just . API.ReadShardStreamResponse $ res)
     Log.info $ "shard reader " <> Log.build rReaderId <> " read stream done."

-- Remove all values with timestamps less than the given starting timestamp
filterRecordBeforeTimestamp :: [S.DataRecord ByteString] -> Int64 -> [S.DataRecord ByteString]
filterRecordBeforeTimestamp [] _ = []
filterRecordBeforeTimestamp records timestamp =
  if ts > timestamp then records else dropWhile (\r -> S.recordTimestamp r < timestamp) records
 where
   ts = S.recordTimestamp (head records)

-- if the start offset is timestampOffset, then return (startLSN, Just timestamp)
-- , otherwise return (startLSN, Nothing)
getStartLSN :: S.LDClient -> S.C_LogID -> Maybe API.ShardOffset -> IO (S.LSN, Maybe Int64)
getStartLSN scLDClient logId offset =
  case fromJust . API.shardOffsetOffset . fromJust $ offset of
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
