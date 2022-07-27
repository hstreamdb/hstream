{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , append
  , appendStream
  , listShards
  , createShardReader
  , deleteShardReader
  , readShard
  , FoundSubscription (..)
  , StreamExists (..)
  , RecordTooBig (..)
  , ShardReaderExists (..)
  , ShardReaderNotExists (..)
  , UnKnownShardOffset (..)
  ) where

import           Control.Concurrent                (modifyMVar_, newEmptyMVar,
                                                    putMVar, readMVar, takeMVar,
                                                    withMVar)
import           Control.Concurrent.STM            (readTVarIO)
import           Control.Exception                 (Exception (displayException),
                                                    bracket, catch, throw,
                                                    throwIO)
import           Control.Monad                     (forM, unless, when)
import qualified Data.ByteString                   as BS
import           Data.Foldable                     (foldl')
import qualified Data.HashMap.Strict               as HM
import qualified Data.Map.Strict                   as M
import           Data.Maybe                        (fromJust, fromMaybe)
import qualified Data.Text                         as T
import qualified Data.Vector                       as V
import           GHC.Stack                         (HasCallStack)
import           Proto3.Suite                      (Enumerated (Enumerated))
import qualified Z.Data.CBytes                     as CB

import           HStream.Common.ConsistentHashing  (getAllocatedNodeId)
import qualified HStream.Logger                    as Log
import           HStream.Server.Core.Common        (decodeRecordBatch)
import           HStream.Server.Exception          (InvalidArgument (..),
                                                    StreamNotExist (..),
                                                    WrongServer (..))
import           HStream.Server.HStreamApi         (CreateShardReaderRequest (createShardReaderRequestShardId))
import qualified HStream.Server.HStreamApi         as API
import           HStream.Server.Persistence        (ShardReader (..))
import qualified HStream.Server.Persistence        as P
import           HStream.Server.Persistence.Object (getSubscriptionWithStream,
                                                    updateSubscription)
import           HStream.Server.Shard              (Shard (..), createShard,
                                                    devideKeySpace,
                                                    mkShardWithDefaultId,
                                                    mkSharedShardMapWithShards)
import           HStream.Server.Types              (ServerContext (..),
                                                    transToStreamName)
import qualified HStream.Stats                     as Stats
import qualified HStream.Store                     as S
import           HStream.Utils
import           ZooKeeper.Exception               (ZNONODE (..))

-------------------------------------------------------------------------------

createStream :: HasCallStack => ServerContext -> API.Stream -> IO ()
createStream ServerContext{..} API.Stream{
  streamBacklogDuration = backlogSec, streamShardCount = shardCount, ..} = do

  when (streamReplicationFactor == 0) $ throwIO (InvalidArgument "Stream replicationFactor cannot be zero")
  let streamId = transToStreamName streamStreamName
      attrs = S.def{ S.logReplicationFactor = S.defAttr1 $ fromIntegral streamReplicationFactor
                   , S.logBacklogDuration   = S.defAttr1 $
                      if backlogSec > 0 then Just $ fromIntegral backlogSec else Nothing}
  catch (S.createStream scLDClient streamId attrs) (\(_ :: S.EXISTS) -> throwIO StreamExists)
  when (shardCount <= 0) $ throwIO (InvalidArgument "ShardCount should be a positive number")

  let partitions = devideKeySpace (fromIntegral shardCount)
  shards <- forM partitions $ \(startKey, endKey) -> do
    let shard = mkShardWithDefaultId streamId startKey endKey (fromIntegral shardCount)
    createShard scLDClient shard

  shardMp <- mkSharedShardMapWithShards shards
  modifyMVar_ shardInfo $ return . HM.insert streamStreamName shardMp
  let shardDict = foldl' (\acc Shard{startKey=key, shardId=sId} -> M.insert key sId acc) M.empty shards
  modifyMVar_ shardTable $ return . HM.insert streamStreamName shardDict

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO ()
deleteStream ServerContext{..} API.DeleteStreamRequest{deleteStreamRequestForce = force,
  deleteStreamRequestStreamName = sName, ..} = do
  storeExists <- S.doesStreamExist scLDClient streamId
  if storeExists then doDelete
    else unless deleteStreamRequestIgnoreNonExist $ throwIO StreamNotExist
  where
    streamId = transToStreamName sName
    doDelete = do
      subs <- getSubscriptionWithStream zkHandle sName
      if null subs
      then S.removeStream scLDClient streamId
      else if force
           then do
             -- TODO: delete the archived stream when the stream is no longer needed
             _archivedStream <- S.archiveStream scLDClient streamId
             updateSubscription zkHandle sName (cBytesToText $ S.getArchivedStreamName _archivedStream)
           else
             throwIO FoundSubscription

listStreams
  :: HasCallStack
  => ServerContext
  -> API.ListStreamsRequest
  -> IO (V.Vector API.Stream)
listStreams ServerContext{..} API.ListStreamsRequest = do
  streams <- S.findStreams scLDClient S.StreamTypeStream
  V.forM (V.fromList streams) $ \stream -> do
    attrs <- S.getStreamLogAttrs scLDClient stream
    -- FIXME: should the default value be 0?
    let r = fromMaybe 0 . S.attrValue . S.logReplicationFactor $ attrs
        b = fromMaybe 0 . fromMaybe Nothing . S.attrValue . S.logBacklogDuration $ attrs
    shardCnt <- length <$> S.listStreamPartitions scLDClient stream
    return $ API.Stream (T.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b) (fromIntegral $ shardCnt - 1)

append :: HasCallStack
       => ServerContext -> API.AppendRequest -> IO API.AppendResponse
append sc@ServerContext{..} request@API.AppendRequest{..} = do
  recv_time <- getPOSIXTime
  Log.debug $ "Receive Append Request: StreamName {"
           <> Log.buildInt appendRequestShardId
           <> "}, nums of records = "
           <> Log.buildInt (V.length appendRequestRecords)

  Stats.handle_time_series_add_queries_in scStatsHolder "append" 1
  Stats.stream_stat_add_append_total scStatsHolder cStreamName 1
  Stats.stream_time_series_add_append_in_requests scStatsHolder cStreamName 1

  append_start <- getPOSIXTime
  resp <- appendStream sc request
  Stats.serverHistogramAdd scStatsHolder Stats.SHL_AppendLatency =<< msecSince append_start
  Stats.serverHistogramAdd scStatsHolder Stats.SHL_AppendRequestLatency =<< msecSince recv_time
  return resp
  where
    cStreamName = textToCBytes appendRequestStreamName

appendStream :: HasCallStack
             => ServerContext -> API.AppendRequest -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest {appendRequestShardId = shardId,
  appendRequestRecords = records, ..} = do
  timestamp <- getProtoTimestamp
  let payload = encodeBatch . API.HStreamRecordBatch $
        encodeRecord . updateRecordTimestamp timestamp <$> records
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  S.AppendCompletion {..} <- S.appendCompressedBS scLDClient shardId payload cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder cStreamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder cStreamName (fromIntegral $ length records)
  let rids = V.zipWith (API.RecordId shardId) (V.replicate (length records) appendCompLSN) (V.fromList [0..])
  return $ API.AppendResponse {
      appendResponseStreamName = appendRequestStreamName
    , appendResponseShardId    = shardId
    , appendResponseRecordIds  = rids }
  where
    cStreamName = textToCBytes appendRequestStreamName

--------------------------------------------------------------------------------

createShardReader
  :: HasCallStack
  => ServerContext
  -> API.CreateShardReaderRequest
  -> IO API.CreateShardReaderResponse
createShardReader ServerContext{..} API.CreateShardReaderRequest{createShardReaderRequestStreamName=rStreamName,
    createShardReaderRequestShardId=rShardId, createShardReaderRequestShardOffset=rOffset, createShardReaderRequestTimeout=rTimeout,
    createShardReaderRequestReaderId=rId} = do
  exist <- P.readerExist rId zkHandle
  when exist $ throwIO ShardReaderExists
  startLSN <- getStartLSN rShardId
  let shardReader = mkShardReader startLSN
  Log.info $ "create shardReader " <> Log.buildString' (show shardReader)
  P.storeReader rId shardReader zkHandle
  return API.CreateShardReaderResponse
    { API.createShardReaderResponseStreamName  = rStreamName
    , API.createShardReaderResponseShardId     = rShardId
    , API.createShardReaderResponseShardOffset = rOffset
    , API.createShardReaderResponseReaderId    = rId
    , API.createShardReaderResponseTimeout     = rTimeout
    }
 where
   mkShardReader offset = ShardReader rStreamName rShardId offset rId rTimeout

   getStartLSN :: S.C_LogID -> IO S.LSN
   getStartLSN logId =
     case fromJust . API.shardOffsetOffset . fromJust $ rOffset of
       API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetEARLIEST)) -> return S.LSN_MIN
       API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetLATEST))   -> (+ 1) <$> S.getTailLSN scLDClient logId
       API.ShardOffsetOffsetRecordOffset API.RecordId{..}                                -> return recordIdBatchId
       _                                                                                 -> throwIO UnKnownShardOffset

deleteShardReader
  :: HasCallStack
  => ServerContext
  -> API.DeleteShardReaderRequest
  -> IO ()
deleteShardReader ServerContext{..} API.DeleteShardReaderRequest{..} = do
  hashRing <- readTVarIO loadBalanceHashRing
  unless (getAllocatedNodeId hashRing deleteShardReaderRequestReaderId == serverID) $
    throwIO $ WrongServer "Send deleteShard request to wrong server."
  isSuccess <- catch (P.removeReader deleteShardReaderRequestReaderId zkHandle >> return True) $
      \ (_ :: ZNONODE) -> return False
  modifyMVar_ shardReaderMap $ \mp -> do
    case HM.lookup deleteShardReaderRequestReaderId mp of
      Nothing -> return mp
      Just _  -> return (HM.delete deleteShardReaderRequestReaderId mp)
  unless isSuccess $ throwIO ShardReaderNotExists

listShards
  :: HasCallStack
  => ServerContext
  -> API.ListShardsRequest
  -> IO (V.Vector API.Shard)
listShards ServerContext{..} API.ListShardsRequest{..} = do
  shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
  V.foldM' getShardInfo V.empty $ V.fromList shards
 where
   streamId = transToStreamName listShardsRequestStreamName
   startKey = CB.pack "startKey"
   endKey   = CB.pack "endKey"
   epoch    = CB.pack "epoch"

   getShardInfo shards logId = do
     attr <- S.getStreamPartitionExtraAttrs scLDClient logId
     case getInfo attr of
       -- FIXME: should raise an exception when get Nothing
       Nothing -> return shards
       Just(sKey, eKey, ep) -> return . V.snoc shards $
         API.Shard { API.shardStreamName        = listShardsRequestStreamName
                   , API.shardShardId           = logId
                   , API.shardStartHashRangeKey = sKey
                   , API.shardEndHashRangeKey   = eKey
                   , API.shardEpoch             = ep
                   -- FIXME: neet a way to find if this shard is active
                   , API.shardIsActive          = True
                   }

   getInfo mp = do
     startHashRangeKey <- cBytesToText <$> M.lookup startKey mp
     endHashRangeKey   <- cBytesToText <$> M.lookup endKey mp
     shardEpoch        <- read . CB.unpack <$> M.lookup epoch mp
     return (startHashRangeKey, endHashRangeKey, shardEpoch)

readShard
  :: HasCallStack
  => ServerContext
  -> API.ReadShardRequest
  -> IO (V.Vector API.ReceivedRecord)
readShard ServerContext{..} API.ReadShardRequest{..} = do
  hashRing <- readTVarIO loadBalanceHashRing
  unless (getAllocatedNodeId hashRing readShardRequestReaderId == serverID) $
    throwIO $ WrongServer "Send readShard request to wrong server."
  bracket getReader putReader readRecords
 where
   ldReaderBufferSize = 10

   getReader = do
     mReader <- HM.lookup readShardRequestReaderId <$> readMVar shardReaderMap
     case mReader of
       Just readerMvar -> takeMVar readerMvar
       Nothing         -> do
         r@ShardReader{..} <- P.getReader readShardRequestReaderId zkHandle >>= \case
           Nothing     -> throwIO ShardReaderNotExists
           Just reader -> return reader
         Log.info $ "get reader from persistence: " <> Log.buildString' (show r)
         reader <- S.newLDReader scLDClient 1 (Just ldReaderBufferSize)
         S.readerStartReading reader readerShardId readerShardOffset S.LSN_MAX
         S.readerSetTimeout reader (fromIntegral readerReadTimeout)
         readerMvar <- newEmptyMVar
         modifyMVar_ shardReaderMap $ return . HM.insert readShardRequestReaderId readerMvar
         return reader
   putReader reader = do
    withMVar shardReaderMap $ \mp -> do
     case HM.lookup readShardRequestReaderId mp of
       Nothing         -> pure ()
       Just readerMvar -> putMVar readerMvar reader
   readRecords reader = do
     records <- S.readerRead reader (fromIntegral readShardRequestMaxRecords)
     let receivedRecordsVecs = decodeRecordBatch <$> records
     let res = foldl' (\acc (_, _, _, record) -> acc <> record) V.empty receivedRecordsVecs
     Log.debug $ "reader " <> Log.buildText readShardRequestReaderId
              <> " read " <> Log.buildInt (V.length res) <> " records"
     return res

--------------------------------------------------------------------------------

data FoundSubscription = FoundSubscription
  deriving (Show)
instance Exception FoundSubscription

data RecordTooBig = RecordTooBig
  deriving (Show)
instance Exception RecordTooBig

data StreamExists = StreamExists
  deriving (Show)
instance Exception StreamExists where
  displayException StreamExists = "StreamExists: Stream has been created"

newtype ShardKeyNotFound = ShardKeyNotFound S.C_LogID
  deriving (Show)
instance Exception ShardKeyNotFound where
  displayException (ShardKeyNotFound shardId) = "Can't get shardKey for shard " <> show shardId

data ShardReaderNotExists = ShardReaderNotExists
  deriving (Show)
instance Exception ShardReaderNotExists

data ShardReaderExists = ShardReaderExists
  deriving (Show)
instance Exception ShardReaderExists

data UnKnownShardOffset = UnKnownShardOffset
  deriving (Show)
instance Exception UnKnownShardOffset
