{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  , append0Stream
  , readShard
  , listShards
  , FoundSubscription (..)
  , StreamExists (..)
  , RecordTooBig (..)
  ) where

import qualified Data.Map.Strict as M
import qualified Data.HashMap.Strict as HM
import           Control.Exception                 (Exception (displayException),
                                                    bracket, catch, throwIO)
import           Control.Monad                     (foldM, forM, unless, void,
                                                    when)
import qualified Data.ByteString                   as BS
import           Data.Foldable                     (foldl')
import qualified Data.Map.Strict                   as M
import qualified Data.HashMap.Strict               as HM
import           Data.Maybe                        (fromJust, fromMaybe)
import           Data.Text                         (Text)
import qualified Data.Text                         as Text
import qualified Data.Vector                       as V
import           GHC.Stack                         (HasCallStack)
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                     as CB

import           Control.Concurrent                (MVar, modifyMVar,
                                                    modifyMVar_)
import           HStream.Connector.HStore          (transToStreamName)
import qualified HStream.Logger                    as Log
import           HStream.Server.Exception          (InvalidArgument (..),
                                                    StreamNotExist (..))
import           HStream.Server.Handler.Common     (decodeRecordBatch)
import           HStream.Server.HStreamApi         (ReadShardRequest (readShardRequestShardId),
                                                    Shard (Shard))
import qualified HStream.Server.HStreamApi         as API
import           HStream.Server.Persistence.Object (getSubscriptionWithStream,
                                                    updateSubscription)
import           HStream.Server.ReaderPool         (getReader, putReader)
import           HStream.Server.Shard              (Shard (..), cBytesToKey,
                                                    createShard, devideKeySpace,
                                                    hashShardKey,
                                                    mkShardWithDefaultId,
                                                    mkSharedShardMapWithShards,
                                                    shardStartKey)
import           HStream.Server.Types              (ServerContext (..),
                                                    ShardDict)
import HStream.Server.Shard (devideKeySpace, shardStartKey, createShard, mkShardWithDefaultId, mkSharedShardMapWithShards, Shard (..), cBytesToKey, hashShardKey)
import qualified HStream.Stats                     as Stats
import qualified HStream.Store                     as S
import           HStream.ThirdParty.Protobuf       as PB
import           HStream.Utils
import           Proto3.Suite                      (Enumerated (Enumerated))
import Control.Concurrent (modifyMVar, modifyMVar_, MVar)
import qualified HStream.Logger as Log

-------------------------------------------------------------------------------

-- createStream will create a stream with a default partition
-- FIXME: Currently, creating a stream which partially exists will do the job
-- but return failure response
createStream :: HasCallStack => ServerContext -> API.Stream -> IO (ServerResponse 'Normal API.Stream)
createStream ServerContext{..} stream@API.Stream{
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
  returnResp stream

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO (ServerResponse 'Normal Empty)
deleteStream ServerContext{..} API.DeleteStreamRequest{deleteStreamRequestForce = force,
  deleteStreamRequestStreamName = sName, ..} = do
  storeExists <- S.doesStreamExist scLDClient streamId
  if storeExists then doDelete
    else unless deleteStreamRequestIgnoreNonExist $ throwIO StreamNotExist
  returnResp Empty
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
    return $ API.Stream (Text.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b) (fromIntegral $ shardCnt - 1)

appendStream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest {appendRequestStreamName = sName,
  appendRequestRecords = records} partitionKey = do
  timestamp <- getProtoTimestamp
  let payload = encodeBatch . API.HStreamRecordBatch $
        encodeRecord . updateRecordTimestamp timestamp <$> records
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  shardId <- getShardId scLDClient shardTable sName partitionKey
  Log.debug $ "shardId for key " <> Log.buildString' (show partitionKey) <> " is " <> Log.buildString' (show shardId)
  S.AppendCompletion {..} <- S.appendCompressedBS scLDClient shardId payload cmpStrategy Nothing
  Log.debug "append success"
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder streamName (fromIntegral $ length records)
  let rids = V.zipWith (API.RecordId shardId) (V.replicate (length records) appendCompLSN) (V.fromList [0..])
  Log.debug $ "rids " <> Log.buildString' (show rids)
  return $ API.AppendResponse sName rids
 where
   streamName = textToCBytes sName

readShard
  :: HasCallStack
  => ServerContext
  -> API.ReadShardRequest
  -> IO (V.Vector API.ReceivedRecord)
readShard ServerContext{..} API.ReadShardRequest{..} = do
  logId <- S.getUnderlyingLogId scLDClient streamId (Just shard)
  startLSN <- getStartLSN logId

  bracket
    (getReader readerPool)
    (flip S.readerStopReading logId >> putReader readerPool)
    (\reader -> readData reader logId startLSN)
  where
    streamId = transToStreamName readShardRequestStreamName
    shard = textToCBytes readShardRequestShardId

    getStartLSN :: S.C_LogID -> IO S.LSN
    getStartLSN logId =
      case fromJust . API.shardOffsetOffset . fromJust $ readShardRequestOffset of
        API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetEARLIEST)) -> return S.LSN_MIN
        API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetLATEST))   -> (+ 1) <$> S.getTailLSN scLDClient logId
        API.ShardOffsetOffsetRecordOffset API.RecordId{..}                                -> return recordIdBatchId
        _                                                                                 -> error "wrong shard offset"

    readData :: S.LDReader -> S.C_LogID -> S.LSN -> IO (V.Vector API.ReceivedRecord)
    readData reader logId startLSN = do
      void $ S.readerStartReading reader logId startLSN (startLSN + fromIntegral readShardRequestMaxRead)
      S.readerSetTimeout reader 0
      records <- S.readerRead reader (fromIntegral readShardRequestMaxRead)
      let receivedRecordsVecs = decodeRecordBatch <$> records
      return $ foldl' (\acc (_, _, _, record) -> acc <> record) V.empty receivedRecordsVecs

--------------------------------------------------------------------------------

append0Stream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
append0Stream ServerContext{..} API.AppendRequest{..} partitionKey = do
  timestamp <- getProtoTimestamp
  let payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . API.hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  shardId <- getShardId scLDClient shardTable appendRequestStreamName partitionKey
  S.AppendCompletion {..} <- S.appendBatchBS scLDClient shardId (V.toList payloads) cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder streamName (fromIntegral $ length appendRequestRecords)
  let records = V.zipWith (\_ idx -> API.RecordId shardId appendCompLSN idx) appendRequestRecords [0..]
  return $ API.AppendResponse appendRequestStreamName records

--------------------------------------------------------------------------------

getShardId :: S.LDClient -> MVar (HM.HashMap Text ShardDict) -> Text -> Maybe Text -> IO S.C_LogID
getShardId client shardTable sName partitionKey = do
  getShardDict >>= return . snd . fromJust . M.lookupLE shardKey
 where
   shardKey   = hashShardKey . fromMaybe clientDefaultKey $ partitionKey
   streamID   = S.mkStreamId S.StreamTypeStream streamName
   streamName = textToCBytes sName

   getShardDict = modifyMVar shardTable $ \mp -> do
     case HM.lookup sName mp of
       Just shards -> do
           Log.info $ "shardDict exist for stream " <> Log.buildText sName
           return (mp, shards)
       Nothing     -> do
         -- loading shard infomation for stream first.
         shards <- M.elems <$> S.listStreamPartitions client streamID
         shardDict <- foldM insertShardDict M.empty shards
         Log.fatal $ "build shardDict for stream " <> Log.buildText sName <> ": " <> Log.buildString' (show shardDict)
         return (HM.insert sName shardDict mp, shardDict)

   insertShardDict dict shardId = do
     Log.fatal $ "getStreamPartitionExtraAttrs for shard " <> Log.buildInt shardId
     attrs <- S.getStreamPartitionExtraAttrs client shardId
     Log.fatal $ "attrs for shard " <> Log.buildInt shardId <> ": " <> Log.buildString' (show attrs)
     startKey <- case M.lookup shardStartKey attrs of
                   Nothing  -> return $ cBytesToKey "0"
                   -- Nothing -> throwIO $ ShardKeyNotFound shardId
                   Just key -> return $ cBytesToKey key
     return $ M.insert startKey shardId dict
     -- case M.lookup shardStartKey attrs of
     --   -- Nothing -> return $ cBytesToKey "0"
     --   Nothing -> return dict
     --   Just key -> return $ M.insert (cBytesToKey key) shardId dict

--------------------------------------------------------------------------------

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
       Nothing -> return . V.snoc shards $
         Shard { shardStreamName = listShardsRequestStreamName
               , shardShardId    = logId
               , shardIsActive   = True
               }
       Just(sKey, eKey, ep) -> return . V.snoc shards $
         Shard { shardStreamName        = listShardsRequestStreamName
               , shardShardId           = logId
               , shardStartHashRangeKey = sKey
               , shardEndHashRangeKey   = eKey
               , shardEpoch             = ep
               -- FIXME: neet a way to find if this shard is active
               , shardIsActive          = True
               }

   getInfo mp = do
     startHashRangeKey <- cBytesToText <$> M.lookup startKey mp
     endHashRangeKey   <- cBytesToText <$> M.lookup endKey mp
     shardEpoch        <- read . CB.unpack <$> M.lookup epoch mp
     return (startHashRangeKey, endHashRangeKey, shardEpoch)

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
