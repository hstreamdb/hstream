{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , listStreamNames
  , append
  , appendStream
  , listShards
  , createShardReader
  , deleteShardReader
  , readShard
  ) where

import           Control.Concurrent               (modifyMVar_, newEmptyMVar,
                                                   putMVar, readMVar, takeMVar,
                                                   withMVar)
import           Control.Concurrent.STM           (readTVarIO)
import           Control.Exception                (bracket, catch, throw,
                                                   throwIO)
import           Control.Monad                    (forM, unless, when)
import qualified Data.ByteString                  as BS
import           Data.Foldable                    (foldl')
import qualified Data.HashMap.Strict              as HM
import qualified Data.Map.Strict                  as M
import           Data.Maybe                       (fromJust, fromMaybe)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)
import           Proto3.Suite                     (Enumerated (Enumerated))
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Exception              (ZNONODE (..))

import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.Core.Common       (decodeRecordBatch)
import           HStream.Server.HStreamApi        (CreateShardReaderRequest (createShardReaderRequestShardId))
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData          as P
import           HStream.Server.Shard             (Shard (..), createShard,
                                                   devideKeySpace,
                                                   mkShardWithDefaultId,
                                                   mkSharedShardMapWithShards)
import           HStream.Server.Types             (ServerContext (..),
                                                   transToStreamName)
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.Utils

-------------------------------------------------------------------------------

createStream :: HasCallStack => ServerContext -> API.Stream -> IO ()
createStream ServerContext{..} API.Stream{
  streamBacklogDuration = backlogSec, streamShardCount = shardCount, ..} = do

  when (streamReplicationFactor == 0) $ throwIO (HE.InvalidReplicaFactor "Stream replicationFactor cannot be zero")
  when (shardCount <= 0) $ throwIO (HE.InvalidShardCount "ShardCount should be a positive number")

  let streamId = transToStreamName streamStreamName
      attrs = S.def{ S.logReplicationFactor = S.defAttr1 $ fromIntegral streamReplicationFactor
                   , S.logBacklogDuration   = S.defAttr1 $
                      if backlogSec > 0 then Just $ fromIntegral backlogSec else Nothing}
  catch (S.createStream scLDClient streamId attrs) $ \(_ :: S.EXISTS) ->
    throwIO $ HE.StreamExists $ "Stream (" <> show streamId <> ") has been created"

  let partitions = devideKeySpace (fromIntegral shardCount)
  shards <- forM partitions $ \(startKey, endKey) -> do
    let shard = mkShardWithDefaultId streamId startKey endKey (fromIntegral shardCount)
    createShard scLDClient shard
  Log.debug $ "create shards for stream " <> Log.buildText streamStreamName <> ": " <> Log.buildString' (show shards)

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
  if storeExists
     then doDelete
     else unless deleteStreamRequestIgnoreNonExist $ throwIO $ HE.StreamNotFound sName
  where
    streamId = transToStreamName sName
    doDelete = do
      subs <- P.getSubscriptionWithStream metaHandle sName
      if null subs
      then S.removeStream scLDClient streamId
      else if force
           then do
             -- TODO: delete the archived stream when the stream is no longer needed
             _archivedStream <- S.archiveStream scLDClient streamId
             P.updateSubscription metaHandle sName (cBytesToText $ S.getArchivedStreamName _archivedStream)
           else
             throwIO $ HE.FoundSubscription "Stream still has subscription"

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
    return $ API.Stream (T.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b) (fromIntegral shardCnt)

listStreamNames :: ServerContext -> IO (V.Vector T.Text)
listStreamNames ServerContext{..} = do
  streams <- S.findStreams scLDClient S.StreamTypeStream
  pure $ V.map (T.pack . S.showStreamName) (V.fromList streams)

append :: HasCallStack
       => ServerContext -> API.AppendRequest -> IO API.AppendResponse
append sc@ServerContext{..} request@API.AppendRequest{..} = do
  recv_time <- getPOSIXTime
  Log.debug $ "Receive Append Request: StreamName {"
           <> Log.buildText appendRequestStreamName
           <> "(shardId: "
           <> Log.buildInt appendRequestShardId
           <> ")}"

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
  appendRequestRecords = mbRecord, ..} = do
  let record@API.BatchedRecord{batchedRecordBatchSize=recordSize} = case mbRecord of
       Nothing -> throw $ HE.InvalidRecord "BatchedRecord shouldn't be Nothing"
       Just r  -> r
  timestamp <- getProtoTimestamp
  let payload = encodBatchRecord . updateRecordTimestamp timestamp $ record
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO $ HE.InvalidRecord "Record size exceeds the maximum size limit"
  S.AppendCompletion {..} <- S.appendCompressedBS scLDClient shardId payload cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder cStreamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder cStreamName (fromIntegral recordSize)
  let rids = V.zipWith (API.RecordId shardId) (V.replicate (fromIntegral recordSize) appendCompLSN) (V.fromList [0..])
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
  exist <- M.checkMetaExists @P.ShardReader rId metaHandle
  when exist $ throwIO $ HE.ShardReaderExists "ShardReaderExists"
  startLSN <- getStartLSN rShardId
  let shardReader = mkShardReader startLSN
  Log.info $ "create shardReader " <> Log.buildString' (show shardReader)
  M.insertMeta rId shardReader metaHandle
  return API.CreateShardReaderResponse
    { API.createShardReaderResponseStreamName  = rStreamName
    , API.createShardReaderResponseShardId     = rShardId
    , API.createShardReaderResponseShardOffset = rOffset
    , API.createShardReaderResponseReaderId    = rId
    , API.createShardReaderResponseTimeout     = rTimeout
    }
 where
   mkShardReader offset = P.ShardReader rStreamName rShardId offset rId rTimeout

   getStartLSN :: S.C_LogID -> IO S.LSN
   getStartLSN logId =
     case fromJust . API.shardOffsetOffset . fromJust $ rOffset of
       API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetEARLIEST)) ->
         return S.LSN_MIN
       API.ShardOffsetOffsetSpecialOffset (Enumerated (Right API.SpecialOffsetLATEST)) ->
         (+ 1) <$> S.getTailLSN scLDClient logId
       API.ShardOffsetOffsetRecordOffset API.RecordId{..} ->
         return recordIdBatchId
       _ ->
         throwIO $ HE.InvalidShardOffset "UnKnownShardOffset"

deleteShardReader
  :: HasCallStack
  => ServerContext
  -> API.DeleteShardReaderRequest
  -> IO ()
deleteShardReader ServerContext{..} API.DeleteShardReaderRequest{..} = do
  hashRing <- readTVarIO loadBalanceHashRing
  unless (getAllocatedNodeId hashRing deleteShardReaderRequestReaderId == serverID) $
    throwIO $ HE.WrongServer "Send deleteShard request to wrong server."
  isSuccess <- catch (M.deleteMeta @P.ShardReader deleteShardReaderRequestReaderId Nothing metaHandle >> return True) $
      \ (_ :: ZNONODE) -> return False
  modifyMVar_ shardReaderMap $ \mp -> do
    case HM.lookup deleteShardReaderRequestReaderId mp of
      Nothing -> return mp
      Just _  -> return (HM.delete deleteShardReaderRequestReaderId mp)
  unless isSuccess $ throwIO $ HE.EmptyShardReader "ShardReaderNotExists"

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
    throwIO $ HE.WrongServer "Send readShard request to wrong server."
  bracket getReader putReader readRecords
 where
   ldReaderBufferSize = 10

   getReader = do
     mReader <- HM.lookup readShardRequestReaderId <$> readMVar shardReaderMap
     case mReader of
       Just readerMvar -> takeMVar readerMvar
       Nothing         -> do
         r@P.ShardReader{..} <- M.getMeta readShardRequestReaderId metaHandle >>= \case
           Nothing     -> throwIO $ HE.EmptyShardReader "ShardReaderNotExists"
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
     receivedRecordsVecs <- forM records decodeRecordBatch
     let res = V.fromList $ map (\(_, _, _, record) -> record) receivedRecordsVecs
     Log.debug $ "reader " <> Log.buildText readShardRequestReaderId
              <> " read " <> Log.buildInt (V.length res) <> " batchRecords"
     return res
