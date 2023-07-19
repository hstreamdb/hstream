{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , getStream
  , listStreams
  , listStreamsWithPrefix
  , append
  , appendStream
  , listShards
  , getTailRecordId
  , trimShard
  , trimStream
  ) where

import           Control.Exception         (catch, throwIO)
import           Control.Monad             (forM, forM_, unless, when)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BSL
import qualified Data.Map.Strict           as M
import           Data.Maybe                (fromMaybe)
import qualified Data.Text                 as T
import qualified Data.Vector               as V
import           GHC.Stack                 (HasCallStack)
import           Google.Protobuf.Timestamp (Timestamp)
import qualified Proto3.Suite              as PT
import qualified Z.Data.CBytes             as CB

import           Data.Word                 (Word64)
import qualified HStream.Exception         as HE
import qualified HStream.Logger            as Log
import qualified HStream.Server.HStreamApi as API
import qualified HStream.Server.MetaData   as P
import           HStream.Server.Shard      (createShard, devideKeySpace,
                                            mkShardWithDefaultId)
import           HStream.Server.Types      (ServerContext (..),
                                            ServerInternalOffset (..),
                                            ToOffset (..), transToStreamName)
import qualified HStream.Stats             as Stats
import qualified HStream.Store             as S
import           HStream.Utils

-------------------------------------------------------------------------------

createStream :: HasCallStack => ServerContext -> API.Stream -> IO API.Stream
createStream ServerContext{..} stream@API.Stream{
  streamBacklogDuration = backlogSec, streamShardCount = shardCount, ..} = do
  timeStamp <- getProtoTimestamp
  let extraAttr = M.fromList [("createTime", lazyByteStringToCBytes $ PT.toLazyByteString timeStamp)]
  let streamId = transToStreamName streamStreamName
      attrs = S.def { S.logReplicationFactor = S.defAttr1 $ fromIntegral streamReplicationFactor
                    , S.logBacklogDuration   = S.defAttr1 $
                       if backlogSec > 0 then Just $ fromIntegral backlogSec else Nothing
                    , S.logAttrsExtras       = extraAttr
                    }
  catch (S.createStream scLDClient streamId attrs) $ \(_ :: S.EXISTS) ->
    throwIO $ HE.StreamExists streamStreamName

  let partitions = devideKeySpace (fromIntegral shardCount)
  shards <- forM partitions $ \(startKey, endKey) -> do
    let shard = mkShardWithDefaultId streamId startKey endKey (fromIntegral shardCount)
    createShard scLDClient shard
  Log.debug $ "create shards for stream " <> Log.build streamStreamName <> ": " <> Log.buildString' (show shards)
  return stream{API.streamCreationTime = Just timeStamp}

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
      then do S.removeStream scLDClient streamId
              Stats.stream_stat_erase scStatsHolder (textToCBytes sName)
      else if force
           then do
             -- TODO:
             -- 1. delete the archived stream when the stream is no longer needed
             -- 2. erase stats for archived stream
             _archivedStream <- S.archiveStream scLDClient streamId
             P.updateSubscription metaHandle sName (cBytesToText $ S.getArchivedStreamName _archivedStream)
           else
             throwIO HE.FoundSubscription

getStream :: ServerContext -> API.GetStreamRequest -> IO API.GetStreamResponse
getStream ServerContext{..} API.GetStreamRequest{ getStreamRequestName = sName} = do
  let streamId = transToStreamName sName
  storeExists <- S.doesStreamExist scLDClient streamId
  unless storeExists $ throwIO $ HE.StreamNotFound sName
  attrs <- S.getStreamLogAttrs scLDClient streamId
  let reFac = fromMaybe 0 . S.attrValue . S.logReplicationFactor $ attrs
      backlogSec = fromMaybe 0 . fromMaybe Nothing . S.attrValue . S.logBacklogDuration $ attrs
      createdAt = PT.fromByteString . BSL.toStrict . cBytesToLazyByteString $ S.logAttrsExtras attrs M.! "createTime"
  shardsCount <- fromIntegral . M.size <$> S.listStreamPartitions scLDClient streamId
  return API.GetStreamResponse {
      getStreamResponseStream = Just API.Stream{
          streamStreamName = sName
        , streamReplicationFactor = fromIntegral reFac
        , streamBacklogDuration = fromIntegral backlogSec
        , streamCreationTime = either (const Nothing) Just createdAt
        , streamShardCount =  shardsCount
        }
      }

listStreams
  :: HasCallStack
  => ServerContext
  -> API.ListStreamsRequest
  -> IO (V.Vector API.Stream)
listStreams sc@ServerContext{..} API.ListStreamsRequest = do
  streams <- S.findStreams scLDClient S.StreamTypeStream
  V.forM (V.fromList streams) (getStreamInfo sc)

listStreamsWithPrefix
  :: HasCallStack
  => ServerContext
  -> API.ListStreamsWithPrefixRequest
  -> IO (V.Vector API.Stream)
listStreamsWithPrefix sc@ServerContext{..} API.ListStreamsWithPrefixRequest{..} = do
  streams <- filter (T.isPrefixOf listStreamsWithPrefixRequestPrefix . T.pack . S.showStreamName) <$> S.findStreams scLDClient S.StreamTypeStream
  V.forM (V.fromList streams) (getStreamInfo sc)

trimStream
  :: HasCallStack
  => ServerContext
  -> T.Text
  -> API.StreamOffset
  -> IO ()
trimStream ServerContext{..} streamName trimPoint = do
  streamExists <- S.doesStreamExist scLDClient streamId
  unless streamExists $ do
    Log.info $ "trimStream failed because stream " <> Log.build streamName <> " is not found."
    throwIO $ HE.StreamNotFound $ "stream " <> T.pack (show streamName) <> " is not found."
  shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
  forM_ shards $ \shardId -> do
    getTrimLSN scLDClient shardId trimPoint >>= S.trim scLDClient shardId
 where
   streamId = transToStreamName streamName

getStreamInfo :: ServerContext -> S.StreamId -> IO API.Stream
getStreamInfo ServerContext{..} stream = do
    attrs <- S.getStreamLogAttrs scLDClient stream
    -- FIXME: should the default value be 0?
    let r = fromMaybe 0 . S.attrValue . S.logReplicationFactor $ attrs
        b = fromMaybe 0 . fromMaybe Nothing . S.attrValue . S.logBacklogDuration $ attrs
        extraAttr = getCreateTime $ S.logAttrsExtras attrs
    shardCnt <- length <$> S.listStreamPartitions scLDClient stream
    return $ API.Stream (T.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b) (fromIntegral shardCnt) extraAttr
 where
   getCreateTime :: M.Map CB.CBytes CB.CBytes -> Maybe Timestamp
   getCreateTime attr = M.lookup "createTime" attr >>= \tmp -> do
     case PT.fromByteString . BSL.toStrict . cBytesToLazyByteString $ tmp of
       Left _          -> Nothing
       Right timestamp -> Just timestamp

getTailRecordId :: ServerContext -> API.GetTailRecordIdRequest -> IO API.GetTailRecordIdResponse
getTailRecordId ServerContext{..} API.GetTailRecordIdRequest{getTailRecordIdRequestShardId=sId} = do
  shardExists <- S.logIdHasGroup scLDClient sId
  unless shardExists $ throwIO $ HE.ShardNotFound $ "Shard with id " <> T.pack (show sId) <> " is not found."
  lsn <- S.getTailLSN scLDClient sId
  let recordId = API.RecordId { recordIdShardId    = sId
                              , recordIdBatchId    = lsn
                              , recordIdBatchIndex = 0
                              }
  return $ API.GetTailRecordIdResponse { getTailRecordIdResponseTailRecordId = Just recordId}

append :: HasCallStack
       => ServerContext
       -> T.Text                 -- streamName
       -> Word64                 -- shardId
       -> API.BatchedRecord      -- payload
       -> IO API.AppendResponse
append sc@ServerContext{..} streamName shardId payload = do
  !recv_time <- getPOSIXTime
  Log.debug $ "Receive Append Request: StreamName {"
           <> Log.build streamName
           <> "(shardId: "
           <> Log.build shardId
           <> ")}"

  Stats.handle_time_series_add_queries_in scStatsHolder "append" 1
  Stats.stream_stat_add_append_total scStatsHolder cStreamName 1
  Stats.stream_time_series_add_append_in_requests scStatsHolder cStreamName 1

  !append_start <- getPOSIXTime
  resp <- appendStream sc streamName shardId payload
  Stats.serverHistogramAdd scStatsHolder Stats.SHL_AppendLatency =<< msecSince append_start
  Stats.serverHistogramAdd scStatsHolder Stats.SHL_AppendRequestLatency =<< msecSince recv_time
  return resp
  where
    cStreamName = textToCBytes streamName

appendStream :: HasCallStack
             => ServerContext
             -> T.Text
             -> Word64
             -> API.BatchedRecord
             -> IO API.AppendResponse
appendStream ServerContext{..} streamName shardId record = do
  let payload = encodBatchRecord record
      recordSize = API.batchedRecordBatchSize record
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO $ HE.InvalidRecordSize payloadSize
  S.AppendCompletion {..} <- S.appendCompressedBS scLDClient shardId payload cmpStrategy Nothing
  Stats.stream_stat_add_append_in_bytes scStatsHolder cStreamName (fromIntegral payloadSize)
  Stats.stream_stat_add_append_in_records scStatsHolder cStreamName (fromIntegral recordSize)
  Stats.stream_time_series_add_append_in_bytes scStatsHolder cStreamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder cStreamName (fromIntegral recordSize)
  let rids = V.zipWith (API.RecordId shardId) (V.replicate (fromIntegral recordSize) appendCompLSN) (V.fromList [0..])
  return $ API.AppendResponse {
      appendResponseStreamName = streamName
    , appendResponseShardId    = shardId
    , appendResponseRecordIds  = rids }
  where
    cStreamName = textToCBytes streamName

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

trimShard
  :: HasCallStack
  => ServerContext
  -> Word64
  -> API.ShardOffset
  -> IO ()
trimShard ServerContext{..} shardId trimPoint = do
  shardExists <- S.logIdHasGroup scLDClient shardId
  unless shardExists $ do
    Log.info $ "trimShard failed because shard " <> Log.build shardId <> " is not exist."
    throwIO $ HE.ShardNotFound $ "Shard with id " <> T.pack (show shardId) <> " is not found."
  getTrimLSN scLDClient shardId trimPoint >>= S.trim scLDClient shardId

--------------------------------------------------------------------------------
-- helper

getTrimLSN :: (ToOffset g, Show g) => S.LDClient -> Word64 -> g -> IO S.LSN
getTrimLSN client shardId trimPoint = do
  lsn <- getLSN client shardId (toOffset trimPoint)
  Log.info $ "getTrimLSN for shard " <> Log.build (show shardId)
          <> ", trimPoint: " <> Log.build (show trimPoint)
          <> ", lsn: " <> Log.build (show lsn)
  return lsn
 where
  getLSN :: S.LDClient -> S.C_LogID -> ServerInternalOffset -> IO S.LSN
  getLSN scLDClient logId offset =
    case offset of
      OffsetEarliest -> return S.LSN_MIN
      OffsetLatest -> S.getTailLSN scLDClient logId
      OffsetRecordId API.RecordId{..} -> return recordIdBatchId
      OffsetTimestamp API.TimestampOffset{..} -> do
        let accuracy = if timestampOffsetStrictAccuracy then S.FindKeyStrict else S.FindKeyApproximate
        S.findTime scLDClient logId timestampOffsetTimestampInMs accuracy

