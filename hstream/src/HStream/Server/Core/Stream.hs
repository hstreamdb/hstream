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
  ) where

import           Control.Concurrent        (modifyMVar_)
import           Control.Exception         (catch, throwIO)
import           Control.Monad             (forM, unless, when)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BSL
import           Data.Foldable             (foldl')
import qualified Data.HashMap.Strict       as HM
import qualified Data.Map.Strict           as M
import           Data.Maybe                (fromMaybe)
import qualified Data.Text                 as T
import qualified Data.Vector               as V
import           GHC.Stack                 (HasCallStack)
import           Google.Protobuf.Timestamp (Timestamp, fromRFC3339, toRFC3339)
import qualified Proto3.Suite              as PT
import qualified Z.Data.CBytes             as CB

import           Data.Word                 (Word64)
import qualified HStream.Exception         as HE
import qualified HStream.Logger            as Log
import qualified HStream.Server.HStreamApi as API
import qualified HStream.Server.MetaData   as P
import           HStream.Server.Shard      (Shard (..), createShard,
                                            devideKeySpace,
                                            mkShardWithDefaultId,
                                            mkSharedShardMapWithShards)
import           HStream.Server.Types      (ServerContext (..),
                                            transToStreamName)
import qualified HStream.Stats             as Stats
import qualified HStream.Store             as S
import           HStream.Utils

-------------------------------------------------------------------------------

createStream :: HasCallStack => ServerContext -> API.Stream -> IO API.Stream
createStream ServerContext{..} stream@API.Stream{
  streamBacklogDuration = backlogSec, streamShardCount = shardCount, ..} = do
  timeStamp <- getProtoTimestamp
  let extraAttr = M.fromList [("createTime", lazyTextToCBytes $ toRFC3339 timeStamp)]
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

  shardMp <- mkSharedShardMapWithShards shards
  modifyMVar_ shardInfo $ return . HM.insert streamStreamName shardMp
  let shardDict = foldl' (\acc Shard{startKey=key, shardId=sId} -> M.insert key sId acc) M.empty shards
  modifyMVar_ shardTable $ return . HM.insert streamStreamName shardDict
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
      createdAt = getCreateTime $ S.logAttrsExtras attrs

  shardsCount <- fromIntegral . M.size <$> S.listStreamPartitions scLDClient streamId
  return API.GetStreamResponse {
      getStreamResponseStream = Just API.Stream{
          streamStreamName = sName
        , streamReplicationFactor = fromIntegral reFac
        , streamBacklogDuration = fromIntegral backlogSec
        , streamCreationTime = createdAt
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

getStreamInfo :: ServerContext -> S.StreamId -> IO API.Stream
getStreamInfo ServerContext{..} stream = do
    attrs <- S.getStreamLogAttrs scLDClient stream
    -- FIXME: should the default value be 0?
    let r = fromMaybe 0 . S.attrValue . S.logReplicationFactor $ attrs
        b = fromMaybe 0 . fromMaybe Nothing . S.attrValue . S.logBacklogDuration $ attrs
        extraAttr = getCreateTime $ S.logAttrsExtras attrs
    shardCnt <- length <$> S.listStreamPartitions scLDClient stream
    return $ API.Stream (T.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b) (fromIntegral shardCnt) extraAttr

getCreateTime :: M.Map CB.CBytes CB.CBytes -> Maybe Timestamp
getCreateTime attr = M.lookup "createTime" attr >>= fromRFC3339 . cBytesToLazyText

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
