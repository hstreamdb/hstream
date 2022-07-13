module HStream.Server.Core.Shard
    ( readShard,
      listShards,
      splitShards,
    )
where

import           Control.Exception                (bracket)
import           Control.Monad                    (void)
import           Data.Foldable                    (foldl')
import qualified Data.Map.Strict                  as M
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB

import           Data.Maybe                       (fromJust)
import           HStream.Connector.HStore         (transToStreamName)
import           HStream.Server.Exception         (InvalidArgument (..),
                                                   StreamNotExist (..))
import           HStream.Server.Handler.Common    (decodeRecordBatch)
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.ReaderPool        (getReader, putReader)
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils
import           Proto3.Suite                     (Enumerated (Enumerated))

-------------------------------------------------------------------------------

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
         API.Shard { API.shardStreamName = listShardsRequestStreamName
                   , API.shardShardId    = logId
                   , API.shardIsActive   = True
                   }
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

splitShards
  :: HasCallStack
  => ServerContext
  -> API.SplitShardsRequest
  -> IO (V.Vector API.Shard)
splitShards = error ""
