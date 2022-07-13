{-# LANGUAGE LambdaCase #-}

module HStream.Server.Core.Shard
  ( readShard
  , listShards
  , splitShards
  , mergeShards
  )
where

import           Control.Exception          (bracket)
import           Control.Monad              (foldM, void)
import           Data.Foldable              (foldl')
import qualified Data.HashMap.Strict        as HM
import qualified Data.Map.Strict            as M
import qualified Data.Vector                as V
import           GHC.Stack                  (HasCallStack)
import qualified Z.Data.CBytes              as CB

import           Control.Concurrent         (MVar, modifyMVar, modifyMVar_,
                                             readMVar)
import           Data.Maybe                 (fromJust, fromMaybe)
import           Data.Text                  (Text)
import           Data.Word                  (Word64)
import           HStream.Server.Core.Common (decodeRecordBatch)
import qualified HStream.Server.HStreamApi  as API
import           HStream.Server.ReaderPool  (getReader, putReader)
import           HStream.Server.Shard       (Shard (..), ShardKey (..),
                                             SharedShardMap, cBytesToKey,
                                             mergeTwoShard, mkShard,
                                             mkSharedShardMapWithShards,
                                             shardKeyToText, splitByKey,
                                             splitHalf, textToShardKey)
import           HStream.Server.Types       (ServerContext (..),
                                             transToStreamName)
import qualified HStream.Store              as S
import           HStream.Utils
import           Proto3.Suite               (Enumerated (Enumerated))
import           Z.Data.CBytes              (CBytes)

-------------------------------------------------------------------------------

listShards
  :: HasCallStack
  => ServerContext
  -> API.ListShardsRequest
  -> IO (V.Vector API.Shard)
listShards ServerContext{..} API.ListShardsRequest{..} = do
  shards <- M.elems <$> S.listStreamPartitions scLDClient streamId
  V.foldM' constructShard V.empty $ V.fromList shards
 where
   streamId = transToStreamName listShardsRequestStreamName

   constructShard shards logId = do
     attr <- S.getStreamPartitionExtraAttrs scLDClient logId
     case getShardInfo attr of
       Nothing -> return . V.snoc shards $
         API.Shard { API.shardStreamName = listShardsRequestStreamName
                   , API.shardShardId    = logId
                   , API.shardIsActive   = True
                   }
       Just(sKey, eKey, ep) -> return . V.snoc shards $
         API.Shard { API.shardStreamName        = listShardsRequestStreamName
                   , API.shardShardId           = logId
                   , API.shardStartHashRangeKey = shardKeyToText sKey
                   , API.shardEndHashRangeKey   = shardKeyToText eKey
                   , API.shardEpoch             = ep
                   -- FIXME: neet a way to find if this shard is active
                   , API.shardIsActive          = True
                   }

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
  :: ServerContext
  -> API.SplitShardsRequest
  -> IO (V.Vector API.Shard)
splitShards ServerContext{..} API.SplitShardsRequest{..} = do
  sharedShardMp <- getShardMap scLDClient shardInfo splitShardsRequestStreamName
  newShards <- splitShard sharedShardMp
  updateShardTable newShards
  return . V.map (shardToPb splitShardsRequestStreamName) $ V.fromList newShards
 where
   splitKey = textToShardKey splitShardsRequestSplitKey

   split :: Bool -> ShardKey -> SharedShardMap -> IO (Shard, Shard)
   split True  key mps = splitHalf scLDClient mps key
   split False key mps = splitByKey scLDClient mps key

   splitShard sharedShardMp =
     modifyMVar shardInfo $ \info -> do
       (s1, s2) <- split splitShardsRequestHalfSplit splitKey sharedShardMp
       return (HM.insert splitShardsRequestStreamName sharedShardMp info, [s1, s2])

   updateShardTable newShards =
     modifyMVar_ shardTable $ \mp -> do
       let dict = fromMaybe M.empty $ HM.lookup splitShardsRequestStreamName mp
           dict' = foldl' (\acc Shard{startKey=sKey, shardId=sId} -> M.insert sKey sId acc) dict newShards
       return $ HM.insert splitShardsRequestStreamName dict' mp

mergeShards
  :: ServerContext
  -> API.MergeShardsRequest
  -> IO API.Shard
mergeShards ServerContext{..} API.MergeShardsRequest{..} = do
  sharedShardMp <- getShardMap scLDClient shardInfo mergeShardsRequestStreamName
  (newShard, removedKey) <- mergeShard sharedShardMp
  updateShardTable newShard removedKey
  return . shardToPb mergeShardsRequestStreamName $ newShard
 where
   mergeShard sharedShardMp = do
     modifyMVar shardInfo $ \info -> do
       let [shardKey1, shardKey2] = V.toList . V.map textToShardKey $ mergeShardsRequestShardKeys
       res <- mergeTwoShard scLDClient sharedShardMp shardKey1 shardKey2
       return (HM.insert mergeShardsRequestStreamName sharedShardMp info, res)

   updateShardTable Shard{startKey=sKey, shardId=sId} removedKey =
     modifyMVar_ shardTable $ \mp -> do
       let dict = fromMaybe M.empty $ HM.lookup mergeShardsRequestStreamName mp
           dict' = M.insert sKey sId dict
           dict'' = M.delete removedKey dict'
       return $ HM.insert mergeShardsRequestStreamName dict'' mp

getShardMap :: S.LDClient -> MVar (HM.HashMap Text SharedShardMap) -> Text -> IO SharedShardMap
getShardMap client shardInfo streamName = do
  let streamId = transToStreamName streamName
  readMVar shardInfo >>= pure <$> HM.lookup streamName >>= \case
    Just mps -> return mps
    Nothing  -> loadSharedShardMap client streamId

loadSharedShardMap :: S.LDClient -> S.StreamId -> IO SharedShardMap
loadSharedShardMap client streamId = do
  shardIds <- M.elems <$> S.listStreamPartitions client streamId
  mkSharedShardMapWithShards =<< foldM createShard [] shardIds
 where
   createShard acc shardId = do
     attrs <- S.getStreamPartitionExtraAttrs client shardId
     case getShardInfo attrs of
       Nothing -> return acc
       Just (sKey, eKey, epoch) -> return $ mkShard shardId streamId sKey eKey epoch : acc

getShardInfo :: M.Map CBytes CBytes -> Maybe (ShardKey, ShardKey, Word64)
getShardInfo mp = do
  startHashRangeKey <- cBytesToKey <$> M.lookup startKey mp
  endHashRangeKey   <- cBytesToKey <$> M.lookup endKey mp
  shardEpoch        <- read . CB.unpack <$> M.lookup epoch mp
  return (startHashRangeKey, endHashRangeKey, shardEpoch)
 where
   startKey = CB.pack "startKey"
   endKey   = CB.pack "endKey"
   epoch    = CB.pack "epoch"

shardToPb :: Text -> Shard -> API.Shard
shardToPb sName Shard{..} = API.Shard
  { API.shardShardId           = shardId
  , API.shardStreamName        = sName
  , API.shardStartHashRangeKey = shardKeyToText startKey
  , API.shardEndHashRangeKey   = shardKeyToText endKey
  , API.shardEpoch             = epoch
  , API.shardIsActive          = True
  }
