{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NamedFieldPuns            #-}
{-# LANGUAGE OverloadedStrings         #-}

module HStream.Server.Shard(
  Shard (..),
  ShardKey,
  mkShard,
  splitShardByKey,
  halfSplit,
  mergeShard,

  ShardMap,
  mkShardMap,
  getShard,
  insertShard,
  deleteShard,

  SharedShardMap,
  mkSharedShardMap,
  getShardMap,
  putShardMap,
  readShardMap,
  getShardByKey,
  getShardMapIdx,
  splitByKey,
  splitHalf,
  mergeTwoShard,

  ShardException,
  CanNotMerge,
  CanNotSplit,
  ShardNotExist,

  hashShardKey,
) where

import           Control.Concurrent.STM (STM, TMVar, atomically,
                                         newEmptyTMVarIO, putTMVar, readTMVar,
                                         swapTMVar, takeTMVar)
import           Control.Exception      (Exception (fromException, toException),
                                         SomeException, bracket, throwIO)
import qualified Crypto.Hash            as CH
import           Data.Bits              (shiftL, shiftR, (.|.))
import qualified Data.ByteArray         as BA
import qualified Data.ByteString        as B
import           Data.Foldable          (foldl')
import           Data.Hashable          (Hashable (hash))
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as M
import qualified Data.Text              as T
import           Data.Typeable          (cast)
import           Data.Vector            (Vector)
import qualified Data.Vector            as V
import           Data.Word              (Word32, Word64)
import qualified HStream.Logger         as Log
import qualified HStream.Store          as S
import qualified Z.Data.CBytes          as CB

type ShardKey = Integer

hashShardKey :: B.ByteString -> ShardKey
hashShardKey key =
  let w8KeyList = BA.unpack (CH.hash key :: CH.Digest CH.MD5)
   in foldl' (\acc c -> (.|.) (acc `shiftL` 8) (fromIntegral c)) (0 :: Integer) w8KeyList

keyToCBytes :: ShardKey -> CB.CBytes
keyToCBytes = CB.pack . show

---------------------------------------------------------------------------------------------------------------
---- Shard

data Shard = Shard
  { logId    :: S.C_LogID
  , streamId :: S.StreamId
  , startKey :: ShardKey
  , endKey   :: ShardKey
  , epoch    :: Word64
  } deriving(Show)

mkShard :: S.C_LogID -> S.StreamId -> ShardKey -> ShardKey -> Word64 -> Shard
mkShard logId streamId startKey endKey epoch = Shard {logId, streamId, startKey, endKey, epoch}

splitShardByKey :: Shard -> ShardKey -> Either ShardException (Shard, Shard)
splitShardByKey shard@Shard{..} key
  | startKey > key || endKey < key = Left . ShardException $ CanNotSplit
  | startKey == key = Right (shard, shard)
    -- split at startKey will return the same shard
  | otherwise =
      let newEpoch = epoch + 2
      -- here key is in (startKey, endKey], so key - 1 will never casue a rewind
          s1 = mkShard logId streamId startKey (key - 1) newEpoch
          s2 = mkShard logId streamId key endKey newEpoch
        in Right (s1, s2)

halfSplit :: Shard -> Either ShardException (Shard, Shard)
halfSplit shard@Shard{..}
  | startKey == endKey = Left . ShardException $ CanNotSplit
  | otherwise = splitShardByKey shard $ startKey + ((endKey - startKey) `div` 2)

mergeShard :: Shard -> Shard -> Either ShardException (Shard, ShardKey)
mergeShard shard1@Shard{logId=logId1, streamId=streamId1, startKey=startKey1, endKey=endKey1, epoch=epoch1}
           shard2@Shard{logId=logId2, streamId=streamId2, startKey=startKey2, endKey=endKey2, epoch=epoch2}
  | logId1 == logId2 = Left . ShardException $ CanNotMerge "can't merge same shard"
  | endKey1 + 1 /= startKey2 || streamId1 /= streamId2 = Left . ShardException $ CanNotMerge "can't merge non-adjacent shards"
    -- always let shard1 before shard2, so that after merge, the new shard's key range is [start1, end2],
    -- and always remove startkey2 from shardMap
  | startKey1 > startKey2 = mergeShard shard2 shard1
  | otherwise = let newEpoch = max epoch1 epoch2 + 1
                    newShard = mkShard logId1 streamId1 startKey1 endKey2 newEpoch
                 in Right (newShard, startKey2)

---------------------------------------------------------------------------------------------------------------
---- shardMap

type ShardMap = Map ShardKey Shard

mkEmptyShardMap :: ShardMap
mkEmptyShardMap = M.empty

mkShardMap :: [(ShardKey, Shard)] -> ShardMap
mkShardMap = M.fromList

getShard :: ShardMap -> ShardKey -> Maybe Shard
getShard mp key = snd <$> M.lookupLE key mp

getShard' :: ShardMap -> ShardKey -> Either ShardException Shard
getShard' info key = let res = getShard info key
                      in maybeToEither res (ShardException ShardNotExist)

getSplitedShard :: ShardMap -> ShardKey -> Either ShardException (Shard, Shard)
getSplitedShard mp key = getShard' mp key >>= flip splitShardByKey key

getHalfSplitedShard :: ShardMap -> ShardKey -> Either ShardException (Shard, Shard)
getHalfSplitedShard mp key = getShard' mp key >>= halfSplit

getMergedShard :: ShardMap -> ShardKey -> ShardMap -> ShardKey -> Either ShardException (Shard, ShardKey)
getMergedShard mp1 key1 mp2 key2 = do
  s1 <- getShard' mp1 key1
  s2 <- getShard' mp2 key2
  mergeShard s1 s2

-- | insert a new shard into ShardMap, if the shardKey is already in the map, then new shard's epoch need to
--   greater than the exist shard. Otherwise the insertion will be ignore.
insertShard :: ShardKey -> Shard -> ShardMap -> ShardMap
insertShard key shard@Shard{epoch=epoch} mp =
  case M.lookup key mp of
    Just Shard{epoch=oEpoch} | epoch <= oEpoch -> mp
                             | otherwise -> M.insert key shard mp
    Nothing -> M.insert key shard mp

deleteShard :: ShardKey -> ShardMap -> ShardMap
deleteShard = M.delete

---------------------------------------------------------------------------------------------------------------
---- sharedShardMap

kNumShardBits :: Int
kNumShardBits = 4

kNumShards :: Int
kNumShards = 1 `shiftL` kNumShardBits

-- | A SharedShardMap is a vector with `kNumShards` slots. Each slot stores a ShardMap. for each Shard,
--   first use `getShardMapIdx key` to find which slot the ShardMap managing that Shard is stored in, then
--   you can safely manipulate that ShardMap under the protection of TMVar.
newtype SharedShardMap = SharedShardMap
  { shardMaps :: Vector (TMVar ShardMap) }

mkSharedShardMap :: IO SharedShardMap
mkSharedShardMap = do shardMaps <- V.replicateM kNumShards newEmptyTMVarIO
                      return SharedShardMap {shardMaps}

getShardMapIdx :: ShardKey -> Word32
getShardMapIdx key = fromIntegral (hash key) `shiftR` (32 - kNumShardBits)

getShardMap :: SharedShardMap -> Word32 -> STM ShardMap
getShardMap SharedShardMap{..} hashValue = takeTMVar $ (V.!) shardMaps (fromIntegral hashValue)

putShardMap :: SharedShardMap -> Word32 -> ShardMap -> STM ()
putShardMap SharedShardMap{..} hashValue = putTMVar ((V.!) shardMaps (fromIntegral hashValue))

readShardMap :: SharedShardMap -> Word32 -> STM ShardMap
readShardMap SharedShardMap{..} hashValue = readTMVar $ (V.!) shardMaps (fromIntegral hashValue)

modifyShardMap :: SharedShardMap -> Word32 -> ShardMap -> STM ShardMap
modifyShardMap SharedShardMap{..} hashValue = swapTMVar ((V.!) shardMaps (fromIntegral hashValue))

getShardByKey :: SharedShardMap -> ShardKey -> IO (Maybe Shard)
getShardByKey mp key = do
  let hashValue = getShardMapIdx key
  shardMp <- atomically $ readShardMap mp hashValue
  return $ getShard shardMp key

type SplitStrategies = ShardMap -> ShardKey -> Either ShardException (Shard, Shard)

-- | Split Shard with specific ShardKey
splitByKey :: S.LDClient -> SharedShardMap -> ShardKey -> IO ()
splitByKey = splitShardInternal getSplitedShard

-- | Split Shard by half
splitHalf :: S.LDClient -> SharedShardMap -> ShardKey -> IO ()
splitHalf = splitShardInternal getHalfSplitedShard

splitShardInternal :: SplitStrategies -> S.LDClient -> SharedShardMap -> ShardKey -> IO ()
splitShardInternal stratege client sharedMp key = do
  let hash1 = getShardMapIdx key
  bracket
    (atomically $ getShardMap sharedMp hash1)
    (atomically . putShardMap sharedMp hash1)
    (\originShardMp -> do
      case stratege originShardMp key of
        Left e         -> throwIO e
        Right (s1, s2) -> do
          s1'@Shard{startKey=key1} <- createShard client s1
          s2'@Shard{startKey=key2} <- createShard client s2
          Log.info $ "Split key " <> Log.buildString' key <> " into two new shards: "
                  <> Log.buildString' (show s1') <> " and "
                  <> Log.buildString' (show s2')

          let hash1' = getShardMapIdx key1
          let hash2' = getShardMapIdx key2
          if hash2' == hash1'
            then do
              -- After split, two new shard are still managed by same shardMap,
              let newShardMp = insertMultiShardToMap originShardMp [s1', s2']
              Log.debug $ "After split " <> Log.buildString' key <> ", "
                       <> "update shardMp " <> Log.buildString' (show newShardMp)
              atomically $ putShardMap sharedMp hash1' newShardMp
            else do
              -- The two new shards are managed by different shardMap, so they
              -- need to be updated separately
              mp2 <- atomically $ getShardMap sharedMp hash2'
              let newMp1 = M.insert key1 s1' originShardMp
                  newMp2 = M.insert key2 s2' mp2
              Log.debug $ "After split " <> Log.buildString' key <> ", "
                       <> "update shardMp " <> Log.buildString' (show newMp1)
                       <> " and " <> Log.buildString' (show newMp2)
              atomically $ do
                putShardMap sharedMp hash1' newMp1
                putShardMap sharedMp hash2' newMp2
    )

mergeTwoShard :: S.LDClient -> SharedShardMap -> ShardKey -> ShardKey -> IO ()
mergeTwoShard client mp key1 key2 = do
  let hash1 = getShardMapIdx key1
  let hash2 = getShardMapIdx key2

  bracket
    (getShards hash1 hash2)
    (cleanUp hash1 hash2)
    (\(shardMp1, shardMp2) -> do
      case getMergedShard shardMp1 key1 shardMp2 key2 of
        Left e                -> throwIO e
        Right (s, removedKey) -> do
          newShard@Shard{startKey} <- createShard client s
          Log.info $ "Merge " <> Log.buildString' key1 <> " and " <> Log.buildString' key2 <> " into "
                  <> Log.buildString' (show newShard)

          let (removedShardMp, updateShardMp) = updateShardMap startKey removedKey newShard shardMp1 shardMp2
          Log.debug $ "After merge " <> Log.buildString' key1 <> " and " <> Log.buildString' key2 <> ","
                   <> " removedShardMp=" <> Log.buildString' (show removedShardMp)
                   <> " newShardMp=" <> Log.buildString' (show updateShardMp)
          atomically $ do
            putShardMap mp (getShardMapIdx removedKey) removedShardMp
            putShardMap mp (getShardMapIdx startKey) updateShardMp
    )
 where
   getShards hash1 hash2
     | hash1 == hash2 = do
        shardMap <- atomically $ getShardMap mp hash1
        return (shardMap, shardMap)
     | otherwise = atomically $ do
         mp1 <- getShardMap mp hash1
         mp2 <- getShardMap mp hash2
         return (mp1, mp2)

   cleanUp hash1 hash2 (mp1, mp2)
     | hash1 == hash2 = atomically $ putShardMap mp hash1 mp1
     | otherwise = atomically $ do
         putShardMap mp hash1 mp1
         putShardMap mp hash2 mp2

   -- key1 -> shardMp1{startKey1, endKey1}, key2 -> shardMp2{startKey2, endKey2}
   -- getMergedShard always return a new shard with key range [start1, end2], so need to
   -- remove startKey2 from shardMp2, update startKey1 from shardMp1
   updateShardMap startKey removedKey newShard shardMp1 shardMp2
     | startKey == key1 && removedKey == key2 =
        let rmShard = M.delete removedKey shardMp2
            upShard = M.insert startKey newShard shardMp1
        in (rmShard, upShard)
     | otherwise = updateShardMap removedKey startKey newShard shardMp2 shardMp1

---------------------------------------------------------------------------------------------------------------
---- helper

createShard :: S.LDClient -> Shard -> IO Shard
createShard client shard@Shard{..} = do
  let attr = M.fromList [("startKey", keyToCBytes startKey), ("endKey", keyToCBytes endKey), ("epoch", CB.pack . show $ epoch)]
  newShardId <- S.createStreamPartitionWithExtrAttr client streamId (Just $ getShardName startKey endKey) attr
  return $ shard {logId = newShardId}

getShardName :: ShardKey -> ShardKey -> CB.CBytes
getShardName startKey endKey = "shard-" <> keyToCBytes startKey <> "-" <> keyToCBytes endKey

maybeToEither :: Maybe a -> e -> Either e a
maybeToEither mb ep = case mb of
  Just v  -> Right v
  Nothing -> Left ep

insertMultiShardToMap :: ShardMap -> [Shard] -> ShardMap
insertMultiShardToMap = foldl' (\acc s@Shard{startKey} -> M.insert startKey s acc)

---------------------------------------------------------------------------------------------------------------
---- shardException

data ShardException = forall e . Exception e => ShardException e

instance Show ShardException where
  show (ShardException e) = show e

instance Exception ShardException

shardExceptionToException :: Exception e => e -> SomeException
shardExceptionToException = toException . ShardException

shardExceptionFromException :: Exception e => SomeException -> Maybe e
shardExceptionFromException x = do
  fromException @ShardException x >>= cast

data CanNotSplit = CanNotSplit
  deriving(Show)
instance Exception CanNotSplit where
  toException   = shardExceptionToException
  fromException = shardExceptionFromException

newtype CanNotMerge = CanNotMerge T.Text
  deriving(Show)
instance Exception CanNotMerge where
  toException   = shardExceptionToException
  fromException = shardExceptionFromException

data ShardNotExist = ShardNotExist
  deriving(Show)
instance Exception ShardNotExist where
  toException   = shardExceptionToException
  fromException = shardExceptionFromException
