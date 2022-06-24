{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExistentialQuantification #-} 

module HStream.Server.Shard where

import Data.Bits (shiftL, shiftR)
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Text as T
import Data.Word (Word64)
import qualified HStream.Store as S
import Control.Exception (Exception (toException, fromException), SomeException)
import qualified Z.Data.CBytes as CB
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Typeable (cast)

type Key = Word64

---------------------------------------------------------------------------------------------------------------
---- Shard

data Shard = Shard 
  { logId    :: S.C_LogID
  , streamId :: S.StreamId
  , startKey :: Key
  , endKey   :: Key 
  } deriving(Show)

mkShard :: S.C_LogID -> S.StreamId -> Key -> Key -> Shard
mkShard logId streamId startKey endKey = Shard {logId, streamId, startKey, endKey}

splitShard :: Shard -> Key -> Either ShardException (Shard, Shard)
splitShard shard@Shard{..} key 
  | startKey > key || endKey < key = Left . ShardException $ CanNotSplit
  | startKey == key = Right (shard, shard)
    -- split at startKey will return the same shard 
  | otherwise =
      -- here key is in (startKey, endKey], so key - 1 will never casue a rewind
      let s1 = mkShard logId streamId startKey (key - 1)
          s2 = mkShard logId streamId key endKey
        in Right (s1, s2)

halfSplit :: Shard -> Either ShardException (Shard, Shard)
halfSplit shard@Shard{..} 
  | startKey == endKey = Left . ShardException $ CanNotSplit
  | otherwise = splitShard shard $ startKey + ((endKey - startKey) `div` 2)

mergeShard :: Shard -> Shard -> Either ShardException (Shard, Key)
mergeShard shard1@Shard{logId=logId1, streamId=streamId1, startKey=startKey1, endKey=endKey1} shard2@Shard{logId=logId2, streamId=streamId2, startKey=startKey2, endKey=endKey2} 
  | logId1 == logId2 = Left . ShardException $ CanNotMerge "cannot merge same shard"
  | endKey1 + 1 /= startKey2 || streamId1 /= streamId2 = Left . ShardException $ CanNotMerge "error shard"
  | startKey1 > startKey2 = mergeShard shard2 shard1
  | otherwise = let newShard = mkShard logId1 streamId1 startKey1 endKey2
                 in Right (newShard, startKey2)

---------------------------------------------------------------------------------------------------------------
---- shardMap

newtype ShardMap = ShardMap { shards :: Map Key Shard }

getShard :: ShardMap -> Key -> Maybe Shard
getShard ShardMap{..} key = snd <$> M.lookupLT key shards

getShard' :: ShardMap -> Key -> Either ShardException Shard
getShard' info key = let res = getShard info key
                      in maybeToEither res (ShardException ShardNotExist)

split :: S.LDClient -> ShardMap -> Key -> IO (Either ShardException ShardMap)
split client info@ShardMap{..} key = case getSplitedShard of
  Left e -> return $ Left e
  Right (s1, s2) -> do
    s1'@Shard{startKey=key1} <- createShard client s1
    s2'@Shard{startKey=key2} <- createShard client s2
    let infoTmp = M.insert key1 s1' shards
        newInfo = M.insert key2 s2' infoTmp
    return . Right $ info {shards = newInfo}
 where
   getSplitedShard = getShard' info key >>= flip splitShard key

merge :: S.LDClient -> ShardMap -> Key -> Key -> IO (Either ShardException ShardMap)
merge client info@ShardMap{..} key1 key2 = do
  case getMergedShard of
    Left e -> return $ Left e
    Right (s, removedKey) -> do
      newShard@Shard{startKey} <- createShard client s
      let infoTmp = M.delete removedKey shards
          newInfo = M.insert startKey newShard infoTmp
      return . Right $ info {shards = newInfo}
 where
   getMergedShard = do
     s1 <- getShard' info key1
     s2 <- getShard' info key2
     mergeShard s1 s2

---------------------------------------------------------------------------------------------------------------
---- shardMap

kNumShardBits :: Int
kNumShardBits = 4

kNumShards :: Word64
kNumShards = 1 `shiftL` kNumShardBits

data SharedShardMap = SharedShardMap 
    { shardMaps :: Vector ShardMap
    }

getShardMap :: SharedShardMap -> Word64 -> ShardMap
getShardMap SharedShardMap{..} hashValue = (V.!) shardMaps (fromIntegral hashValue)

---------------------------------------------------------------------------------------------------------------
---- helper

createShard :: S.LDClient -> Shard -> IO Shard
createShard client shard@Shard{..} = do
  newShardId <- S.createStreamPartition client streamId (Just $ getShardName startKey endKey) 
  return $ shard {logId = newShardId} 

wordToCBytes :: Integral a => a -> CB.CBytes
wordToCBytes = CB.pack . show . fromIntegral

getShardName :: Key -> Key -> CB.CBytes
getShardName startKey endKey = "shard-" <> wordToCBytes startKey <> "-" <> wordToCBytes endKey

maybeToEither :: Maybe a -> e -> Either e a
maybeToEither mb ep = case mb of
  Just v -> Right v
  Nothing -> Left ep

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
