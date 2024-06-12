module HStream.Server.CacheStore
( mkCacheStore
, deleteCacheStore
, writeRecord
, dumpToHStore
)
where

import           Control.Concurrent    (forkFinally)
import           Control.Monad         (void, when)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as BSC
import           Data.Int              (Int64)
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as T
import           Data.Time.Clock.POSIX (getPOSIXTime)
import           Data.Word             (Word64)

import           Database.RocksDB
import qualified HStream.Store         as S

-- The CachedStore is actually a column family in rocksdb,
-- with the timestamp of creating the store as the name of the column family.
data CachedStore = CachedStore
  { db           :: DB
  , name         :: String
  , columnFamily :: ColumnFamily
  , writeOptions :: WriteOptions
  , readOptions  :: ReadOptions
  }

mkCacheStore :: FilePath -> DBOptions -> WriteOptions -> ReadOptions -> IO CachedStore
mkCacheStore path dbOpts writeOptions readOptions = do
  db <- open dbOpts path
  name <- show <$> getCurrentTimestamp
  columnFamily <- createColumnFamily db dbOpts name
  return CachedStore{..}

deleteCacheStore :: CachedStore -> IO ()
deleteCacheStore CachedStore{..} = do
  destroyColumnFamily columnFamily
  dropColumnFamily db columnFamily
  close db

writeRecord :: CachedStore -> T.Text -> Word64 -> ByteString -> IO S.AppendCompletion
writeRecord CachedStore{..} streamName shardId payload = do
  let k = encodeKey streamName shardId
  putCF db writeOptions columnFamily k payload
  lsn <- getCurrentTimestamp
  return S.AppendCompletion
          { appendCompLogID = shardId
          , appendCompLSN = fromIntegral lsn
          , appendCompTimestamp = 0
          }

-- dump all cached records to HStore
-- TODO: How to notify server when dump complete?
dumpToHStore :: CachedStore -> S.LDClient -> S.Compression -> IO ()
dumpToHStore CachedStore{..} ldClient cmpStrategy = void $ forkFinally dump finalizer
 where
  dump =
    withIteratorCF db readOptions columnFamily $ \ iter -> do
      seekToFirst iter

      whileM (valid iter) $ do
        k <- key iter
        payload <- value iter
        let (_, shardId) = decodeKey k
        -- TODO: How to handle LSN?
        -- FIXME: Retry when error
        void $ S.appendCompressedBS ldClient shardId payload cmpStrategy Nothing
        next iter

      -- FIXME: What if iterator return error when iterating?
      errorM <- getError iter
      case errorM of
        Just msg -> undefined
        Nothing  -> return ()

  finalizer (Left _)  = undefined
  finalizer (Right _) = undefined

whileM :: IO Bool -> IO () -> IO ()
whileM cond act = do
  cont <- cond
  when cont $ do
    act
    whileM cond act
{-# INLINABLE whileM #-}

encodeKey :: T.Text -> Word64 -> ByteString
encodeKey streamName shardId = BS.concat [T.encodeUtf8 streamName, ":", BSC.pack $ show shardId]

decodeKey :: ByteString -> (T.Text, Word64)
decodeKey bs =
  let (textBs, rest) = BS.breakSubstring ":" bs
      -- Remove the colon from the start of the rest and convert the remainder to a Word64
      shardIdBs = BSC.drop 1 rest
  in (T.decodeUtf8 textBs, read $ BSC.unpack shardIdBs)

getCurrentTimestamp :: IO Int64
getCurrentTimestamp = round <$> getPOSIXTime
