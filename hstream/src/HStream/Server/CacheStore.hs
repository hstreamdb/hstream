{-# LANGUAGE NamedFieldPuns #-}
module HStream.Server.CacheStore
( CachedStore
, mkCacheStore
, initStore
, deleteCacheStore
, writeRecord
, dumpToHStore
)
where

import           Control.Concurrent    (MVar, forkFinally, modifyMVar_, newMVar,
                                        readMVar)
import           Control.Exception     (throwIO)
import           Control.Monad         (void, when)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as BSC
import           Data.Int              (Int64)
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as T
import           Data.Word             (Word64)
import           System.Clock

import           Database.RocksDB
import qualified HStream.Logger        as Log
import qualified HStream.Store         as S

-- CachedStore contains all options to create and run a real rocksdb store.
-- Note that the Store is only created when it is being used.
data CachedStore = CachedStore
  { store        :: MVar (Maybe Store)
  , path         :: FilePath
  , dbOpts       :: DBOptions
  , writeOptions :: WriteOptions
  , readOptions  :: ReadOptions
  }

-- Store is actually a column family in rocksdb,
-- with the timestamp of creating the store as the name of the column family.
data Store = Store
  { db           :: DB
  , name         :: String
  , columnFamily :: ColumnFamily
  }

mkCacheStore :: FilePath -> DBOptions -> WriteOptions -> ReadOptions -> IO CachedStore
mkCacheStore path dbOpts writeOptions readOptions = do
  store <- newMVar Nothing
  return CachedStore {..}

-- Create a rocksdb store
-- Users need to ensure that they create a store before performing other operations.
initStore :: CachedStore -> IO ()
initStore CachedStore{..} = do
  modifyMVar_ store $ \st -> do
    case st of
      Just st' -> return $ Just st'
      Nothing  -> do
        db <- open dbOpts path
        name <- show <$> getCurrentTimestamp
        columnFamily <- createColumnFamily db dbOpts name
        Log.info $ "Create cached store " <> Log.build name
        return $ Just Store{..}

-- Destroy both rocksdb and columnFamily
-- Users need to ensure that all other operations are stopped before deleting the store.
deleteCacheStore :: CachedStore -> IO ()
deleteCacheStore CachedStore{store} = do
  modifyMVar_ store $ \st -> do
    case st of
      Just Store{..} -> do
        destroyColumnFamily columnFamily
        dropColumnFamily db columnFamily
        close db
        Log.info $ "Delete cached store " <> Log.build name
        return Nothing
      Nothing -> return Nothing

getStore :: CachedStore -> IO Store
getStore CachedStore{..} = do
  st <- readMVar store
  case st of
    Just st' -> return st'
    -- FIXME: handle exception
    Nothing  -> do 
      Log.fatal $ "Cached store not initialized."
      throwIO $ userError "Store is not initialized."

writeRecord :: CachedStore -> T.Text -> Word64 -> ByteString -> IO S.AppendCompletion
writeRecord st@CachedStore{..} streamName shardId payload = do
  Store{..} <- getStore st
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
dumpToHStore st@CachedStore{..} ldClient cmpStrategy = void $ forkFinally dump finalizer
 where
  dump = do
    Store{..} <- getStore st
    Log.info $ "Starting dump cached store data to HStore"
    start <- getTime Monotonic
    withIteratorCF db readOptions columnFamily $ \iter -> do
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
        Just msg -> Log.fatal $ "cached store iterator error: " <> Log.build msg
        Nothing  -> do
          end <- getTime Monotonic
          let sDuration = toNanoSecs (diffTimeSpec end start) `div` 1000000000
          Log.info $ "Finish dump cached store, total time " <> Log.build sDuration <> "s"
          return ()

  -- What if dump process error?
  finalizer (Left e)  = do
    Log.fatal $ "dump cached store to HStore failed: " <> Log.build (show e)
    return ()
  finalizer (Right _) = return ()

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
getCurrentTimestamp = fromIntegral . toNanoSecs <$> getTime Monotonic
