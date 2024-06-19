{-# LANGUAGE NamedFieldPuns #-}

module HStream.Server.CacheStore
( CacheStore
, mkCacheStore
, initCacheStore
, deleteCacheStore
, writeRecord
, dumpToHStore
, StoreMode(..)
, setCacheStoreMode
)
where

import           Control.Concurrent     (MVar, forkFinally, modifyMVar_,
                                         newMVar, readMVar, swapMVar,
                                         threadDelay)
import           Control.Concurrent.STM (TVar, atomically, check, newTVarIO,
                                         readTVar, swapTVar, writeTVar)
import           Control.Exception      (Exception (displayException), throwIO,
                                         try)
import           Control.Monad          (unless, void, when)
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
import qualified Data.ByteString.Char8  as BSC
import           Data.Int               (Int64)
import           Data.IORef             (IORef, atomicModifyIORef', newIORef)
import qualified Data.Text              as T
import qualified Data.Text.Encoding     as T
import           Data.Word              (Word64)
import           System.Clock
import           Text.Printf            (printf)

import           Database.RocksDB
import qualified HStream.Logger         as Log
import qualified HStream.Store          as S
import qualified HStream.Exception as HE

-- StoreMode is a logical concept that represents the operations that the current CacheStore can perform.
--   Cache mode: data can only be written to the store
--   Dump mode: data can only be exported from the store
data StoreMode = CacheMode | DumpMode deriving(Show, Eq)

-- CacheStore contains all options to create and run a real rocksdb store.
-- Note that the Store is only created when it is being used.
data CacheStore = CacheStore
  { store        :: MVar (Maybe Store)
  , path         :: FilePath
  , dbOpts       :: DBOptions
  , writeOptions :: WriteOptions
  , readOptions  :: ReadOptions
  , enableWrite  :: MVar Bool
  , dumpState    :: TVar DumpState
  , counter      :: IORef Word64
  }

-- Store is actually a column family in rocksdb,
-- with the timestamp of creating the store as the name of the column family.
data Store = Store
  { db           :: DB
  , name         :: String
  , columnFamily :: ColumnFamily
  }

data DumpState =
    NoDump    -- No dump task running
  | Dumping   -- Dump is on-going
  | Suspend   -- Dump is suspend
 deriving(Show, Eq)

mkCacheStore :: FilePath -> DBOptions -> WriteOptions -> ReadOptions -> IO CacheStore
mkCacheStore path dbOpts writeOptions readOptions = do
  store <- newMVar Nothing
  enableWrite <- newMVar False
  dumpState <- newTVarIO NoDump
  counter <- newIORef 0
  return CacheStore {..}

-- Create a rocksdb store
-- Users need to ensure that they create a store before performing other operations.
initCacheStore :: CacheStore -> IO ()
initCacheStore CacheStore{..} = do
  modifyMVar_ store $ \st -> do
    case st of
      Just st' -> return $ Just st'
      Nothing  -> do
        db <- open dbOpts path
        Log.info $ "Open rocksdb"
        name <- show <$> getCurrentTimestamp
        columnFamily <- createColumnFamily db dbOpts name
        Log.info $ "Create cached store " <> Log.build name
        return $ Just Store{..}
  atomicModifyIORef' counter $ const (0, ())

-- Destroy both rocksdb and columnFamily
-- Users need to ensure that all other operations are stopped before deleting the store.
deleteCacheStore :: CacheStore -> IO ()
deleteCacheStore CacheStore{store} = do
  modifyMVar_ store $ \st -> do
    case st of
      Just Store{..} -> do
        dropColumnFamily db columnFamily
        Log.info $ "Drop column family"
        destroyColumnFamily columnFamily
        Log.info $ "Destroy column family"
        close db
        Log.info $ "Delete cached store " <> Log.build name
        return Nothing
      Nothing -> return Nothing

getStore :: CacheStore -> IO Store
getStore CacheStore{store} = do
  st <- readMVar store
  case st of
    Just st' -> return st'
    Nothing  -> do
      Log.fatal $ "Cached store not initialized, should not get here"
      throwIO $ HE.UnexpectedError "Store is not initialized."

writeRecord :: CacheStore -> T.Text -> Word64 -> ByteString -> IO S.AppendCompletion
writeRecord st@CacheStore{..} streamName shardId payload = do
  canWrite <- readMVar enableWrite
  unless canWrite $ do
    Log.warning $ "Cannot write to cached store becasue the store is not write-enabled."
    -- throw an unavailable exception to client and let it retry
    throwIO $ HE.ResourceAllocationException "CacheStore is not write-enabled."

  Store{..} <- getStore st
  -- Obtain a monotonically increasing offset to ensure that each encoded-key is unique.
  offset <- atomicModifyIORef' counter (\x -> (x + 1, x))
  let k = encodeKey streamName shardId offset
  putCF db writeOptions columnFamily k payload
  lsn <- getCurrentTimestamp
  return S.AppendCompletion
          { appendCompLogID = shardId
          , appendCompLSN = fromIntegral lsn
          , appendCompTimestamp = 0
          }

-- dump all cached records to HStore
-- TODO: How to notify server when dump complete?
dumpToHStore :: CacheStore -> S.LDClient -> S.Compression -> IO ()
dumpToHStore st@CacheStore{..} ldClient cmpStrategy = do
  needSpawn <- atomically $ do
    state <- readTVar dumpState
    case state of
      NoDump -> do
        writeTVar dumpState Dumping
        return True
      _ -> return False
  when (needSpawn) . void $ forkFinally dump finalizer
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
        -- TODO: What if retry failed?
        void $ appendHStoreWithRetry st ldClient shardId payload cmpStrategy
        next iter

      -- FIXME: What if iterator return error when iterating?
      errorM <- getError iter
      case errorM of
        Just msg -> Log.fatal $ "Cached store iterator error: " <> Log.build msg
        Nothing  -> do
          end <- getTime Monotonic
          let sDuration = toNanoSecs (diffTimeSpec end start) `div` 1000000
          Log.info $ "Finish dump cached store, total time " <> Log.build sDuration <> "ms"
          return ()

  -- What if dump process error?
  finalizer (Left e)  = do
    Log.fatal $ "dump cached store to HStore failed: " <> Log.build (show e)
    _ <- atomically $ swapTVar dumpState NoDump
    return ()
  finalizer (Right _) = do
    _ <- atomically $ swapTVar dumpState NoDump
    deleteCacheStore st
    return ()

appendHStoreWithRetry :: CacheStore -> S.LDClient -> Word64 -> ByteString -> S.Compression -> IO ()
appendHStoreWithRetry CacheStore{..} ldClient shardId payload cmpStrategy = do
  void $ loop (3 :: Int)
 where
   loop cnt
     | cnt >= 0 = do
        res <- try $ S.appendCompressedBS ldClient shardId payload cmpStrategy Nothing
        case res of
          Left (e :: S.SomeHStoreException) -> do
           void . atomically $ readTVar dumpState >>= \s -> check (s == Dumping)
           let cnt' = cnt - 1
           Log.warning $ "Dump to shardId " <> Log.build shardId <> " failed"
                      <> ", error: " <> Log.build (displayException e)
                      <> ", left retries = " <> Log.build (show cnt')
           -- sleep 1s
           threadDelay $ 1 * 1000 * 1000
           loop cnt'
          Right lsn -> return $ Just lsn
     | otherwise = do
       Log.fatal $ "Dump to shardId " <> Log.build shardId <> " failed after exausting the retry attempts, drop the record."
       return Nothing

-----------------------------------------------------------------------------------------------------
-- helper

setCacheStoreMode :: CacheStore -> StoreMode -> IO ()
setCacheStoreMode CacheStore{..} CacheMode = do
  void $ swapMVar enableWrite True
  void $ atomically $ do
    state <- readTVar dumpState
    case state of
      Dumping -> writeTVar dumpState Suspend
      _       -> pure ()
  Log.info $ "Set CacheStore to CacheMode"
setCacheStoreMode CacheStore{..} DumpMode = do
  void $ swapMVar enableWrite False
  void $ atomically $ do
    state <- readTVar dumpState
    case state of
      Suspend -> writeTVar dumpState Dumping
      _       -> pure ()
  Log.info $ "Set CacheStore to DumpMode"

whileM :: IO Bool -> IO () -> IO ()
whileM cond act = do
  cont <- cond
  when cont $ do
    act
    whileM cond act
{-# INLINABLE whileM #-}

encodeKey :: T.Text -> Word64 -> Word64 ->  ByteString
encodeKey streamName shardId offset =
  BS.concat [T.encodeUtf8 streamName, ":", BSC.pack $ show shardId, ":", BSC.pack $ printf "%032d" offset]

decodeKey :: ByteString -> (T.Text, Word64)
decodeKey bs =
  let (streamName : shardId : _) = BSC.split ':' bs
   in (T.decodeUtf8 streamName, read $ BSC.unpack shardId)

getCurrentTimestamp :: IO Int64
getCurrentTimestamp = fromIntegral . toNanoSecs <$> getTime Monotonic
