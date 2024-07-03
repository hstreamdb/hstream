{-# LANGUAGE BangPatterns   #-}
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

import           Control.Concurrent         (MVar, forkFinally, modifyMVar_,
                                             newMVar, readMVar, swapMVar,
                                             threadDelay)
import           Control.Concurrent.STM     (TVar, atomically, check, newTVarIO,
                                             readTVar, readTVarIO, swapTVar,
                                             writeTVar)
import           Control.Exception          (Exception (displayException),
                                             throwIO, try)
import           Control.Monad              (unless, void, when)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Char8      as BSC
import           Data.Int                   (Int64)
import           Data.IORef                 (IORef, atomicModifyIORef',
                                             atomicWriteIORef, newIORef,
                                             readIORef)
import qualified Data.List                  as L
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import           Data.Time.Clock.POSIX      (getPOSIXTime)
import           Data.Word                  (Word64)
import           System.Clock
import           Text.Printf                (printf)

import           Data.Maybe                 (fromJust, isJust)
import           Database.RocksDB
import           Database.RocksDB.Exception (RocksDbException)
import qualified HStream.Exception          as HE
import qualified HStream.Logger             as Log
import qualified HStream.Stats              as ST
import qualified HStream.Store              as S
import           HStream.Utils              (msecSince, textToCBytes)

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
  -- ^ The state of dump process
  , needResume   :: IORef Bool
  -- ^ If dumpstate changes from dumping to suspend, needResume
  -- will be set to True. This indicates that the cache store should
  -- resume dump process after dumpstate changes back to dumping
  , counter      :: IORef Word64
  , statsHolder  :: ST.StatsHolder
  }

-- Store is actually a column family in rocksdb,
-- with the timestamp of creating the store as the name of the column family.
data Store = Store
  { db            :: DB
  , name          :: String
  , columnFamily  :: ColumnFamily
  , nextKeyToDump :: IORef ByteString
  -- ^ next key to dump to hstore
  , totalWrite    :: IORef Word64
  -- ^ total records count written to the store
  , totalRead     :: IORef Word64
  -- ^ total records count read from the store
  }

data DumpState =
    NoDump    -- No dump task running
  | Dumping   -- Dump is on-going
  | Suspend   -- Dump is suspend
 deriving(Show, Eq)

mkCacheStore
  :: FilePath -> DBOptions -> WriteOptions -> ReadOptions -> ST.StatsHolder -> IO CacheStore
mkCacheStore path dbOpts writeOptions readOptions statsHolder = do
  store <- newMVar Nothing
  enableWrite <- newMVar False
  dumpState <- newTVarIO NoDump
  counter <- newIORef 0
  needResume <- newIORef False
  return CacheStore {..}

-- Create a rocksdb store
-- Users need to ensure that they create a store before performing other operations.
initCacheStore :: CacheStore -> IO ()
initCacheStore CacheStore{..} = do
  modifyMVar_ store $ \st -> do
    case st of
      Just st'@Store{..} -> do
        Log.info $ "Get cache store " <> Log.build name
        return $ Just st'
      Nothing  -> do
        Log.info $ "Open rocksdb"
        res <- try $ listColumnFamilies dbOpts path
        db <- case res of
          Left (_ :: RocksDbException) -> do
            -- `listColumnFamilies` expects the db to exist. If the call failed, we'll just assume
            -- this is a new db, relying on a subsequent `open` failing in case it's a more severe issue.
            open dbOpts path
          Right cfs -> do
            -- open db and remove existed column families
            let defaultCfIndex = fromJust $ L.elemIndex "default" cfs
            let cfs' = map (\n -> ColumnFamilyDescriptor {name = n, options = dbOpts}) cfs
            (db, cfhs) <- openColumnFamilies dbOpts path cfs'
            -- skip drop default column family
            let shouldDrop = [x | (i, x) <- zip [0..] cfhs, i /= defaultCfIndex]
            mapM_ (dropColumnFamily db) shouldDrop
            mapM_ destroyColumnFamily cfhs
            return db

        name <- show <$> getCurrentTimestamp
        columnFamily <- createColumnFamily db dbOpts name
        Log.info $ "Create cache store " <> Log.build name

        totalWrite <- newIORef 0
        totalRead <- newIORef 0
        nextKeyToDump <- newIORef BS.empty
        atomicWriteIORef counter 0
        ST.cache_store_stat_erase statsHolder "cache_store"

        return $ Just Store{..}

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
        Log.info $ "Delete cache store " <> Log.build name

        tWrites <- readIORef totalWrite
        tReads <- readIORef totalRead
        Log.info $ "CacheStore totalWrites: " <> Log.build tWrites <> ", totalReads: " <> Log.build tReads
        when (tWrites /= tReads) $ do
          Log.warning $ "CacheStore totalWrites and totalReads are not equal."

        return Nothing
      Nothing -> return Nothing

getStore :: CacheStore -> IO Store
getStore CacheStore{store} = do
  st <- readMVar store
  case st of
    Just st' -> return st'
    Nothing  -> do
      Log.fatal $ "Cache store not initialized, should not get here"
      throwIO $ HE.UnexpectedError "Store is not initialized."

writeRecord :: CacheStore -> T.Text -> Word64 -> ByteString -> IO (Either RocksDbException S.AppendCompletion)
writeRecord st@CacheStore{..} streamName shardId payload = do
  canWrite <- readMVar enableWrite
  unless canWrite $ do
    Log.warning $ "Cannot write to cache store becasue the store is not write-enabled."
    -- throw an unavailable exception to client and let it retry
    throwIO $ HE.ResourceAllocationException "CacheStore is not write-enabled."

  Store{..} <- getStore st
  -- Obtain a monotonically increasing offset to ensure that each encoded-key is unique.
  offset <- atomicModifyIORef' counter (\x -> (x + 1, x))
  let k = encodeKey streamName shardId offset
  !append_start <- getPOSIXTime
  res <- try @RocksDbException $ putCF db writeOptions columnFamily k payload
  case res of
    Left e -> do
      ST.cache_store_stat_add_cs_append_failed statsHolder cStreamName 1
      return $ Left e
    Right _ -> do
      ST.serverHistogramAdd statsHolder ST.SHL_AppendCacheStoreLatency =<< msecSince append_start
      ST.cache_store_stat_add_cs_append_in_bytes statsHolder cStreamName (fromIntegral $ BS.length payload)
      ST.cache_store_stat_add_cs_append_in_records statsHolder cStreamName 1
      ST.cache_store_stat_add_cs_append_total statsHolder cStreamName 1
      void $ atomicModifyIORef' totalWrite (\x -> (x + 1, x))
      lsn <- getCurrentTimestamp
      return . Right $
        S.AppendCompletion
          { appendCompLogID = shardId
          , appendCompLSN   = fromIntegral lsn
          , appendCompTimestamp = 0
          }
 where
   cStreamName = "cache_store"

-- dump all cached records to HStore
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
    cacheStore <- getStore st
    Log.info $ "Starting dump cached data to HStore"
    dumpLoop cacheStore
    Log.info $ "Finish dump cached data to HStore"

  dumpLoop cacheStore = do
    state <- readTVarIO dumpState
    case state of
      NoDump -> return ()
      Suspend -> do
        void . atomically $ readTVar dumpState >>= \s -> check (s == Dumping)
        dumpLoop cacheStore
      Dumping -> do
        doDump cacheStore ldClient cmpStrategy readOptions dumpState needResume statsHolder
        offset <- readIORef counter
        Log.info $ "Finish doDump, current offset: " <> Log.build (show offset)
        resume <- readIORef needResume
        when resume $ dumpLoop cacheStore

  -- What if dump process error?
  finalizer (Left e)  = do
    Log.fatal $ "dump cached data to HStore failed: " <> Log.build (show e)
    reset
  finalizer (Right _) = do
    deleteCacheStore st
    reset

  reset = do
    _ <- atomically $ swapTVar dumpState NoDump
    atomicWriteIORef needResume False

doDump
  :: Store
  -> S.LDClient
  -> S.Compression
  -> ReadOptions
  -> TVar DumpState
  -> IORef Bool
  -> ST.StatsHolder
  -> IO ()
doDump Store{..} ldClient cmpStrategy readOptions dumpState resume statsHolder = do
  start <- getTime Monotonic
  withIteratorCF db readOptions columnFamily $ \iter -> do
    atomicWriteIORef resume False
    firstKey <- readIORef nextKeyToDump
    if BS.null firstKey
    then do
      Log.info $ "Start dump cache store from beginning"
      seekToFirst iter
    else do
      Log.info $ "Dump resumed from key " <> Log.build firstKey
      seek iter firstKey

    whileM (checkContinue iter) $ do
      k <- key iter
      payload <- value iter
      void $ atomicModifyIORef' totalRead (\x -> (x + 1, x))
      ST.cache_store_stat_add_cs_read_in_bytes statsHolder cStreamName (fromIntegral $ BS.length payload + BS.length k)
      ST.cache_store_stat_add_cs_read_in_records statsHolder cStreamName 1

      atomicWriteIORef nextKeyToDump k
      let (sName, shardId) = decodeKey k
      -- TODO: How to handle LSN?
      success <- appendHStoreWithRetry ldClient sName shardId payload cmpStrategy dumpState statsHolder
      if success
        then do
          ST.cache_store_stat_add_cs_delivered_in_records statsHolder cStreamName 1
          ST.cache_store_stat_add_cs_delivered_total statsHolder cStreamName 1
        else do
          ST.cache_store_stat_add_cs_delivered_failed statsHolder cStreamName 1

      !move_iter_start <- getPOSIXTime
      next iter
      ST.serverHistogramAdd statsHolder ST.SHL_ReadCacheStoreLatency =<< msecSince move_iter_start

    -- FIXME: What if iterator return error when iterating?
    errorM <- getError iter
    case errorM of
      Just msg -> Log.fatal $ "Cache store iterator error: " <> Log.build msg
      Nothing  -> do
        end <- getTime Monotonic
        let sDuration = toNanoSecs (diffTimeSpec end start) `div` 1000000
        nextKey <- readIORef nextKeyToDump
        Log.info $ "Exit cache store iterator, total time " <> Log.build sDuration <> "ms"
                <> ", next key to dump: " <> Log.build nextKey
        return ()
 where
  cStreamName = "cache_store"
  checkContinue iter = do
    state <- readTVarIO dumpState
    iterValid <- valid iter
    return $ state == Dumping && iterValid

appendHStoreWithRetry
  :: S.LDClient
  -> T.Text
  -> Word64
  -> ByteString
  -> S.Compression
  -> TVar DumpState
  -> ST.StatsHolder
  -> IO Bool
appendHStoreWithRetry ldClient streamName shardId payload cmpStrategy dumpState statsHolder = do
  isJust <$> loop (3 :: Int)
 where
   cName = textToCBytes streamName
   -- exitNum is used to quickly exit the loop when the dump state is not dumping, avoiding more retries.
   exitNum = -99

   loop cnt
     | cnt > 0 = do
        !append_start <- getPOSIXTime
        res <- try $ S.appendCompressedBS ldClient shardId payload cmpStrategy Nothing
        case res of
          Right lsn -> do
            ST.serverHistogramAdd statsHolder ST.SHL_AppendLatency =<< msecSince append_start
            ST.stream_stat_add_append_in_bytes statsHolder cName (fromIntegral $ BS.length payload)
            ST.stream_stat_add_append_in_records statsHolder cName 1
            ST.stream_stat_add_append_total statsHolder cName 1
            return $ Just lsn
          Left (e :: S.SomeHStoreException) -> do
            ST.stream_stat_add_append_failed statsHolder cName 1
            ST.stream_time_series_add_append_failed_requests statsHolder cName 1
            state <- readTVarIO dumpState
            case state of
              Dumping -> do
                let cnt' = cnt - 1
                Log.warning $ "Dump to shardId " <> Log.build shardId <> " failed"
                           <> ", error: " <> Log.build (displayException e)
                           <> ", left retries = " <> Log.build (show cnt')
                -- sleep 1s
                threadDelay $ 1 * 1000 * 1000
                loop cnt'
              _ -> loop exitNum
     | cnt == exitNum = do
         Log.warning $ "Dump to shardId " <> Log.build shardId
                    <> " failed because cache store is not in dumping state, will retry later."
         return Nothing
     | otherwise = do
         Log.fatal $ "Dump to shardId " <> Log.build shardId
                  <> " failed after exausting the retry attempts, drop the record."
         return Nothing

-----------------------------------------------------------------------------------------------------
-- helper

setCacheStoreMode :: CacheStore -> StoreMode -> IO ()
setCacheStoreMode CacheStore{..} CacheMode = do
  void $ swapMVar enableWrite True
  resume <- atomically $ do
    state <- readTVar dumpState
    case state of
      Dumping -> writeTVar dumpState Suspend >> return True
      _       -> return False
  atomicWriteIORef needResume resume
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
  BS.concat [BSC.pack $ printf "%032d" offset, ":", T.encodeUtf8 streamName, ":", BSC.pack $ show shardId]

decodeKey :: ByteString -> (T.Text, Word64)
decodeKey bs =
  let [_ , streamName , shardId] = BSC.split ':' bs
   in (T.decodeUtf8 streamName, read $ BSC.unpack shardId)

getCurrentTimestamp :: IO Int64
getCurrentTimestamp = fromIntegral . toNanoSecs <$> getTime Monotonic
