{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.LogStore.Base
  ( -- * Exported Types
    LogName,
    Entry,
    OpenOptions (..),
    LogHandle,
    EntryID,
    Config (..),
    Context,
    LogStoreException (..),

    -- * Basic functions
    initialize,
    open,
    appendEntry,
    appendEntries,
    readEntries,
    readEntriesByCount,
    close,
    shutDown,
    withLogStore,

    -- * Options
    defaultOpenOptions,
    defaultConfig,
  )
where

import           Control.Concurrent               (MVar, modifyMVar_, newMVar,
                                                   threadDelay)
import           Control.Concurrent.Async         (Async, async, cancel)
import qualified Control.Concurrent.Classy.RWLock as RWL
import           Control.Concurrent.STM           (TVar, atomically, newTVarIO,
                                                   readTVar, retry, writeTVar)
import           Control.Exception                (bracket, throwIO)
import           Control.Monad                    (foldM, forever, when)
import           Control.Monad.IO.Class           (MonadIO, liftIO)
import           Control.Monad.Reader             (ReaderT, ask, runReaderT)
import           Control.Monad.Trans              (lift)
import           Control.Monad.Trans.Resource     (MonadUnliftIO, allocate,
                                                   runResourceT)
import qualified Data.ByteString                  as B
import qualified Data.Cache.LRU                   as L
import           Data.Default                     (def)
import qualified Data.HashMap.Strict              as H
import           Data.Hashable                    (Hashable)
import           Data.IORef                       (IORef, newIORef, readIORef,
                                                   writeIORef)
import           Data.Maybe                       (fromMaybe, isJust)
import           Data.Sequence                    (Seq (..), (><), (|>))
import qualified Data.Sequence                    as Seq
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import qualified Database.RocksDB                 as R
import           GHC.Generics                     (Generic)
import           HStream.LogStore.Exception
import           HStream.LogStore.Internal
import qualified HStream.Utils                    as U
import           System.Directory                 (createDirectoryIfMissing)
import           System.FilePath                  ((</>))

data Config = Config
  { rootDbPath             :: FilePath,
    dataCfWriteBufferSize  :: Word64,
    enableDBStatistics     :: Bool,
    dbStatsDumpPeriodSec   :: Word32,
    partitionInterval      :: Int, -- seconds
    partitionFilesNumLimit :: Int,
    maxOpenDbs             :: Int
  }

defaultConfig :: Config
defaultConfig =
  Config
    { rootDbPath = "/tmp/log-store/rocksdb",
      dataCfWriteBufferSize = 200 * 1024 * 1024,
      enableDBStatistics = False,
      dbStatsDumpPeriodSec = 600,
      partitionInterval = 60,
      partitionFilesNumLimit = 16,
      maxOpenDbs = -1
    }

data Context = Context
  { dbPath                  :: FilePath,
    metaDbHandle            :: R.DB,
    writeFlagForCurDb       :: MVar Bool,
    rwLockForCurDb          :: RWL.RWLock IO,
    curDataDbHandleRef      :: IORef R.DB,
    logHandleCache          :: TVar (H.HashMap LogHandleKey LogHandle),
    maxLogIdRef             :: IORef LogID,
    backgroundShardingTask  :: Async (),
    dbHandlesForReadCache   :: TVar (L.LRU String R.DB),
    dbHandlesForReadRcMap   :: TVar (H.HashMap String Int),
    dbHandlesForReadEvcited :: TVar (H.HashMap String R.DB)
  }

shardingTask ::
  Int ->
  Int ->
  FilePath ->
  Word64 ->
  MVar Bool ->
  RWL.RWLock IO ->
  IORef R.DB ->
  IO ()
shardingTask
  partitionInterval
  partitionFilesNumLimit
  dbPath
  cfWriteBufferSize
  writeFlag
  rwLock
  curDataDbHandleRef = forever $ do
    -- putStrLn "ready to sharding, will delay..."
    threadDelay $ partitionInterval * 1000000
    curDb <- RWL.withRead rwLock $ readIORef curDataDbHandleRef
    curDataDbFilesNum <- getFilesNumInDb curDb
    -- putStrLn $ "sharding block finish, curfileNums: " ++ show curDataDbFilesNum ++ ", partitionFilesNumLimit: " ++ show partitionFilesNumLimit
    when
      (curDataDbFilesNum >= partitionFilesNumLimit)
      ( do
          modifyMVar_ writeFlag (\_ -> return True)
          RWL.withWrite
            rwLock
            ( do
                newDbName <- generateDataDbName
                newDbHandle <- createDataDb dbPath newDbName cfWriteBufferSize
                R.flush curDb def
                R.close curDb
                -- putStrLn $ "sharding create new db: " ++ newDbName
                writeIORef curDataDbHandleRef newDbHandle
                modifyMVar_ writeFlag (\_ -> return False)
            )
      )

openMetaDb :: MonadIO m => Config -> m R.DB
openMetaDb Config {..} = do
  liftIO $ createDirectoryIfMissing True rootDbPath
  R.open
    R.defaultDBOptions
      { R.createIfMissing = True
      }
    (rootDbPath </> metaDbName)

-- init Context using Config
-- 1. open (or create) db, metaCF, create a new dataCF
-- 2. init context variables: logHandleCache, maxLogIdRef
-- 3. start background task: shardingTask
initialize :: MonadIO m => Config -> m Context
initialize cfg@Config {..} =
  liftIO $ do
    metaDb <- openMetaDb cfg

    newDataDbName <- generateDataDbName
    newDataDbHandle <- createDataDb rootDbPath newDataDbName dataCfWriteBufferSize
    newDataDbHandleRef <- newIORef newDataDbHandle

    newWriteFlag <- newMVar False
    newRWLock <- RWL.newRWLock
    logHandleCache <- newTVarIO H.empty
    maxLogId <- getMaxLogId metaDb
    logIdRef <- newIORef maxLogId
    bgShardingTask <-
      async $
        shardingTask
          partitionInterval
          partitionFilesNumLimit
          rootDbPath
          dataCfWriteBufferSize
          newWriteFlag
          newRWLock
          newDataDbHandleRef

    let cacheSize = if maxOpenDbs <= 0 then Nothing else Just $ toInteger maxOpenDbs
    dbHandlesForReadCache <- newTVarIO $ L.newLRU cacheSize
    dbHandlesForReadRcMap <- newTVarIO H.empty
    dbHandlesForReadEvcited <- newTVarIO H.empty

    return
      Context
        { dbPath = rootDbPath,
          metaDbHandle = metaDb,
          writeFlagForCurDb = newWriteFlag,
          rwLockForCurDb = newRWLock,
          curDataDbHandleRef = newDataDbHandleRef,
          logHandleCache = logHandleCache,
          maxLogIdRef = logIdRef,
          backgroundShardingTask = bgShardingTask,
          dbHandlesForReadCache = dbHandlesForReadCache,
          dbHandlesForReadRcMap = dbHandlesForReadRcMap,
          dbHandlesForReadEvcited = dbHandlesForReadEvcited
        }
  where
    getMaxLogId :: R.DB -> IO LogID
    getMaxLogId db = do
      maxLogId <- R.get db def maxLogIdKey
      case maxLogId of
        Nothing -> do
          let initLogId = 0
          R.put db def maxLogIdKey $ encodeLogId initLogId
          return initLogId
        Just v -> return (decodeLogId v)

data OpenOptions = OpenOptions
  { readMode        :: Bool,
    writeMode       :: Bool,
    createIfMissing :: Bool
  }
  deriving (Eq, Show, Generic)

instance Hashable OpenOptions

defaultOpenOptions :: OpenOptions
defaultOpenOptions =
  OpenOptions
    { readMode = True,
      writeMode = False,
      createIfMissing = False
    }

type Entry = B.ByteString

data LogHandle = LogHandle
  { logName       :: LogName,
    logID         :: LogID,
    openOptions   :: OpenOptions,
    maxEntryIdRef :: TVar EntryID
  }
  deriving (Eq)

data LogHandleKey
  = LogHandleKey LogName OpenOptions
  deriving (Eq, Show, Generic)

instance Hashable LogHandleKey

open :: MonadIO m => LogName -> OpenOptions -> ReaderT Context m LogHandle
open name opts@OpenOptions {..} = do
  Context {..} <- ask
  res <- R.get metaDbHandle def (encodeLogName name)
  let valid = checkOpts res
  if valid
    then do
      cacheRes <- lookupCache logHandleCache
      case cacheRes of
        Just lh ->
          return lh
        Nothing -> do
          newLh <- mkLogHandle res
          updateCache logHandleCache newLh
    else
      liftIO $
        throwIO $
          LogStoreLogNotFoundException $
            "no log named " ++ T.unpack name ++ " found"
  where
    logHandleKey = LogHandleKey name opts

    lookupCache :: MonadIO m => TVar (H.HashMap LogHandleKey LogHandle) -> m (Maybe LogHandle)
    lookupCache tc = liftIO $
      atomically $ do
        cache <- readTVar tc
        case H.lookup logHandleKey cache of
          Nothing -> return Nothing
          Just lh -> return $ Just lh

    updateCache :: MonadIO m => TVar (H.HashMap LogHandleKey LogHandle) -> LogHandle -> m LogHandle
    updateCache tc lh = liftIO $
      atomically $ do
        cache <- readTVar tc
        case H.lookup logHandleKey cache of
          Nothing -> do
            let newCache = H.insert logHandleKey lh cache
            writeTVar tc newCache
            return lh
          Just handle -> return handle

    mkLogHandle :: MonadIO m => Maybe B.ByteString -> ReaderT Context m LogHandle
    mkLogHandle res =
      case res of
        Nothing -> do
          logId <- create name
          maxEntryIdRef <- liftIO $ newTVarIO dumbMinEntryId
          return $
            LogHandle
              { logName = name,
                logID = logId,
                openOptions = opts,
                maxEntryIdRef = maxEntryIdRef
              }
        Just bId ->
          do
            let logId = decodeLogId bId
            r <- getMaxEntryId logId
            maxEntryIdRef <- liftIO $ newTVarIO $ fromMaybe dumbMinEntryId r
            return $
              LogHandle
                { logName = name,
                  logID = logId,
                  openOptions = opts,
                  maxEntryIdRef = maxEntryIdRef
                }

    checkOpts :: Maybe B.ByteString -> Bool
    checkOpts res =
      case res of
        Nothing ->
          createIfMissing
        Just _ -> True

-- getDataCfHandlesForRead :: FilePath -> IO (R.DB, [R.ColumnFamily])
-- getDataCfHandlesForRead dbPath = do
--   cfNames <- R.listColumnFamilies def dbPath
--   let dataCfNames = filter (/= metaCFName) cfNames
--   let dataCfDescriptors = map (\cfName -> R.ColumnFamilyDescriptor {name = cfName, options = def}) dataCfNames
--   (dbForReadOnly, handles) <-
--     R.openForReadOnlyColumnFamilies
--       def
--       dbPath
--       dataCfDescriptors
--       False
--   return (dbForReadOnly, tail handles)
--

withDbHandleForRead ::
  TVar (L.LRU String R.DB) ->
  TVar (H.HashMap String R.DB) ->
  TVar (H.HashMap String Int) ->
  FilePath ->
  String ->
  (R.DB -> IO a) ->
  IO a
withDbHandleForRead
  dbHandleCache
  dbHandlesEvicted
  dbHandleRcMap
  dbPath
  dbName =
    bracket
      ( do
          r <-
            atomically
              ( do
                  rcMap <- readTVar dbHandleRcMap
                  let rc = H.lookupDefault 0 dbName rcMap
                  let newRcMap = H.insert dbName (rc + 1) rcMap
                  writeTVar dbHandleRcMap newRcMap

                  cache <- readTVar dbHandleCache
                  let (newCache, res) = L.lookup dbName cache
                  case res of
                    Nothing ->
                      if rc == 0
                        then return Nothing
                        else do
                          gcMap <- readTVar dbHandlesEvicted
                          case H.lookup dbName gcMap of
                            Nothing -> retry
                            Just v  -> return $ Just v
                    Just v -> do
                      writeTVar dbHandleCache newCache
                      return $ Just v
              )

          case r of
            Nothing -> do
              dbHandle <-
                R.openForReadOnly
                  def
                  (dbPath </> dbName)
                  False
              _ <- insertDbHandleToCache dbHandle
              return dbHandle
            Just handle ->
              return handle
      )
      ( \_ -> do
          dbHandlesForFree <- unRef
          mapM_ R.close dbHandlesForFree
      )
    where
      insertDbHandleToCache :: R.DB -> IO (Maybe String)
      insertDbHandleToCache dbHandle =
        atomically
          ( do
              cache <- readTVar dbHandleCache
              let (newCache, evictedKV) = L.insertInforming dbName dbHandle cache
              writeTVar dbHandleCache newCache
              case evictedKV of
                Nothing -> return Nothing
                Just (k, v) -> do
                  s <- readTVar dbHandlesEvicted
                  writeTVar dbHandlesEvicted $ H.insert k v s
                  return $ Just k
          )

      unRef :: IO [R.DB]
      unRef =
        atomically $
          do
            rcMap <- readTVar dbHandleRcMap
            let rc = H.lookup dbName rcMap
            case rc of
              Just _ -> do
                let newRcMap = H.adjust (+ (-1)) dbName rcMap
                writeTVar dbHandleRcMap newRcMap

                gcMap <- readTVar dbHandlesEvicted
                let f k _ = H.lookupDefault 0 k newRcMap > 0
                let validGcMap = H.filterWithKey f gcMap
                let inValidGcMap = H.difference gcMap validGcMap
                writeTVar dbHandlesEvicted validGcMap

                let validRcMap = H.filterWithKey f newRcMap
                writeTVar dbHandleRcMap validRcMap

                return $ H.elems inValidGcMap
              Nothing -> error "this should never reach"

getMaxEntryId :: MonadIO m => LogID -> ReaderT Context m (Maybe EntryID)
getMaxEntryId logId = do
  Context {..} <- ask
  readOnlyDataDbNames <- getReadOnlyDataDbNames dbPath writeFlagForCurDb rwLockForCurDb
  foldM
    (f dbHandlesForReadCache dbHandlesForReadEvcited dbHandlesForReadRcMap dbPath)
    Nothing
    (reverse readOnlyDataDbNames)
  where
    f cache gcMap rcMap dbPath prevRes dbName =
      case prevRes of
        Nothing ->
          liftIO $
            withDbHandleForRead
              cache
              gcMap
              rcMap
              dbPath
              dbName
              (\dbForRead -> R.withIterator dbForRead def findMaxEntryIdInDb)
        Just res -> return $ Just res

    findMaxEntryIdInDb :: MonadIO m => R.Iterator -> m (Maybe EntryID)
    findMaxEntryIdInDb iterator = do
      R.seekForPrev iterator (encodeEntryKey $ EntryKey logId dumbMaxEntryId)
      isValid <- R.valid iterator
      if isValid
        then do
          entryKey <- R.key iterator
          let (EntryKey entryLogId entryId) = decodeEntryKey entryKey
          if entryLogId == logId
            then return $ Just entryId
            else return Nothing
        else do
          errStr <- R.getError iterator
          case errStr of
            Nothing -> return Nothing
            Just str -> liftIO $ throwIO $ LogStoreIOException $ "getMaxEntryId error: " ++ str

exists :: MonadIO m => LogName -> ReaderT Context m Bool
exists name = do
  Context {..} <- ask
  logId <- R.get metaDbHandle def (encodeLogName name)
  return $ isJust logId

create :: MonadIO m => LogName -> ReaderT Context m LogID
create name = do
  flag <- exists name
  if flag
    then
      liftIO $
        throwIO $
          LogStoreLogAlreadyExistsException $ "log " ++ T.unpack name ++ " already existed"
    else do
      Context {..} <- ask
      initLog metaDbHandle maxLogIdRef
  where
    initLog metaDb maxLogIdRef = do
      logId <- generateLogId metaDb maxLogIdRef
      R.put metaDb def (encodeLogName name) (encodeLogId logId)
      return logId

appendEntry :: MonadIO m => LogHandle -> Entry -> ReaderT Context m EntryID
appendEntry LogHandle {..} entry = do
  Context {..} <- ask
  if writeMode openOptions
    then liftIO $ do
      yieldWhenSeeWriteFlag writeFlagForCurDb
      RWL.withRead
        rwLockForCurDb
        ( do
            entryIds <- generateEntryIds maxEntryIdRef 1
            let entryId = head entryIds
            curDataDb <- readIORef curDataDbHandleRef
            R.put
              curDataDb
              R.defaultWriteOptions
              (encodeEntryKey $ EntryKey logID entryId)
              entry
            return entryId
        )
    else
      liftIO $
        throwIO $
          LogStoreUnsupportedOperationException $ "log named " ++ T.unpack logName ++ " is not writable."

appendEntries :: MonadIO m => LogHandle -> V.Vector Entry -> ReaderT Context m (V.Vector EntryID)
appendEntries LogHandle {..} entries = do
  Context {..} <- ask
  liftIO $ do
    yieldWhenSeeWriteFlag writeFlagForCurDb
    RWL.withRead
      rwLockForCurDb
      ( do
          curDataDb <- readIORef curDataDbHandleRef
          R.withWriteBatch $ appendEntries' curDataDb
      )
  where
    appendEntries' :: R.DB -> R.WriteBatch -> IO (V.Vector EntryID)
    appendEntries' db batch = do
      entryIds <- generateEntryIds maxEntryIdRef $ V.length entries
      let idVector = V.fromList entryIds
      mapM_
        (\(entryId, entry) -> R.batchPut batch (encodeEntryKey $ EntryKey logID entryId) entry)
        (V.zip idVector entries)
      R.write db def batch
      return idVector

-- readEntries ::
--   MonadIO m =>
--   LogHandle ->
--   Maybe EntryID ->
--   Maybe EntryID ->
--   ReaderT Context m (Serial (Entry, EntryID))
-- readEntries LogHandle {..} firstKey lastKey = do
--   Context {..} <- ask
--   dataCfNames <- getDataCfNameSet dbPath
--   streams <- mapM (readEntriesInCf resourcesForReadRef dbPath) dataCfNames
--   return $ sconcat $ fromList streams
--   where
--     readEntriesInCf resRef dbPath dataCfName =
--       liftIO $ do
--         cfr@CFResourcesForRead {..} <- openCFReadOnly dbPath dataCfName
--         atomicModifyIORefCAS resRef (\res -> (res ++ [cfr], res))
--         let kvStream = R.rangeCF dbHandleForRead def cfHandleForRead start end
--         return $
--           kvStream
--             & S.map (first decodeEntryKey)
--             -- & S.filter (\(EntryKey logId _, _) -> logId == logID)
--             & S.map (\(EntryKey _ entryId, entry) -> (entry, entryId))
--
--     start =
--       case firstKey of
--         Nothing -> Just $ encodeEntryKey $ EntryKey logID firstNormalEntryId
--         Just k -> Just $ encodeEntryKey $ EntryKey logID k
--     end =
--       case lastKey of
--         Nothing -> Just $ encodeEntryKey $ EntryKey logID maxEntryId
--         Just k -> Just $ encodeEntryKey $ EntryKey logID k

readEntries ::
  MonadIO m =>
  LogHandle ->
  Maybe EntryID ->
  Maybe EntryID ->
  ReaderT Context m (Seq (EntryID, Entry))
readEntries LogHandle {..} firstKey lastKey = do
  Context {..} <- ask
  readOnlyDataDbNames <- getReadOnlyDataDbNames dbPath writeFlagForCurDb rwLockForCurDb
  prevRes <-
    foldM
      (f dbHandlesForReadCache dbHandlesForReadEvcited dbHandlesForReadRcMap logID dbPath)
      Seq.empty
      (filterReadOnlyDb firstKey readOnlyDataDbNames)
  if goOn prevRes
    then
      liftIO $
        RWL.withRead
          rwLockForCurDb
          ( do
              curDb <- readIORef curDataDbHandleRef
              res <- R.withIterator curDb def $ readEntriesInDb logID
              return $ prevRes >< res
          )
    else return prevRes
  where
    f cache gcMap rcMap logId dbPath prevRes dataDbName =
      if goOn prevRes
        then liftIO $ do
          res <-
            withDbHandleForRead
              cache
              gcMap
              rcMap
              dbPath
              dataDbName
              (\dbForRead -> R.withIterator dbForRead def $ readEntriesInDb logId)
          return $ prevRes >< res
        else return prevRes

    goOn prevRes = Seq.null prevRes || fst (U.lastElemInSeq prevRes) < limitEntryId

    readEntriesInDb :: MonadIO m => LogID -> R.Iterator -> m (Seq (EntryID, Entry))
    readEntriesInDb logId iterator = do
      R.seek iterator (encodeEntryKey $ EntryKey logId startEntryId)
      loop logId iterator Seq.empty

    loop :: MonadIO m => LogID -> R.Iterator -> Seq (EntryID, Entry) -> m (Seq (EntryID, Entry))
    loop logId iterator acc = do
      isValid <- R.valid iterator
      if isValid
        then do
          entryKey <- R.key iterator
          let (EntryKey entryLogId entryId) = decodeEntryKey entryKey
          if entryLogId == logId && entryId <= limitEntryId
            then do
              entry <- R.value iterator
              R.next iterator
              loop logId iterator (acc |> (entryId, entry))
            else return acc
        else do
          errStr <- R.getError iterator
          case errStr of
            Nothing -> return acc
            Just str -> liftIO $ throwIO $ LogStoreIOException $ "readEntries error: " ++ str

    startEntryId = fromMaybe dumbMinEntryId firstKey
    limitEntryId = fromMaybe dumbMaxEntryId lastKey

readEntriesByCount ::
  MonadIO m =>
  LogHandle ->
  Maybe EntryID ->
  Int ->
  ReaderT Context m (Seq (EntryID, Entry))
readEntriesByCount LogHandle {..} firstKey num = do
  Context {..} <- ask
  readOnlyDataDbNames <- getReadOnlyDataDbNames dbPath writeFlagForCurDb rwLockForCurDb
  prevRes <-
    foldM
      (f dbHandlesForReadCache dbHandlesForReadEvcited dbHandlesForReadRcMap logID dbPath)
      Seq.empty
      (filterReadOnlyDb firstKey readOnlyDataDbNames)
  if goOn prevRes
    then liftIO $ do
      yieldWhenSeeWriteFlag writeFlagForCurDb
      RWL.withRead
        rwLockForCurDb
        ( do
            curDb <- readIORef curDataDbHandleRef
            res <- R.withIterator curDb def $ readEntriesInDb logID
            return $ prevRes >< res
        )
    else return prevRes
  where
    f cache gcMap rcMap logId dbPath prevRes dataDbName =
      if goOn prevRes
        then liftIO $ do
          res <-
            withDbHandleForRead
              cache
              gcMap
              rcMap
              dbPath
              dataDbName
              (\dbForRead -> R.withIterator dbForRead def $ readEntriesInDb logId)
          return $ prevRes >< res
        else return prevRes

    goOn prevRes = Seq.length prevRes < num

    readEntriesInDb :: MonadIO m => LogID -> R.Iterator -> m (Seq (EntryID, Entry))
    readEntriesInDb logId iterator = do
      R.seek iterator (encodeEntryKey $ EntryKey logId startEntryId)
      loop logId iterator Seq.empty

    loop :: MonadIO m => LogID -> R.Iterator -> Seq (EntryID, Entry) -> m (Seq (EntryID, Entry))
    loop logId iterator acc =
      if goOn acc
        then do
          isValid <- R.valid iterator
          if isValid
            then do
              entryKey <- R.key iterator
              let (EntryKey entryLogId entryId) = decodeEntryKey entryKey
              if entryLogId == logId
                then do
                  entry <- R.value iterator
                  R.next iterator
                  loop logId iterator (acc |> (entryId, entry))
                else return acc
            else do
              errStr <- R.getError iterator
              case errStr of
                Nothing -> return acc
                Just str -> liftIO $ throwIO $ LogStoreIOException $ "readEntries error: " ++ str
        else return acc

    startEntryId = fromMaybe dumbMinEntryId firstKey

filterReadOnlyDb :: Maybe EntryID -> [String] -> [String]
filterReadOnlyDb firstKey dbNames =
  case firstKey of
    Nothing -> dbNames
    Just entryId ->
      let dbTimestamps = fmap (read . drop (length dataDbNamePrefix)) dbNames
          (l, r) = span (<= timestamp entryId) dbTimestamps
          c = if null l then r else last l : r
       in fmap ((dataDbNamePrefix ++) . show) c

-- todo:
-- what should do when call close?
-- 1. free resource
-- 2. once close, should forbid operation on this LogHandle
close :: MonadIO m => LogHandle -> ReaderT Context m ()
close LogHandle {..} = return ()

shutDown :: MonadIO m => ReaderT Context m ()
shutDown = do
  Context {..} <- ask
  liftIO $ cancel backgroundShardingTask

  readOnlyDbHandles <- liftIO $ dbHandlesWaitForFree dbHandlesForReadCache dbHandlesForReadEvcited
  mapM_ R.close readOnlyDbHandles

  R.close metaDbHandle
  curDataDbHandle <- liftIO $ readIORef curDataDbHandleRef
  R.close curDataDbHandle
  where
    dbHandlesWaitForFree cache gcMap = atomically $ do
      c <- readTVar cache
      g <- readTVar gcMap
      return $ fmap snd $ L.toList c ++ H.toList g

withLogStore :: MonadUnliftIO m => Config -> ReaderT Context m a -> m a
withLogStore cfg r =
  runResourceT
    ( do
        (_, ctx) <-
          allocate
            (initialize cfg)
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )
