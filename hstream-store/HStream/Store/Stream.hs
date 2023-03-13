{-# LANGUAGE CPP             #-}
{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE MultiWayIf      #-}
{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Stream
  ( -- * Stream
    StreamSettings (..)
  , updateGloStreamSettings
  , StreamType (..)
  , StreamId (streamType, streamName)
  , ArchivedStream (getArchivedStreamName)
  , isArchiveStreamName
  , showStreamName
  , mkStreamId
  , mkStreamIdFromFullLogDir
  , getStreamLogAttrs
  , getStreamPartitionHeadTimestamp
  , getStreamPartitionExtraAttrs
    -- ** Operations
  , createStream
  , createStreamPartition
  , renameStream
  , renameStream'
  , archiveStream
  , doesArchivedStreamExist
  , unArchiveStream
  , removeArchivedStream
  , removeStream
  , findStreams
  , doesStreamExist
  , listStreamPartitions
  , doesStreamPartitionExist
  , getStreamExtraAttrs
  , updateStreamExtraAttrs
    -- ** helpers
  , getUnderlyingLogId
  , getStreamIdFromLogId

    -- * Logdevice
  , FFI.LogID (..)
  , FFI.C_LogID
  , FFI.LDLogGroup
  , FFI.LDDirectory
  , FFI.NodeLocationScope (..)
  , pattern FFI.NodeLocationScope_NODE
  , pattern FFI.NodeLocationScope_RACK
  , pattern FFI.NodeLocationScope_ROW
  , pattern FFI.NodeLocationScope_CLUSTER
  , pattern FFI.NodeLocationScope_DATA_CENTER
  , pattern FFI.NodeLocationScope_REGION
  , pattern FFI.NodeLocationScope_ROOT
  , pattern FFI.NodeLocationScope_INVALID

    -- * Log attributes
  , LD.Attribute (..)
  , LD.LogAttributes (..)
  , LD.defAttr1

    -- * Logdevice Writer
  , LD.append
  , LD.appendBS
  , LD.appendCompressedBS
  , LD.appendBatch
  , LD.appendBatchBS
  , LD.AppendCompletion (..)
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable
  , pattern FFI.KeyTypeUndefined
  , FFI.Compression (..)

    -- * Logdevice Checkpoint Store
  , FFI.LDCheckpointStore
  , initCheckpointStoreLogID
  , checkpointStoreLogID
  , LD.newFileBasedCheckpointStore
  , LD.newRSMBasedCheckpointStore
  , LD.newZookeeperBasedCheckpointStore
  , LD.ckpStoreGetLSN
  , LD.ckpStoreUpdateLSN
  , LD.ckpStoreRemoveCheckpoints
  , LD.ckpStoreRemoveAllCheckpoints

    -- * Logdevice Reader
  , FFI.RecordByteOffset (..)
  , FFI.DataRecord (..)
  , FFI.DataRecordAttr (..)
  , FFI.GapRecord (..)
  , FFI.GapType (..)
  , FFI.recordLogID
  , FFI.recordLSN
  , FFI.recordTimestamp
  , FFI.recordBatchOffset
  , FFI.recordByteOffset
  , FFI.LDReader
  , LD.newLDReader
  , LD.readerStartReading
  , LD.DataRecordFormat
  , LD.readerRead
  , LD.readerReadAllowGap
  , LD.readerSetTimeout
  , LD.readerSetWithoutPayload
  , LD.readerSetIncludeByteOffset
  , LD.readerSetWaitOnlyWhenNoData
  , LD.readerStopReading
    -- ** Checkpointed Reader
  , FFI.LDSyncCkpReader
  , newLDFileCkpReader
  , newLDRsmCkpReader
  , newLDZkCkpReader
  , LD.ckpReaderStartReading
  , LD.startReadingFromCheckpoint
  , LD.startReadingFromCheckpointOrStart
  , LD.ckpReaderRead
  , LD.ckpReaderReadAllowGap
  , LD.ckpReaderSetTimeout
  , LD.ckpReaderSetWithoutPayload
  , LD.ckpReaderSetIncludeByteOffset
  , LD.ckpReaderSetWaitOnlyWhenNoData
  , LD.ckpReaderStopReading
  , LD.writeCheckpoints
  , LD.writeLastCheckpoints
  , LD.removeCheckpoints
  , LD.removeAllCheckpoints

    -- * Internal helpers
  , getStreamDirPath
  , getStreamLogPath

    -- * Re-export
  , def
  ) where

import           Control.Exception                (try)
import           Control.Monad                    (filterM, forM, (<=<))
import           Data.Bifunctor                   (bimap, second)
import           Data.Bits                        (bit)
import           Data.Default                     (def)
import           Data.Foldable                    (foldrM)
import           Data.Hashable                    (Hashable)
import           Data.Int                         (Int64)
import           Data.IORef                       (IORef, atomicModifyIORef',
                                                   newIORef, readIORef)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Foreign.C                        (CSize)
import           Foreign.ForeignPtr               (withForeignPtr)
import           GHC.Generics                     (Generic)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
#if !MIN_VERSION_filepath(1, 4, 100)
import qualified System.FilePath.Posix            as P
#else
import qualified System.OsPath.Posix              as P
import qualified System.OsString.Internal.Types   as P
#endif
import qualified Z.Data.Builder                   as ZB
import qualified Z.Data.CBytes                    as CBytes
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.Text                      as ZT
import qualified Z.Data.Vector                    as ZV

import           HStream.Base                     (genUnique)
import           HStream.Base.Bytes               (cbytes2sbs, cbytes2sbsUnsafe,
                                                   sbs2cbytes, sbs2cbytesUnsafe)
import qualified HStream.Logger                   as Log
import qualified HStream.Store.Exception          as E
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Internal.Types     as FFI

-- Comment this to disable local cache
-- #define HSTREAM_USE_LOCAL_STREAM_CACHE

#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
import           Control.Concurrent               (MVar, modifyMVar_, newMVar,
                                                   readMVar)
import           Control.Exception                (finally)
import           Control.Monad                    (forM_)
import qualified Data.Cache                       as Cache
import           Data.Maybe                       (isJust)
#endif

-------------------------------------------------------------------------------

data StreamSettings = StreamSettings
  { streamNameLogDir    :: CBytes
  , streamViewLogDir    :: CBytes
  , streamTempLogDir    :: CBytes
  , streamDefaultKey    :: CBytes
  , streamArchivePrefix :: CBytes
  }

gloStreamSettings :: IORef StreamSettings
gloStreamSettings = unsafePerformIO . newIORef $
  -- NOTE: no trailing slash
  StreamSettings { streamNameLogDir = "/hstream/stream"
                 , streamViewLogDir = "/hstream/view"
                 , streamTempLogDir = "/tmp/hstream"
                 , streamDefaultKey = "__default_key__"
                 , streamArchivePrefix = "__archive__"
                 }
{-# NOINLINE gloStreamSettings #-}

updateGloStreamSettings :: (StreamSettings -> StreamSettings)-> IO ()
updateGloStreamSettings f = atomicModifyIORef' gloStreamSettings $ \s -> (f s, ())

-------------------------------------------------------------------------------

#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE

-- StreamId : { streamKey: logid }
type StreamCache = Cache.Cache StreamId (Map CBytes FFI.C_LogID)

-- | Global logdir path to logid cache
--
-- Note: for a cluster version, to make the cache work properly, all operations
-- of one stream (e.g. create, delete, ...) must be processed by only one
-- specific server.
gloLogPathCache :: MVar StreamCache
gloLogPathCache = unsafePerformIO $ newMVar =<< Cache.newCache Nothing
{-# NOINLINE gloLogPathCache #-}

getGloLogPathCache :: StreamId -> CBytes -> IO (Maybe FFI.C_LogID)
getGloLogPathCache streamid key = do
  cache <- readMVar gloLogPathCache
  m_v <- Cache.lookup' cache streamid
  case m_v of
    Nothing -> pure Nothing
    Just mp -> pure $ Map.lookup key mp

doesCacheExist :: StreamId -> IO Bool
doesCacheExist streamid = do
  cache <- readMVar gloLogPathCache
  isJust <$> Cache.lookup' cache streamid

updateGloLogPathCache :: StreamId -> CBytes -> FFI.C_LogID -> IO ()
updateGloLogPathCache streamid key logid =
  modifyMVar_ gloLogPathCache $ \c -> do
    m_v <- Cache.lookup' c streamid
    case m_v of
      Nothing -> Cache.insert c streamid (Map.singleton key logid) >> pure c
      Just mp -> Cache.insert c streamid (Map.insert key logid mp) >> pure c

#endif

-------------------------------------------------------------------------------

newtype ArchivedStream = ArchivedStream { getArchivedStreamName :: CBytes}

isArchiveStreamName :: StreamName -> IO Bool
isArchiveStreamName name = do
  prefix <- CBytes.toBytes . streamArchivePrefix <$> readIORef gloStreamSettings
  let name' = CBytes.toBytes name
  pure $ prefix `ZV.isPrefixOf` name'

-- ArchivedStreamName format: "prefix + name + unique word64"
toArchivedStreamName :: StreamId -> IO CBytes
toArchivedStreamName StreamId{..} = do
  -- After a stream has been archived, a new stream with the same name as the
  -- previous one is allowed to be created. The unique suffix is used to ensure
  -- that the following scenarios are executed correctly:
  --    step1. create stream "test"
  --    step2. archive stream "test"
  --    step3. create stream "test"
  --    step4. archive stream "test"
  -- If the suffix isn't unique, step4 will failed.
  suffix <- CBytes.buildCBytes . ZB.int <$> genUnique
  already <- isArchiveStreamName streamName
  if already
     then pure $ streamName <> suffix
     else do prefix <- streamArchivePrefix <$> readIORef gloStreamSettings
             pure $ prefix <> streamName <> suffix

-------------------------------------------------------------------------------

data StreamType = StreamTypeStream | StreamTypeView | StreamTypeTemp
  deriving (Show, Eq, Generic)

instance Hashable StreamType

type StreamName = CBytes

data StreamId = StreamId
  { streamType :: StreamType
  , streamName :: StreamName
  -- ^ A stream name is an identifier of the stream.
  -- The first character of the StreamName should not be '/'.
  } deriving (Show, Eq, Generic)

instance Hashable StreamId

-- TODO: validation, a stream name should not:
-- 1. Contains '/'
-- 2. Start with "__"
mkStreamId :: StreamType -> CBytes -> StreamId
mkStreamId = StreamId

mkStreamIdFromFullLogDir :: StreamType -> CBytes -> IO StreamId
mkStreamIdFromFullLogDir streamType path = do
  s <- readIORef gloStreamSettings
  let name =
        case streamType of
          StreamTypeStream -> t2 P.makeRelative (streamNameLogDir s) path
          StreamTypeView   -> t2 P.makeRelative (streamViewLogDir s) path
          StreamTypeTemp   -> t2 P.makeRelative (streamTempLogDir s) path
  return $ StreamId streamType name

showStreamName :: StreamId -> String
showStreamName = CBytes.unpack . streamName

-------------------------------------------------------------------------------

-- | Create stream
--
-- Currently a Stream is a logdir.
createStream
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> LD.LogAttributes
  -> IO ()
createStream client streamid attrs = do
  path <- getStreamDirPath streamid
  dir <- LD.makeLogDirectory client path attrs True
  LD.syncLogsConfigVersion client =<< LD.logDirectoryGetVersion dir

-- | Create a partition of a stream. If the stream doesn't exist, throw
-- StoreError.
--
-- Currently a stream partition is a loggroup which only contains one random
-- logid.
createStreamPartition
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> Map CBytes CBytes
  -> IO FFI.C_LogID
createStreamPartition client streamid m_key attr = do
  stream_exist <- doesStreamExist client streamid
#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
  if stream_exist
     then do (log_path, key) <- getStreamLogPath streamid m_key
             logid <- createRandomLogGroup client log_path def{LD.logAttrsExtras = attr}
             updateGloLogPathCache streamid key logid
             pure logid
     else E.throwStoreError ("No such stream: " <> ZT.pack (showStreamName streamid))
                            callStack
#else
  if stream_exist
     then do (log_path, _key) <- getStreamLogPath streamid m_key
             createRandomLogGroup client log_path def{LD.logAttrsExtras = attr}
     else E.throwStoreError ("No such stream: " <> ZT.pack (showStreamName streamid))
                            callStack
#endif

renameStream
  :: HasCallStack
  => FFI.LDClient
  -> StreamType
  -> CBytes
  -- ^ The source stream name to rename
  -> CBytes
  -- ^ The new stream name you are renaming to
  -> IO ()
renameStream client streamType srcName destName =
  _renameStream_ client (mkStreamId streamType srcName) (mkStreamId streamType destName)

renameStream'
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -- ^ The source stream to rename
  -> CBytes
  -- ^ The new stream name you are renaming to
  -> IO ()
renameStream' client from destName =
  _renameStream_ client from (mkStreamId (streamType from) destName)

_renameStream_ :: HasCallStack => FFI.LDClient -> StreamId -> StreamId -> IO ()
_renameStream_ client from to = do
  from' <- getStreamDirPath from
  to'   <- getStreamDirPath to
#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
  modifyMVar_ gloLogPathCache $ \cache -> do
    m_v <- Cache.lookup' cache from
    finally (LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from' to')
            (Cache.delete cache from)
    forM_ m_v $ Cache.insert cache to
    pure cache
#else
  LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from' to'
#endif
{-# INLINABLE _renameStream_ #-}

-- | Archive a stream, then you won't find it by 'findStreams'.
-- Return the archived stream, so that you can unarchive or remove it.
--
-- Note that all operations depend on logid will work as expected.
archiveStream :: HasCallStack => FFI.LDClient -> StreamId -> IO ArchivedStream
archiveStream client streamid = do
  archiveStreamName <- toArchivedStreamName streamid
  renameStream' client streamid archiveStreamName
  return $ ArchivedStream archiveStreamName

doesArchivedStreamExist :: HasCallStack => FFI.LDClient -> ArchivedStream -> IO Bool
doesArchivedStreamExist client (ArchivedStream name) =
  doesStreamExist client $ StreamId StreamTypeStream name

unArchiveStream
  :: HasCallStack => FFI.LDClient -> ArchivedStream -> StreamName -> IO ()
unArchiveStream client (ArchivedStream name) = renameStream client StreamTypeStream name

removeArchivedStream :: HasCallStack => FFI.LDClient -> ArchivedStream -> IO ()
removeArchivedStream client (ArchivedStream name) =
  removeStream client $ StreamId StreamTypeStream name

removeStream :: HasCallStack => FFI.LDClient -> StreamId -> IO ()
#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
removeStream client streamid = modifyMVar_ gloLogPathCache $ \cache -> do
  path <- getStreamDirPath streamid
  finally (LD.syncLogsConfigVersion client =<< LD.removeLogDirectory client path True)
          (Cache.delete cache streamid)
  pure cache
#else
removeStream client streamid = do
  path <- getStreamDirPath streamid
  LD.syncLogsConfigVersion client =<< LD.removeLogDirectory client path True
#endif

-- | Find all active streams.
findStreams
  :: HasCallStack
  => FFI.LDClient -> StreamType -> IO [StreamId]
findStreams client streamType = do
  dir_path <- getStreamDirPath $ mkStreamId streamType ""
  d <- try $ LD.getLogDirectory client dir_path
  case d of
    Left (_ :: E.NOTFOUND) -> return []
    Right dir -> do
      ss <- filterM (fmap not . isArchiveStreamName) =<< LD.logDirChildrenNames dir
      forM ss (pure . mkStreamId streamType)

doesStreamExist :: HasCallStack => FFI.LDClient -> StreamId -> IO Bool
doesStreamExist client streamid = do
#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
  cacheExist <- doesCacheExist streamid
  if cacheExist
     then return True
     else do
#endif
        path <- getStreamDirPath streamid
        r <- try $ LD.getLogDirectory client path
        case r of
          Left (_ :: E.NOTFOUND) -> return False
          Right _                -> return True

listStreamPartitions :: HasCallStack => FFI.LDClient -> StreamId -> IO (Map.Map CBytes FFI.C_LogID)
listStreamPartitions client streamid = do
  dir_path <- getStreamDirPath streamid
  keys <- LD.logDirLogsNames =<< LD.getLogDirectory client dir_path
  foldrM insertMap Map.empty keys
  where
    insertMap key keyMap = do
      logId <- getUnderlyingLogId client streamid (Just key)
      return $ Map.insert key logId keyMap

doesStreamPartitionExist
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> IO Bool
doesStreamPartitionExist client streamid m_key = do
#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
  (logpath, key) <- getStreamLogPath streamid m_key
  m_v <- getGloLogPathCache streamid key
  case m_v of
    Just _  -> return True
    Nothing -> do
      r <- try $ LD.getLogGroup client logpath
      case r of
        Left (_ :: E.NOTFOUND) -> return False
        Right lg               -> do
          updateGloLogPathCache streamid key . fst =<< LD.logGroupGetRange lg
          return True
#else
  (logpath, _key) <- getStreamLogPath streamid m_key
  r <- try $ LD.getLogGroup client logpath
  case r of
    Left (_ :: E.NOTFOUND) -> return False
    Right _loggroup        -> return True
#endif

-------------------------------------------------------------------------------
-- StreamAttrs

getStreamLogAttrs :: FFI.LDClient -> StreamId -> IO LD.LogAttributes
getStreamLogAttrs client streamid = do
  dir_path <- getStreamDirPath streamid
  dir <- LD.getLogDirectory client dir_path
  LD.logDirectoryGetAttrs dir

getStreamExtraAttrs :: FFI.LDClient -> StreamId -> IO (Map CBytes CBytes)
getStreamExtraAttrs client streamid = do
  dir_path <- getStreamDirPath streamid
  dir <- LD.getLogDirectory client dir_path
  LD.logAttrsExtras <$> LD.logDirectoryGetAttrs dir

-- | Update a bunch of extra attrs in the stream, return the old attrs.
--
-- If the key does exist, the function will insert the new one.
updateStreamExtraAttrs
  :: FFI.LDClient
  -> StreamId
  -> Map CBytes CBytes
  -> IO (Map CBytes CBytes)
updateStreamExtraAttrs client streamid new_attrs = do
  dir_path <- getStreamDirPath streamid
  attrs <- LD.logDirectoryGetAttrsPtr =<< LD.getLogDirectory client dir_path
  attrs' <- LD.updateLogAttrsExtrasPtr attrs new_attrs
  withForeignPtr attrs' $
    LD.syncLogsConfigVersion client <=< LD.ldWriteAttributes client dir_path
  LD.getAttrsExtrasFromPtr attrs

-- | Approximate milliseconds timestamp of the next record after trim point.
--
-- Set to Nothing if there is no records bigger than trim point.
getStreamPartitionHeadTimestamp
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> IO (Maybe Int64)
getStreamPartitionHeadTimestamp client stream m_key = do
  headAttrs <- LD.getLogHeadAttrs client =<< getUnderlyingLogId client stream m_key
  ts <- LD.getLogHeadAttrsTrimPointTimestamp headAttrs
  case ts of
    FFI.C_MAX_MILLISECONDS -> return Nothing
    _                      -> return $ Just ts

getStreamPartitionExtraAttrs
  :: FFI.LDClient
  -> FFI.C_LogID
  -> IO (Map CBytes CBytes)
getStreamPartitionExtraAttrs client logId = do
   fmap LD.logAttrsExtras <$> LD.logGroupGetAttrs =<< LD.getLogGroupByID client logId

-------------------------------------------------------------------------------

getUnderlyingLogId
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> IO FFI.C_LogID
getUnderlyingLogId client streamid m_key = do
#ifdef HSTREAM_USE_LOCAL_STREAM_CACHE
  (log_path, key) <- getStreamLogPath streamid m_key
  m_logid <- getGloLogPathCache streamid key
  case m_logid of
    Nothing -> do
      logid <- fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client log_path)
      updateGloLogPathCache streamid key logid
      pure logid
    Just ld -> pure ld
#else
  (log_path, _key) <- getStreamLogPath streamid m_key
  fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client log_path)
#endif
{-# INLINABLE getUnderlyingLogId #-}

getStreamIdFromLogId
  :: HasCallStack
  => FFI.LDClient
  -> FFI.C_LogID
  -> IO (StreamId, Maybe CBytes)
getStreamIdFromLogId client logid = do
  -- e.g. /hstream/stream/some_stream/some_key
  logpath <- fmap thawToPosixPath . LD.logGroupGetFullName =<< LD.getLogGroupByID client logid
  let (logdir, key) = second thawFromPosixPath $ P.splitFileName logpath
  let (dir, name) = bimap (fromPosixPath . P.normalise) thawFromPosixPath $ P.splitFileName logdir
  s <- readIORef gloStreamSettings
  streamid <- if | dir == streamNameLogDir s -> pure $ mkStreamId StreamTypeStream name
                 | dir == streamViewLogDir s -> pure $ mkStreamId StreamTypeView name
                 | dir == streamTempLogDir s -> pure $ mkStreamId StreamTypeTemp name
                 | otherwise -> E.throwStoreError (ZB.buildText $ "Unknown logid " <> ZB.int logid) callStack
  let key_ = if key == streamDefaultKey s then Nothing else Just key
  pure (streamid, key_)

-------------------------------------------------------------------------------
--  Checkpoint Store

-- | Try to set logid for checkpoint store.
--
-- idx: 63...56...0
--      |    |    |
-- bit: 00...1...00
initCheckpointStoreLogID :: FFI.LDClient -> LD.LogAttributes -> IO FFI.C_LogID
initCheckpointStoreLogID client attrs = do
  r <- try $ LD.getLogGroupByID client checkpointStoreLogID
  case r of
    Left (_ :: E.NOTFOUND) -> do
      _ <- LD.makeLogGroup client "/hstream/internal/checkpoint" checkpointStoreLogID checkpointStoreLogID attrs True
      return checkpointStoreLogID
    Right _ -> return checkpointStoreLogID

checkpointStoreLogID :: FFI.C_LogID
checkpointStoreLogID = bit 56

-------------------------------------------------------------------------------
-- Reader

newLDFileCkpReader
  :: FFI.LDClient
  -> CBytes
  -- ^ CheckpointedReader name
  -> CBytes
  -- ^ root path
  -> CSize
  -- ^ maximum number of logs that can be read from
  -- this Reader at the same time
  -> Maybe Int64
  -- ^ specify the read buffer size for this client, fallback
  -- to the value in settings if it is Nothing.
  -> IO FFI.LDSyncCkpReader
newLDFileCkpReader client name root_path max_logs m_buffer_size = do
  store <- LD.newFileBasedCheckpointStore root_path
  reader <- LD.newLDReader client max_logs m_buffer_size
  LD.newLDSyncCkpReader name reader store

newLDRsmCkpReader
  :: FFI.LDClient
  -> CBytes
  -- ^ CheckpointedReader name
  -> FFI.C_LogID
  -- ^ checkpointStore logid, this should be 'checkpointStoreLogID'.
  -> Int64
  -- ^ Timeout for the RSM to stop after calling shutdown, in milliseconds.
  -> CSize
  -- ^ maximum number of logs that can be read from
  -- this Reader at the same time
  -> Maybe Int64
  -- ^ specify the read buffer size for this client, fallback
  -- to the value in settings if it is Nothing.
  -> IO FFI.LDSyncCkpReader
newLDRsmCkpReader client name logid timeout max_logs m_buffer_size = do
  store <- LD.newRSMBasedCheckpointStore client logid timeout
  reader <- LD.newLDReader client max_logs m_buffer_size
  LD.newLDSyncCkpReader name reader store

-- TODO: remove
newLDZkCkpReader
  :: FFI.LDClient
  -> CBytes
  -- ^ CheckpointedReader name
  -> CSize
  -- ^ maximum number of logs that can be read from
  -- this Reader at the same time
  -> Maybe Int64
  -- ^ specify the read buffer size for this client, fallback
  -- to the value in settings if it is Nothing.
  -> IO FFI.LDSyncCkpReader
newLDZkCkpReader client name max_logs m_buffer_size = do
  store <- LD.newZookeeperBasedCheckpointStore client
  reader <- LD.newLDReader client max_logs m_buffer_size
  LD.newLDSyncCkpReader name reader store

-------------------------------------------------------------------------------
-- internal helpers

createRandomLogGroup
  :: HasCallStack
  => FFI.LDClient
  -> CBytes
  -> LD.LogAttributes
  -> IO FFI.C_LogID
createRandomLogGroup client logPath attrs = go 10
  where
    go :: Int -> IO FFI.C_LogID
    go maxTries =
      if maxTries <= 0
         then E.throwStoreError "Ran out all retries, but still failed :(" callStack
         else do
           logid <- genUnique
           result <- try $ LD.makeLogGroup client logPath logid logid attrs True
           case result of
             Right group -> do
               LD.syncLogsConfigVersion client =<< LD.logGroupGetVersion group
               return logid
             Left (_ :: E.ID_CLASH) -> do
               Log.warning "LogDevice ID_CLASH!"
               go $! maxTries - 1
{-# INLINABLE createRandomLogGroup #-}

getStreamDirPath :: StreamId -> IO CBytes
getStreamDirPath StreamId{..} = do
  s <- readIORef gloStreamSettings
  case streamType of
    StreamTypeStream -> pure $ t2 P.combine (streamNameLogDir s) streamName
    StreamTypeView   -> pure $ t2 P.combine (streamViewLogDir s) streamName
    StreamTypeTemp   -> pure $ t2 P.combine (streamTempLogDir s) streamName
{-# INLINABLE getStreamDirPath #-}

getStreamLogPath :: StreamId -> Maybe CBytes -> IO (CBytes, CBytes)
getStreamLogPath streamid m_key = do
  s <- readIORef gloStreamSettings
  dir_path <- getStreamDirPath streamid
  let key_name = case m_key of
       Just ""  -> streamDefaultKey s
       Just key -> key
       Nothing  -> streamDefaultKey s
  let full_path = t2 P.combine dir_path key_name
  pure (full_path, key_name)
{-# INLINABLE getStreamLogPath #-}

-------------------------------------------------------------------------------

#if !MIN_VERSION_filepath(1, 4, 100)
t2 :: (String -> String -> String) -> CBytes -> CBytes -> CBytes
t2 f a1 a2 = fromPosixPath $ f (toPosixPath a1) (toPosixPath a2)

fromPosixPath :: String -> StreamName
fromPosixPath = CBytes.pack
{-# INLINABLE fromPosixPath #-}

thawFromPosixPath :: String -> StreamName
thawFromPosixPath = fromPosixPath
{-# INLINABLE thawFromPosixPath #-}

toPosixPath :: StreamName -> String
toPosixPath = CBytes.unpack
{-# INLINABLE toPosixPath #-}

thawToPosixPath :: StreamName -> String
thawToPosixPath = toPosixPath
{-# INLINABLE thawToPosixPath #-}

#else

-- TODO: All use ShortByteString instead of CBytes

t2 :: (P.PosixString -> P.PosixString -> P.PosixString) -> CBytes -> CBytes -> CBytes
t2 f a1 a2 = fromPosixPath $ f (toPosixPath a1) (toPosixPath a2)

fromPosixPath :: P.PosixString -> StreamName
fromPosixPath (P.PosixString sbs) = sbs2cbytes sbs
{-# INLINABLE fromPosixPath #-}

thawFromPosixPath :: P.PosixString -> StreamName
thawFromPosixPath (P.PosixString sbs) = sbs2cbytesUnsafe sbs
{-# INLINABLE thawFromPosixPath #-}

toPosixPath :: StreamName -> P.PosixPath
toPosixPath name = P.PosixString (cbytes2sbs name)
{-# INLINABLE toPosixPath #-}

thawToPosixPath :: StreamName -> P.PosixPath
thawToPosixPath name = P.PosixString (cbytes2sbsUnsafe name)
{-# INLINABLE thawToPosixPath #-}
#endif

-------------------------------------------------------------------------------

#undef HSTREAM_USE_LOCAL_STREAM_CACHE
