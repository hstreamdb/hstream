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
    -- ** Operations
  , createStream
  , createStreamPartition
  , createStreamPartitionWithExtrAttr
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
import           Control.Monad                    (filterM, forM, void, (<=<))
import           Data.Bits                        (bit)
import           Data.Default                     (def)
import           Data.Foldable                    (foldrM)
import           Data.Hashable                    (Hashable)
import           Data.Int                         (Int64)
import           Data.IORef                       (IORef, atomicModifyIORef',
                                                   newIORef, readIORef)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromMaybe)
import           Foreign.C                        (CSize)
import           Foreign.ForeignPtr               (withForeignPtr)
import           GHC.Generics                     (Generic)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
import qualified Z.Data.Builder                   as ZB
import qualified Z.Data.CBytes                    as CBytes
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.Text                      as ZT
import qualified Z.Data.Vector                    as ZV
import qualified Z.IO.FileSystem                  as FS

import qualified HStream.Logger                   as Log
import qualified HStream.Store.Exception          as E
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Internal.Types     as FFI
import           HStream.Utils                    (genUnique)

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

newtype ArchivedStream = ArchivedStream { getArchivedStreamName :: CBytes}

isArchiveStreamName :: StreamName -> IO Bool
isArchiveStreamName name = do
  prefix <- CBytes.toBytes . streamArchivePrefix <$> readIORef gloStreamSettings
  let name' = CBytes.toBytes name
  pure $ prefix `ZV.isPrefixOf` name'

-- ArchivedStreamName format: "prefix + name + suffix"
toArchivedStreamName :: HasCallStack => FFI.LDClient -> StreamId -> IO CBytes
toArchivedStreamName client streamid@StreamId{..} = do
  -- we use default loggroup's logid as the suffix
  logid <- getUnderlyingLogId client streamid Nothing
  let suffix = CBytes.buildCBytes $ ZB.int logid
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
  name <- case streamType of
            StreamTypeStream -> FS.relative (streamNameLogDir s) path
            StreamTypeView   -> FS.relative (streamViewLogDir s) path
            StreamTypeTemp   -> FS.relative (streamTempLogDir s) path
  return $ StreamId streamType name

showStreamName :: StreamId -> String
showStreamName = CBytes.unpack . streamName

-------------------------------------------------------------------------------

-- | Create stream
--
-- Currently a Stream is a logidr.
createStream :: HasCallStack
             => FFI.LDClient -> StreamId -> LD.LogAttributes -> IO ()
createStream client streamid attrs = do
  path <- getStreamDirPath streamid
  void $ LD.makeLogDirectory client path attrs True
  -- create default loggroup
  (log_path, _key) <- getStreamLogPath streamid Nothing
  void $ createRandomLogGroup client log_path def

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
  -> IO FFI.C_LogID
createStreamPartition client streamid m_key = do
  createStreamPartitionWithExtrAttr client streamid m_key Map.empty

createStreamPartitionWithExtrAttr
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> Map CBytes CBytes
  -> IO FFI.C_LogID
createStreamPartitionWithExtrAttr client streamid m_key attr = do
  stream_exist <- doesStreamExist client streamid
  if stream_exist
     then do (log_path, _key) <- getStreamLogPath streamid m_key
             createRandomLogGroup client log_path def {LD.logAttrsExtras = attr}
     else E.throwStoreError ("No such stream: " <> ZT.pack (showStreamName streamid))
                            callStack

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
  LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from' to'
{-# INLINABLE _renameStream_ #-}

-- | Archive a stream, then you won't find it by 'findStreams'.
-- Return the archived stream, so that you can unarchive or remove it.
--
-- Note that all operations depend on logid will work as expected.
archiveStream :: HasCallStack => FFI.LDClient -> StreamId -> IO ArchivedStream
archiveStream client streamid = do
  archiveStreamName <- toArchivedStreamName client streamid
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
removeStream client streamid = do
  path <- getStreamDirPath streamid
  LD.syncLogsConfigVersion client =<< LD.removeLogDirectory client path True

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
  (logpath, _key) <- getStreamLogPath streamid m_key
  r <- try $ LD.getLogGroup client logpath
  case r of
    Left (_ :: E.NOTFOUND) -> return False
    Right _                -> return True

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

-------------------------------------------------------------------------------

getUnderlyingLogId
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> IO FFI.C_LogID
getUnderlyingLogId client streamid m_key = do
  (log_path, _key) <- getStreamLogPath streamid m_key
  fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client log_path)
{-# INLINABLE getUnderlyingLogId #-}

getStreamIdFromLogId
  :: HasCallStack
  => FFI.LDClient
  -> FFI.C_LogID
  -> IO (StreamId, Maybe CBytes)
getStreamIdFromLogId client logid = do
  -- e.g. /hstream/stream/some_stream/some_key
  logpath <- LD.logGroupGetFullName =<< LD.getLogGroupByID client logid
  (logdir, key) <- FS.splitBaseName logpath
  (dir, name) <- FS.splitBaseName logdir
  s <- readIORef gloStreamSettings
  let errmsg = "Unknown stream path: " <> CBytes.toText logpath
  dir' <- FS.normalize dir
  streamid <- if | dir' == streamNameLogDir s -> pure $ mkStreamId StreamTypeStream name
                 | dir' == streamViewLogDir s -> pure $ mkStreamId StreamTypeView name
                 | dir' == streamTempLogDir s -> pure $ mkStreamId StreamTypeTemp name
                 | otherwise -> E.throwStoreError errmsg callStack
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
    StreamTypeStream -> streamNameLogDir s `FS.join` streamName
    StreamTypeView   -> streamViewLogDir s `FS.join` streamName
    StreamTypeTemp   -> streamTempLogDir s `FS.join` streamName
{-# INLINABLE getStreamDirPath #-}

getStreamLogPath :: StreamId -> Maybe CBytes -> IO (CBytes, CBytes)
getStreamLogPath streamid m_key = do
  s <- readIORef gloStreamSettings
  dir_path <- getStreamDirPath streamid
  let key_name = fromMaybe (streamDefaultKey s) m_key
  full_path <- dir_path `FS.join` key_name
  pure (full_path, key_name)
{-# INLINABLE getStreamLogPath #-}
