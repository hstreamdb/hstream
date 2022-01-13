{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Stream
  ( -- * Stream
    StreamSettings (..)
  , updateGloStreamSettings
  , StreamType (..)
  , StreamId (streamType, streamName)
  , showStreamName
  , mkStreamId
  , mkStreamIdFromLogPath
    -- ** helpers
  , getUnderlyingLogPath
  , getUnderlyingLogId
  , getOrCreateUnderlyingLogId
    -- ** Log
  , FFI.LogAttrs (LogAttrs)
  , FFI.HsLogAttrs (..)
    -- ** Operations
  , createStream
  , removeStream
  , findStreams
  , getStreamReplicaFactor
  , getStreamHeadTimestamp
  , doesStreamExist

    -- * Internal Log
  , FFI.LogID (..)
  , FFI.C_LogID
  , FFI.LDLogGroup
  , FFI.LDDirectory
  , LD.getLogGroup
  , LD.getLogGroupByID
  , LD.logGroupGetName
  , LD.logGroupGetFullName
  , LD.logGroupGetExtraAttr
  , LD.logGroupUpdateExtraAttrs

    -- * Writer
  , LD.append
  , LD.appendBS
  , LD.appendBatch
  , LD.appendBatchBS
  , LD.AppendCompletion (..)
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable
  , pattern FFI.KeyTypeUndefined
  , FFI.Compression (..)

    -- * Checkpoint Store
  , FFI.LDCheckpointStore
  , LD.newFileBasedCheckpointStore
  , LD.newRSMBasedCheckpointStore
  , LD.newZookeeperBasedCheckpointStore
  , LD.ckpStoreGetLSN
  , LD.ckpStoreUpdateLSN
  , LD.ckpStoreRemoveCheckpoints
  , LD.ckpStoreRemoveAllCheckpoints

    -- * Reader
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
  , LD.writeCheckpoints
  , LD.writeLastCheckpoints
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

    -- * Checkpoint Store
  , initCheckpointStoreLogID
  , checkpointStoreLogID
  ) where

import           Control.Exception                (finally, try)
import           Control.Monad                    (forM_, unless)
import           Data.Bits                        (bit)
import qualified Data.Cache                       as Cache
import           Data.Functor                     ((<&>))
import           Data.IORef                       (IORef, atomicModifyIORef',
                                                   newIORef, readIORef)
import           Data.Int                         (Int64)
import           Data.Word                        (Word32)
import           Foreign.C                        (CSize)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CBytes
import qualified Z.IO.FileSystem                  as FS

import qualified HStream.Logger                   as Log
import qualified HStream.Store.Exception          as E
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Internal.Types     as FFI
import           HStream.Utils                    (genUnique)

-------------------------------------------------------------------------------

data StreamSettings = StreamSettings
  { streamNameLogDir :: CBytes
  , streamViewLogDir :: CBytes
  , streamTempLogDir :: CBytes
  }

gloStreamSettings :: IORef StreamSettings
gloStreamSettings = unsafePerformIO . newIORef $
  StreamSettings { streamNameLogDir = "/hstream/stream"
                 , streamViewLogDir = "/hstream/view"
                 , streamTempLogDir = "/tmp/hstream"
                 }
{-# NOINLINE gloStreamSettings #-}

updateGloStreamSettings :: (StreamSettings -> StreamSettings)-> IO ()
updateGloStreamSettings f = atomicModifyIORef' gloStreamSettings $ \s -> (f s, ())

data StreamType = StreamTypeStream | StreamTypeView | StreamTypeTemp
  deriving (Show, Eq)

data StreamId = StreamId
  { streamType :: StreamType
  , streamName :: CBytes
  -- ^ A stream name is an identifier of the stream.
  -- The first character of the StreamName should not be '/'.
  } deriving (Show, Eq)

type Key = CBytes

defaultKey :: CBytes
defaultKey = "default-key-when-key-not-given"
--------------------------------------------------------------------------------

-- TODO: validation
mkStreamId :: StreamType -> CBytes -> StreamId
mkStreamId = StreamId

-- TODO: validation
mkStreamIdFromLogPath :: StreamType -> CBytes -> IO StreamId
mkStreamIdFromLogPath streamType path = do
  s <- readIORef gloStreamSettings
  name <- case streamType of
            StreamTypeStream ->
              FS.relative (streamNameLogDir s) path
              >>= flip FS.changeBaseName ""
              >>= FS.normalize
            StreamTypeView   -> FS.relative (streamViewLogDir s) path
            StreamTypeTemp   -> FS.relative (streamTempLogDir s) path
  return $ StreamId streamType name

showStreamName :: StreamId -> String
showStreamName = CBytes.unpack . streamName

getUnderlyingLogPath :: StreamId -> Maybe Key -> IO CBytes
getUnderlyingLogPath (StreamId StreamTypeStream stream) (Just key) = do
  s <- readIORef gloStreamSettings
  stream `FS.join` key
    >>= FS.join (streamNameLogDir s)
getUnderlyingLogPath StreamId{..} _ = do
  s <- readIORef gloStreamSettings
  case streamType of
    StreamTypeStream -> streamNameLogDir s `FS.join` streamName >>= flip FS.join defaultKey
    StreamTypeView   -> streamViewLogDir s `FS.join` streamName
    StreamTypeTemp   -> streamTempLogDir s `FS.join` streamName
{-# INLINABLE getUnderlyingLogPath #-}

getStreamReplicaFactor :: FFI.LDClient -> StreamId -> IO Int
getStreamReplicaFactor client stream = do
  FFI.logReplicationFactor <$> getStreamAttrs client stream

getStreamAttrs :: FFI.LDClient -> StreamId -> IO FFI.HsLogAttrs
getStreamAttrs client stream = do
  logid <- getUnderlyingLogId client stream Nothing
  loggroup <- LD.getLogGroupByID client logid
  LD.logGroupGetHsLogAttrs loggroup

doesPathExist :: HasCallStack => FFI.LDClient -> CBytes -> IO Bool
doesPathExist client path = do
  m_v <- Cache.lookup logPathCache path
  case m_v of
    Just _  -> return True
    Nothing -> do
      r <- try $ LD.getLogGroup client path
      case r of
        Left (_ :: E.NOTFOUND) -> return False
        Right group -> do
          logid <- fst <$> LD.logGroupGetRange group
          Cache.insert logPathCache path logid
          return True

doesPartitionExist :: HasCallStack
  => FFI.LDClient -> StreamId -> Key -> IO Bool
doesPartitionExist client stream key =
  getUnderlyingLogPath stream (Just key) >>= doesPathExist client

-- | Global loggroup path to logid cache
logPathCache :: Cache.Cache CBytes FFI.C_LogID
logPathCache = unsafePerformIO $ Cache.newCache Nothing
{-# NOINLINE logPathCache #-}

createRandomLogGroup :: HasCallStack => FFI.LDClient -> CBytes -> FFI.LogAttrs -> IO ()
createRandomLogGroup client logPath attrs = Log.withDefaultLogger $ go 10
  where
    go :: Int -> IO ()
    go maxTries =
      if maxTries <= 0
         then E.throwStoreError "Ran out all retries, but still failed :(" callStack
         else do
           logid <- genUnique
           result <- try $ LD.makeLogGroup client logPath logid logid attrs True
           case result of
             Right group -> do
               LD.syncLogsConfigVersion client =<< LD.logGroupGetVersion group
               Cache.insert logPathCache logPath logid
             Left (_ :: E.ID_CLASH) -> do
               Log.warning "LogDevice ID_CLASH!"
               go $! maxTries - 1
{-# INLINABLE createRandomLogGroup #-}

--------------------------------------------------------------------------------

-- | Create stream
--
-- Currently a Stream is a loggroup which only contains one random logid.
createStream :: HasCallStack => FFI.LDClient -> StreamId -> FFI.LogAttrs -> IO ()
createStream client stream attrs = do
  path <- getUnderlyingLogPath stream Nothing
  createRandomLogGroup client path attrs

renameStream
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -- ^ The source stream to rename
  -> StreamId
  -- ^ The new stream you are renaming to
  -> IO ()
renameStream client from to = do
  from' <- getUnderlyingLogPath from Nothing
  to'   <- getUnderlyingLogPath to Nothing
  finally (LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from' to')
          (Cache.delete logPathCache from')
  m_v <- Cache.lookup' logPathCache from'
  forM_ m_v $ Cache.insert logPathCache to'

removeStream :: HasCallStack => FFI.LDClient -> StreamId -> IO ()
removeStream client stream = do
  path <- getUnderlyingLogPath stream Nothing
  finally (LD.syncLogsConfigVersion client =<< LD.removeLogGroup client path)
          (Cache.delete logPathCache path)

findStreams
  :: HasCallStack
  => FFI.LDClient -> StreamType -> Bool -> IO [StreamId]
findStreams client streamType _ = do
  prefix <- streamNameLogDir <$> readIORef gloStreamSettings
  d <- try $ LD.getLogDirectory client prefix
  case d of
    Left (_ :: E.NOTFOUND) -> return []
    Right dir -> LD.logDirChildrenNames dir <&> map  (mkStreamId streamType)

-- | Approximate milliseconds timestamp of the next record after trim point.
--
-- Set to Nothing if there is no records bigger than trim point.
getStreamHeadTimestamp :: FFI.LDClient -> StreamId -> IO (Maybe Int64)
getStreamHeadTimestamp client stream = do
  headAttrs <- LD.getLogHeadAttrs client =<< getUnderlyingLogId client stream Nothing
  ts <- LD.getLogHeadAttrsTrimPointTimestamp headAttrs
  case ts of
    FFI.C_MAX_MILLISECONDS -> return Nothing
    _                      -> return $ Just ts

doesStreamExist :: HasCallStack => FFI.LDClient -> StreamId -> IO Bool
doesStreamExist client stream = getUnderlyingLogPath stream Nothing >>= doesPathExist client

-- getStreamIdFromLogId :: HasCallStack => FFI.LDClient -> StreamId -> IO Bool
getUnderlyingLogId :: HasCallStack
  => FFI.LDClient -> StreamId -> Maybe Key
  -> IO FFI.C_LogID
getUnderlyingLogId client stream key = do
  getUnderlyingLogPath stream key >>= getCLogIDByLogGroup client
{-# INLINABLE getUnderlyingLogId #-}

getOrCreateUnderlyingLogId :: HasCallStack
  => FFI.LDClient -> StreamId -> Key
  -> IO FFI.C_LogID
getOrCreateUnderlyingLogId client stream key = do
  exists <- doesPartitionExist client stream key
  unless exists $ createPartition client stream key
  getUnderlyingLogId client stream (Just key)

createPartition :: HasCallStack
  => FFI.LDClient -> StreamId -> Key -> IO ()
createPartition client stream key = do
  attrs <- getStreamAttrs client stream
  path <- getUnderlyingLogPath stream (Just key)
  createRandomLogGroup client path (FFI.LogAttrs attrs)

-- | Try to set logid for checkpoint store.
--
-- idx: 63...56...0
--      |    |    |
-- bit: 00...1...00
initCheckpointStoreLogID :: FFI.LDClient -> FFI.LogAttrs -> IO FFI.C_LogID
initCheckpointStoreLogID client attrs = do
  r <- try $ LD.getLogGroupByID client checkpointStoreLogID
  case r of
    Left (_ :: E.NOTFOUND) -> do
      _ <- LD.makeLogGroup client "/internal/checkpoint" checkpointStoreLogID checkpointStoreLogID attrs True
      return checkpointStoreLogID
    Right _ -> return checkpointStoreLogID

checkpointStoreLogID :: FFI.C_LogID
checkpointStoreLogID = bit 56

-------------------------------------------------------------------------------

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
  -> Word32
  -- ^ The number of retries when synchronously writing checkpoints.
  -> IO FFI.LDSyncCkpReader
newLDFileCkpReader client name root_path max_logs m_buffer_size retries = do
  store <- LD.newFileBasedCheckpointStore root_path
  reader <- LD.newLDReader client max_logs m_buffer_size
  LD.newLDSyncCkpReader name reader store retries

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
  -> Word32
  -- ^ The number of retries when synchronously writing checkpoints.
  -> IO FFI.LDSyncCkpReader
newLDRsmCkpReader client name logid timeout max_logs m_buffer_size retries = do
  store <- LD.newRSMBasedCheckpointStore client logid timeout
  reader <- LD.newLDReader client max_logs m_buffer_size
  LD.newLDSyncCkpReader name reader store retries

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
  -> Word32
  -- ^ The number of retries when synchronously writing checkpoints.
  -> IO FFI.LDSyncCkpReader
newLDZkCkpReader client name max_logs m_buffer_size retries = do
  store <- LD.newZookeeperBasedCheckpointStore client
  reader <- LD.newLDReader client max_logs m_buffer_size
  LD.newLDSyncCkpReader name reader store retries

-------------------------------------------------------------------------------

getCLogIDByLogGroup :: HasCallStack => FFI.LDClient -> CBytes -> IO FFI.C_LogID
getCLogIDByLogGroup client path = do
  m_v <- Cache.lookup logPathCache path
  case m_v of
    Just v -> return v
    Nothing -> do
      logid <- fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client path)
      Cache.insert logPathCache path logid
      return logid
