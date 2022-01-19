{-# LANGUAGE MultiWayIf      #-}
{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Stream
  ( -- * Stream
    StreamSettings (..)
  , updateGloStreamSettings
  , StreamType (..)
  , StreamId (streamType, streamName)
  , showStreamName
  , mkStreamId
  , mkStreamIdFromFullLogDir
  , getStreamReplicaFactor
  , getStreamPartitionHeadTimestamp
    -- ** Operations
  , createStream
  , createStreamPartition
  , renameStream
  , renameStream'
  , removeStream
  , findStreams
  , doesStreamExist
    -- ** helpers
  , getUnderlyingLogId
  , getStreamIdFromLogId

    -- * Internal Log
  , FFI.LogID (..)
  , FFI.C_LogID
  , FFI.LDLogGroup
  , FFI.LDDirectory
    -- ** Log
  , FFI.LogAttrs (LogAttrs, LogAttrsDef)
  , FFI.HsLogAttrs (..)

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
  , initCheckpointStoreLogID
  , checkpointStoreLogID
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

    -- * Internal helpers
  , getStreamDirPath
  , getStreamLogPath
  ) where

import           Control.Concurrent               (MVar, modifyMVar_, newMVar,
                                                   readMVar)
import           Control.Exception                (finally, try)
import           Control.Monad                    (forM, forM_, void)
import           Data.Bits                        (bit)
import qualified Data.Cache                       as Cache
import           Data.Hashable                    (Hashable)
import           Data.IORef                       (IORef, atomicModifyIORef',
                                                   newIORef, readIORef)
import           Data.Int                         (Int64)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromMaybe)
import           Data.Word                        (Word32)
import           Foreign.C                        (CSize)
import           GHC.Generics                     (Generic)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CBytes
import qualified Z.Data.Text                      as ZT
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
  , streamDefaultKey :: CBytes
  }

gloStreamSettings :: IORef StreamSettings
gloStreamSettings = unsafePerformIO . newIORef $
  -- NOTE: no trailing slash
  StreamSettings { streamNameLogDir = "/hstream/stream"
                 , streamViewLogDir = "/hstream/view"
                 , streamTempLogDir = "/tmp/hstream"
                 , streamDefaultKey = "__default_key__"
                 }
{-# NOINLINE gloStreamSettings #-}

updateGloStreamSettings :: (StreamSettings -> StreamSettings)-> IO ()
updateGloStreamSettings f = atomicModifyIORef' gloStreamSettings $ \s -> (f s, ())

type StreamCache = Cache.Cache StreamId (Map CBytes FFI.C_LogID)

-- | Global logdir path to logid cache
gloLogPathCache :: MVar StreamCache
gloLogPathCache = unsafePerformIO $ newMVar =<< Cache.newCache Nothing
{-# NOINLINE gloLogPathCache #-}

getGloLogPathCache :: StreamId -> CBytes -> IO (Maybe FFI.C_LogID)
getGloLogPathCache streamid logpath = do
  cache <- readMVar gloLogPathCache
  m_v <- Cache.lookup' cache streamid
  case m_v of
    Nothing -> pure Nothing
    Just mp -> pure $ Map.lookup logpath mp

updateGloLogPathCache :: StreamId -> CBytes -> FFI.C_LogID -> IO ()
updateGloLogPathCache streamid logpath logid =
  modifyMVar_ gloLogPathCache $ \c -> do
    m_v <- Cache.lookup' c streamid
    case m_v of
      Nothing -> Cache.insert c streamid (Map.singleton logpath logid) >> pure c
      Just mp -> Cache.insert c streamid (Map.insert logpath logid mp) >> pure c

-------------------------------------------------------------------------------

data StreamType = StreamTypeStream | StreamTypeView | StreamTypeTemp
  deriving (Show, Eq, Generic)
instance Hashable StreamType

data StreamId = StreamId
  { streamType :: StreamType
  , streamName :: CBytes
  -- ^ A stream name is an identifier of the stream.
  -- The first character of the StreamName should not be '/'.
  } deriving (Show, Eq, Generic)
instance Hashable StreamId

-- TODO: validation
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

getStreamReplicaFactor :: FFI.LDClient -> StreamId -> IO Int
getStreamReplicaFactor client streamid = do
  dir_path <- getStreamDirPath streamid
  dir <- LD.getLogDirectory client dir_path
  LD.getAttrsReplicationFactorFromPtr =<< LD.logDirectorypGetAttrs dir

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

-- | Create stream
--
-- Currently a Stream is a loggroup which only contains one random logid.
createStream :: HasCallStack => FFI.LDClient -> StreamId -> FFI.LogAttrs -> IO ()
createStream client streamid attrs = do
  path <- getStreamDirPath streamid
  void $ LD.makeLogDirectory client path attrs True
  -- create default loggroup
  log_path <- getStreamLogPath streamid Nothing
  logid <- createRandomLogGroup client log_path FFI.LogAttrsDef
  updateGloLogPathCache streamid log_path logid

createStreamPartition
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> IO ()
createStreamPartition client streamid m_key = do
  stream_exist <- doesStreamExist client streamid
  if stream_exist
     then do log_path <- getStreamLogPath streamid m_key
             logid <- createRandomLogGroup client log_path FFI.LogAttrsDef
             updateGloLogPathCache streamid log_path logid
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
  _renameStrem_ client (mkStreamId streamType srcName) (mkStreamId streamType destName)

renameStream'
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -- ^ The source stream to rename
  -> CBytes
  -- ^ The new stream name you are renaming to
  -> IO ()
renameStream' client from destName =
  _renameStrem_ client from (mkStreamId (streamType from) destName)

_renameStrem_ :: HasCallStack => FFI.LDClient -> StreamId -> StreamId -> IO ()
_renameStrem_ client from to = do
  from' <- getStreamDirPath from
  to'   <- getStreamDirPath to
  modifyMVar_ gloLogPathCache $ \cache -> do
    m_v <- Cache.lookup' cache from
    finally (LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from' to')
            (Cache.delete cache from)
    forM_ m_v $ Cache.insert cache to
    pure cache
{-# INLINABLE _renameStrem_ #-}

removeStream :: HasCallStack => FFI.LDClient -> StreamId -> IO ()
removeStream client streamid = modifyMVar_ gloLogPathCache $ \cache -> do
  path <- getStreamDirPath streamid
  finally (LD.syncLogsConfigVersion client =<< LD.removeLogDirectory client path True)
          (Cache.delete cache streamid)
  pure cache

findStreams
  :: HasCallStack
  => FFI.LDClient -> StreamType -> IO [StreamId]
findStreams client streamType = do
  dir_path <- getStreamDirPath $ mkStreamId streamType ""
  d <- try $ LD.getLogDirectory client dir_path
  case d of
    Left (_ :: E.NOTFOUND) -> return []
    Right dir -> do
      ps <- LD.logDirChildrenNames dir
      forM ps (pure . mkStreamId streamType)

doesStreamExist :: HasCallStack => FFI.LDClient -> StreamId -> IO Bool
doesStreamExist client streamid = do
  cache <- readMVar gloLogPathCache
  m_v <- Cache.lookup' cache streamid
  case m_v of
    Just _  -> return True
    Nothing -> do
      path <- getStreamDirPath streamid
      r <- try $ LD.getLogDirectory client path
      case r of
        Left (_ :: E.NOTFOUND) -> return False
        Right _                -> return True

getUnderlyingLogId
  :: HasCallStack
  => FFI.LDClient
  -> StreamId
  -> Maybe CBytes
  -> IO FFI.C_LogID
getUnderlyingLogId client streamid m_key = do
  log_path <- getStreamLogPath streamid m_key
  m_logid <- getGloLogPathCache streamid log_path
  case m_logid of
    Nothing -> fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client log_path)
    Just ld -> pure ld
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
initCheckpointStoreLogID :: FFI.LDClient -> FFI.LogAttrs -> IO FFI.C_LogID
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
-- internal helpers

createRandomLogGroup
  :: HasCallStack
  => FFI.LDClient
  -> CBytes
  -> FFI.LogAttrs
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

getStreamLogPath :: StreamId -> Maybe CBytes -> IO CBytes
getStreamLogPath streamid m_key = do
  s <- readIORef gloStreamSettings
  dir_path <- getStreamDirPath streamid
  let key_name = fromMaybe (streamDefaultKey s) m_key
  dir_path `FS.join` key_name
{-# INLINABLE getStreamLogPath #-}
