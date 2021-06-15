{-# LANGUAGE DeriveAnyClass #-}

module HStream.Store.Stream
  ( -- * Stream
    StreamSettings (..)
  , updateGloStreamSettings
    -- ** StreamName
  , StreamName
  , mkStreamName
  , getStreamName
  , FFI.LogAttrs (LogAttrs)
  , FFI.HsLogAttrs (..)
    -- **
  , createStream
  , renameStream
  , removeStream
  , findStreams
  , getStreamReplicaFactor
  , getStreamHeadTimestamp
  , doesStreamExists

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
    -- ** helpers
  , streamNameToLogPath
  , logPathToStreamName
  , getCLogIDByStreamName

    -- * Writer
  , LD.append
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable
  , FFI.AppendCallBackData (..)

    -- * Checkpoint Store
  , LD.newFileBasedCheckpointStore
  , LD.newRSMBasedCheckpointStore
  , LD.newZookeeperBasedCheckpointStore
  , LD.ckpStoreGetLSN

    -- * Reader
  , FFI.RecordByteOffset (..)
  , FFI.DataRecord (..)

  , LD.newLDReader
  , LD.readerStartReading
  , LD.readerRead
  , LD.readerSetTimeout
  , LD.readerSetWithoutPayload
  , LD.readerSetIncludeByteOffset
  , LD.readerSetWaitOnlyWhenNoData
  , stopReader
    -- ** Checkpointed Reader
  , newLDFileCkpReader
  , newLDRsmCkpReader
  , newLDZkCkpReader
  , LD.writeCheckpoints
  , LD.writeLastCheckpoints
  , LD.ckpReaderStartReading
  , LD.startReadingFromCheckpoint
  , LD.ckpReaderRead
  , LD.ckpReaderSetTimeout
  , LD.ckpReaderSetWithoutPayload
  , LD.ckpReaderSetIncludeByteOffset
  , LD.ckpReaderSetWaitOnlyWhenNoData
  , stopCkpReader
  , initCheckpointStoreLogID
  ) where

import           Control.Exception                (finally, try)
import           Control.Monad                    (forM, forM_, unless)
import           Data.Bits                        (bit, shiftL, shiftR, (.&.),
                                                   (.|.))
import qualified Data.Cache                       as Cache
import           Data.IORef                       (IORef, atomicModifyIORef',
                                                   newIORef, readIORef)
import           Data.Int                         (Int64)
import           Data.String                      (IsString)
import           Data.Time.Clock.System           (SystemTime (..))
import           Data.Word                        (Word16, Word32)
import           Foreign.C                        (CSize)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
import           System.Random                    (randomRIO)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.IO.FileSystem                  as FS
import           Z.IO.Time                        (getSystemTime')

import qualified HStream.Store.Exception          as E
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Internal.Types     as FFI
import qualified HStream.Store.Logger             as Log

-------------------------------------------------------------------------------

-- | A stream name is an identity of the stream.
--
-- The first character of the StreamName should not be '/'.
newtype StreamName = StreamName CBytes
  deriving newtype (Show, Eq, Ord, Semigroup, Monoid, IsString)

-- TODO: validation
mkStreamName :: CBytes -> StreamName
mkStreamName = StreamName

getStreamName :: StreamName -> CBytes
getStreamName (StreamName name) = name

newtype StreamSettings = StreamSettings
  { streamNamePrefix :: CBytes
  }

gloStreamSettings :: IORef StreamSettings
gloStreamSettings = unsafePerformIO $ newIORef (StreamSettings "/hstream")
{-# NOINLINE gloStreamSettings #-}

updateGloStreamSettings :: (StreamSettings -> StreamSettings)-> IO ()
updateGloStreamSettings f = atomicModifyIORef' gloStreamSettings $ \s -> (f s, ())

streamNameToLogPath :: StreamName -> IO CBytes
streamNameToLogPath (StreamName stream) = do
  s <- readIORef gloStreamSettings
  streamNamePrefix s `FS.join` stream

logPathToStreamName :: CBytes -> IO StreamName
logPathToStreamName path = do
  prefix <- streamNamePrefix <$> readIORef gloStreamSettings
  StreamName <$>FS.relative prefix path

-- | Global loggroup path to logid cache
logPathCache :: Cache.Cache CBytes FFI.C_LogID
logPathCache = unsafePerformIO $ Cache.newCache Nothing
{-# NOINLINE logPathCache #-}

-- | Create stream
--
-- Currently a Stream is a loggroup which only contains one random logid.
createStream :: HasCallStack => FFI.LDClient -> StreamName -> FFI.LogAttrs -> IO ()
createStream client stream attrs = Log.withDefaultLogger $ go 10
  where
    go :: Int -> IO ()
    go maxTries =
      if maxTries <= 0
         then E.throwStoreError "Ran out all retries, but still failed :(" callStack
         else do
           logid <- genRandomLogID
           logPath <- streamNameToLogPath stream
           result <- try $ LD.makeLogGroup client logPath logid logid attrs True
           case result of
             Right group -> do
               LD.syncLogsConfigVersion client =<< LD.logGroupGetVersion group
               Cache.insert logPathCache logPath logid
             Left (_ :: E.ID_CLASH) -> do
               Log.warning "LogDevice ID_CLASH!"
               go $! maxTries - 1

renameStream
  :: HasCallStack
  => FFI.LDClient
  -> StreamName
  -- ^ The source path to rename
  -> StreamName
  -- ^ The new path you are renaming to
  -> IO ()
renameStream client from to = do
  from' <- streamNameToLogPath from
  to'   <- streamNameToLogPath to
  finally (LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from' to')
          (Cache.delete logPathCache from')
  m_v <- Cache.lookup' logPathCache from'
  forM_ m_v $ Cache.insert logPathCache to'

removeStream :: HasCallStack => FFI.LDClient -> StreamName -> IO ()
removeStream client stream = do
  path <- streamNameToLogPath stream
  finally (LD.syncLogsConfigVersion client =<< LD.removeLogGroup client path)
          (Cache.delete logPathCache path)

findStreams :: HasCallStack => FFI.LDClient -> Bool -> IO [StreamName]
findStreams client recursive = do
  prefix <- streamNamePrefix <$> readIORef gloStreamSettings
  d <- try $ LD.getLogDirectory client prefix
  case d of
    Left (_ :: E.NOTFOUND) -> return []
    Right dir -> do
      ps <- LD.logDirectoryGetLogsName recursive dir
      forM ps logPathToStreamName

getStreamReplicaFactor :: FFI.LDClient -> StreamName -> IO Int
getStreamReplicaFactor client name = do
  logid <- getCLogIDByStreamName client name
  loggroup <- LD.getLogGroupByID client logid
  LD.getAttrsReplicationFactorFromPtr =<< LD.logGroupGetAttrs loggroup

-- | Approximate milliseconds timestamp of the next record after trim point.
--
-- Set to Nothing if there is no records bigger than trim point.
getStreamHeadTimestamp :: FFI.LDClient -> StreamName -> IO (Maybe Int64)
getStreamHeadTimestamp client name = do
  headAttrs <- LD.getLogHeadAttrs client =<< getCLogIDByStreamName client name
  ts <- LD.getLogHeadAttrsTrimPointTimestamp headAttrs
  case ts of
    FFI.C_MAX_MILLISECONDS -> return Nothing
    _                      -> return $ Just ts

doesStreamExists :: HasCallStack => FFI.LDClient -> StreamName -> IO Bool
doesStreamExists client stream = do
  path <- streamNameToLogPath stream
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

getCLogIDByStreamName :: FFI.LDClient -> StreamName -> IO FFI.C_LogID
getCLogIDByStreamName client stream = do
  path <- streamNameToLogPath stream
  m_v <- Cache.lookup logPathCache path
  case m_v of
    Just v -> return v
    Nothing -> do
      logid <- fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client path)
      Cache.insert logPathCache path logid
      return logid

-- | Generate a random logid through a simplify version of snowflake algorithm.
--
-- idx: 63...56 55...0
--      |    |  |    |
-- bit: 0....0  xx....
genRandomLogID :: IO FFI.C_LogID
genRandomLogID = do
  let startTS = 1577808000  -- 2020-01-01
  ts <- getSystemTime'
  let sec = systemSeconds ts - startTS
  unless (sec > 0) $ error "Impossible happened, make sure your system time is synchronized."
  -- 32bit
  let tsBit :: Int64 = fromIntegral (maxBound :: Word32) .&. sec
  -- 8bit
  let tsBit' :: Word32 = shiftR (systemNanoseconds ts) 24
  -- 16bit
  rdmBit :: Word16 <- randomRIO (0, maxBound :: Word16)
  return $ fromIntegral (shiftL tsBit 24)
       .|. fromIntegral (shiftL tsBit' 16)
       .|. fromIntegral rdmBit

-- | Try to set logid for checkpoint store.
--
-- idx: 63...56...0
--      |    |    |
-- bit: 00...1...00
initCheckpointStoreLogID :: FFI.LDClient -> FFI.LogAttrs -> IO FFI.C_LogID
initCheckpointStoreLogID client attrs = do
  let logid = bit 56
  r <- try $ LD.getLogGroupByID client logid
  case r of
    Left (_ :: E.NOTFOUND) -> do
      _ <- LD.makeLogGroup client "/internal/checkpoint" logid logid attrs True
      return logid
    Right _ -> return logid

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
  -- ^ checkpointStore logid
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

stopReader :: FFI.LDClient -> FFI.LDReader -> StreamName -> IO ()
stopReader client reader name = do
  logid <- getCLogIDByStreamName client name
  LD.readerStopReading reader logid

stopCkpReader :: FFI.LDClient -> FFI.LDSyncCkpReader -> StreamName -> IO ()
stopCkpReader client reader name = do
  logid <- getCLogIDByStreamName client name
  LD.ckpReaderStopReading reader logid
