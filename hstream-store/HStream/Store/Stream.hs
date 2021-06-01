{-# LANGUAGE DeriveAnyClass #-}

module HStream.Store.Stream
  ( -- * StreamName
    StreamName
  , FFI.LogAttrs (LogAttrs)
  , FFI.HsLogAttrs (..)
  , createStream
  , renameStream
  , removeStream
  , doesStreamExists

  , FFI.LogID (..)
  , FFI.C_LogID
  , getCLogIDByStreamName
  , LD.getLogGroup
  , LD.getLogGroupByID
  , LD.logGroupGetName
  , LD.logGroupGetFullyQualifiedName
  , LD.logGroupGetExtraAttr
  , LD.logGroupUpdateExtraAttrs

    -- * Writer
  , LD.append
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable
  , FFI.AppendCallBackData (..)

    -- * Reader
  , FFI.RecordByteOffset (..)
  , FFI.DataRecord (..)

  , LD.newFileBasedCheckpointStore
  , LD.newRSMBasedCheckpointStore
  , LD.newZookeeperBasedCheckpointStore
  , LD.ckpStoreGetLSN

  , LD.newLDReader
  , LD.readerStartReading
  , LD.readerRead
  , LD.readerSetTimeout
  , LD.readerSetWithoutPayload
  , LD.readerSetIncludeByteOffset
  , LD.readerSetWaitOnlyWhenNoData
  , stopReader

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
  ) where

import           Control.Exception                (try)
import           Control.Monad                    (unless)
import           Data.Bits                        (shiftL, shiftR, (.&.), (.|.))
import qualified Data.Cache                       as Cache
import           Data.Int                         (Int64)
import           Data.Time.Clock.System           (SystemTime (..))
import           Data.Word                        (Word16, Word32)
import           Foreign.C                        (CSize)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
import           System.Random                    (randomRIO)
import           Z.Data.CBytes                    (CBytes)
import           Z.IO.Time                        (getSystemTime')

import qualified HStream.Store.Exception          as E
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Internal.Types     as FFI
import qualified HStream.Store.Logger             as Log

-------------------------------------------------------------------------------

type StreamName = CBytes

-- | Global Stream name to logid cache
streamNameCache :: Cache.Cache StreamName FFI.C_LogID
streamNameCache = unsafePerformIO $ Cache.newCache Nothing
{-# NOINLINE streamNameCache #-}

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
           result <- try $ LD.makeLogGroup client stream logid logid attrs True
           case result of
             Right group -> do
               LD.syncLogsConfigVersion client =<< LD.logGroupGetVersion group
               Cache.insert streamNameCache stream logid
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
  LD.syncLogsConfigVersion client =<< LD.renameLogGroup client from to
  -- FIXME
  -- Do NOT combine these operations to a atomically one, since we need
  -- delete the old stream name even the new one is insert failed.
  m_v <- Cache.lookup' streamNameCache from
  case m_v of
    Just x -> do Cache.delete streamNameCache from
                 Cache.insert streamNameCache to x
    Nothing -> return ()

removeStream :: HasCallStack => FFI.LDClient -> StreamName -> IO ()
removeStream client stream = do
  LD.syncLogsConfigVersion client =<< LD.removeLogGroup client stream
  Cache.delete streamNameCache stream

doesStreamExists :: HasCallStack => FFI.LDClient -> StreamName -> IO Bool
doesStreamExists client stream = do
  m_v <- Cache.lookup streamNameCache stream
  case m_v of
    Just _  -> return True
    Nothing -> do r <- try $ LD.getLogGroup client stream
                  case r of
                    Left (_ :: E.NOTFOUND) -> return False
                    Right group -> do
                      logid <- fst <$> LD.logGroupGetRange group
                      Cache.insert streamNameCache stream logid
                      return True

getCLogIDByStreamName :: FFI.LDClient -> StreamName -> IO FFI.C_LogID
getCLogIDByStreamName client stream = do
  m_v <- Cache.lookup streamNameCache stream
  case m_v of
    Just v -> return v
    Nothing -> do
      logid <- fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client stream)
      Cache.insert streamNameCache stream logid
      return logid

-- | Generate a random logid through a simplify version of snowflake algorithm.
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

stopReader :: FFI.LDClient -> FFI.LDReader -> CBytes -> IO ()
stopReader client reader name = do
  logid <- getCLogIDByStreamName client name
  LD.readerStopReading reader logid

stopCkpReader :: FFI.LDClient -> FFI.LDSyncCkpReader -> CBytes -> IO ()
stopCkpReader client reader name = do
  logid <- getCLogIDByStreamName client name
  LD.ckpReaderStopReading reader logid
