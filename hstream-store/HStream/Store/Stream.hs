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
  , LD.logGroupGetExtraAttr
  , LD.logGroupUpdateExtraAttrs

    -- * Writer
  , LD.append
  , appendRecord
  , ProducerRecord (..)
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable
  , FFI.AppendCallBackData (..)
  , encodeRecord

    -- * Reader
  , ConsumerRecord (..)
  , decodeRecord
  , FFI.DataRecord (..)

  , LD.newFileBasedCheckpointStore
  , LD.newRSMBasedCheckpointStore
  , LD.newZookeeperBasedCheckpointStore
  , LD.ckpStoreGetLSN

  , LD.newLDReader
  , LD.readerStartReading
  , LD.readerRead
  , LD.readerSetTimeout
  , readerReadRecord

  , newLDFileCkpReader
  , newLDRsmCkpReader
  , newLDZkCkpReader
  , LD.writeCheckpoints
  , LD.writeLastCheckpoints
  , LD.ckpReaderStartReading
  , LD.startReadingFromCheckpoint
  , LD.ckpReaderRead
  , LD.ckpReaderSetTimeout
  ) where

import           Control.Exception                (try)
import           Control.Monad                    (unless)
import           Data.Bits                        (shiftL, shiftR, (.&.), (.|.))
import qualified Data.Cache                       as Cache
import           Data.Int                         (Int64)
import           Data.Time.Clock.System           (SystemTime (..))
import           Data.Word                        (Word16, Word32, Word64)
import           Foreign.C                        (CSize)
import           GHC.Generics                     (Generic)
import           GHC.Stack                        (HasCallStack, callStack)
import           System.IO.Unsafe                 (unsafePerformIO)
import           System.Random                    (randomRIO)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.JSON                      as JSON
import qualified Z.Data.MessagePack               as MP
import           Z.Data.Vector                    (Bytes)
import           Z.IO.Time                        (getSystemTime')

import qualified HStream.Store.Exception          as E
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Internal.Types     as FFI

-------------------------------------------------------------------------------

type StreamName = CBytes

-- | Global Stream name to logid cache
topicNameCache :: Cache.Cache StreamName FFI.C_LogID
topicNameCache = unsafePerformIO $ Cache.newCache Nothing
{-# NOINLINE topicNameCache #-}

-- | Create stream
--
-- Currently a Stream is a loggroup which only contains one random logid.
createStream :: HasCallStack => FFI.LDClient -> StreamName -> FFI.LogAttrs -> IO ()
createStream client topic attrs = go 10
  where
    go :: Int -> IO ()
    go maxTries =
      if maxTries <= 0
         then E.throwStoreError "Ran out all retries, but still failed :(" callStack
         else do
           logid <- genRandomLogID
           result <- try $ LD.makeLogGroup client topic logid logid attrs True
           case result of
             Right group -> do
               LD.syncLogsConfigVersion client =<< LD.logGroupGetVersion group
               Cache.insert topicNameCache topic logid
             Left (_ :: E.ID_CLASH) -> go $! maxTries - 1

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
  -- Do NOT combine these operations to a atomically one, since we need
  -- delete the old topic name even the new one is insert failed.
  m_v <- Cache.lookup' topicNameCache from
  case m_v of
    Just x -> do Cache.delete topicNameCache from
                 Cache.insert topicNameCache to x
    Nothing -> return ()

removeStream :: HasCallStack => FFI.LDClient -> StreamName -> IO ()
removeStream client topic = do
  LD.syncLogsConfigVersion client =<< LD.removeLogGroup client topic
  Cache.delete topicNameCache topic

doesStreamExists :: HasCallStack => FFI.LDClient -> StreamName -> IO Bool
doesStreamExists client topic = do
  m_v <- Cache.lookup topicNameCache topic
  case m_v of
    Just _  -> return True
    Nothing -> do r <- try $ LD.getLogGroup client topic
                  case r of
                    Left (_ :: E.NOTFOUND) -> return False
                    Right _                -> return True

getCLogIDByStreamName :: FFI.LDClient -> StreamName -> IO FFI.C_LogID
getCLogIDByStreamName client topic = do
  m_v <- Cache.lookup topicNameCache topic
  maybe (fmap fst $ LD.logGroupGetRange =<< LD.getLogGroup client topic) return m_v

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

data ProducerRecord = ProducerRecord
  { dataInTopic     :: StreamName
  , dataInKey       :: Maybe CBytes
  , dataInValue     :: Bytes
  , dataInTimestamp :: Int64
  } deriving (Show, Generic, JSON.JSON, MP.MessagePack)

encodeRecord :: ProducerRecord -> Bytes
encodeRecord = JSON.encode

data ConsumerRecord = ConsumerRecord
  { dataOutTopic     :: StreamName
  , dataOutOffset    :: Word64
  , dataOutKey       :: Maybe CBytes
  , dataOutValue     :: Bytes
  , dataOutTimestamp :: Int64
  } deriving (Show, Generic, JSON.JSON, MP.MessagePack)

decodeRecord :: HasCallStack => FFI.DataRecord -> ConsumerRecord
decodeRecord FFI.DataRecord{..} = do
  case JSON.decode' recordPayload of
    -- TODO
    Left _err -> error "JSON decode error!"
    Right ProducerRecord{..} ->
      ConsumerRecord { dataOutTopic     = dataInTopic
                     , dataOutOffset    = recordLSN
                     , dataOutKey       = dataInKey
                     , dataOutValue     = dataInValue
                     , dataOutTimestamp = dataInTimestamp
                     }

-- | Appends a new record.
appendRecord
  :: HasCallStack
  => FFI.LDClient
  -> FFI.C_LogID
  -> ProducerRecord
  -> Maybe (FFI.KeyType, CBytes)
  -> IO FFI.AppendCallBackData
appendRecord client logid payload m_key_attr =
  LD.append client logid (encodeRecord payload) m_key_attr

readerReadRecord :: FFI.LDReader -> Int -> IO [ConsumerRecord]
readerReadRecord reader maxlen = map decodeRecord <$> LD.readerRead reader maxlen

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
