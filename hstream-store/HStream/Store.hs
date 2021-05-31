{-# LANGUAGE PatternSynonyms #-}

module HStream.Store
  ( LDClient
  , LDSyncCkpReader
  , LSN
  , pattern LSN_MAX
  , pattern LSN_MIN
  , pattern LSN_INVALID

  , newLDClient
  , getMaxPayloadSize
  , setClientSetting
  , setClientSettings
  , getClientSetting
  , getTailLSN
  , trim

    -- * Stream
  , module HStream.Store.Stream

    -- * Logger
  , module HStream.Store.Logger

    -- * Exception
  , module HStream.Store.Exception

    -- * DEPRECATED
  , ProducerConfig (..), Producer, mkProducer
  , ConsumerConfig (..), Consumer, mkConsumer
  , sendMessage , pollMessages
  , AdminClientConfig (..), mkAdminClient, AdminClient
  , createTopic_, doesTopicExists_
  ) where

import           Control.Monad                    (forM_)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           GHC.Stack                        (HasCallStack)

import           HStream.Store.Exception
import           HStream.Store.Internal.LogDevice
import           HStream.Store.Internal.Types
import           HStream.Store.Logger
import           HStream.Store.Stream

-- DEPRECATED {
import           Control.Monad                    (forM, void)
import           Data.Int
import           Data.Word
import           Z.Data.CBytes                    (CBytes)
-- }

setClientSettings :: HasCallStack => LDClient -> Map CBytes CBytes -> IO ()
setClientSettings client settings = forM_ (Map.toList settings) $ \(k, v) -> do
  setClientSetting client k v

-------------------------------------------------------------------------------
-- DEPRECATED

{-# DEPRECATED ProducerConfig "" #-}
newtype ProducerConfig = ProducerConfig { producerConfigUri :: CBytes }
  deriving (Show)

{-# DEPRECATED Producer "" #-}
newtype Producer = Producer LDClient

{-# DEPRECATED ConsumerConfig "" #-}
data ConsumerConfig = ConsumerConfig
  { consumerConfigUri         :: CBytes
  , consumerName              :: CBytes
    -- ^ Unique identifier of one consumer
  , consumerBufferSize        :: Int64
    -- ^ specify the read buffer size for this client, fallback
    -- to the value in settings if it is -1
  , consumerCheckpointUri     :: CBytes
  , consumerCheckpointRetries :: Word32
  } deriving (Show)

{-# DEPRECATED Consumer "" #-}
data Consumer = Consumer
  { _unConsumer     :: LDSyncCkpReader
  , _consumerTopics :: Map StreamName C_LogID
  }

{-# DEPRECATED mkProducer "" #-}
mkProducer :: ProducerConfig -> IO Producer
mkProducer config = do
  client <- newLDClient (producerConfigUri config)
  return $ Producer client

{-# DEPRECATED mkConsumer "" #-}
mkConsumer :: ConsumerConfig -> [StreamName] -> IO Consumer
mkConsumer ConsumerConfig{..} ts = do
  client <- newLDClient consumerConfigUri
  topics <- forM ts $ \t -> do
    topicID <- getCLogIDByStreamName client t
    lastSN <- getTailLSN client topicID
    return (topicID, lastSN)
  ckpReader <- newLDFileCkpReader client consumerName consumerCheckpointUri
                                  (fromIntegral $ length ts) (Just consumerBufferSize)
                                  consumerCheckpointRetries
  forM_ topics $ \(topicID, lastSN)-> ckpReaderStartReading ckpReader topicID (lastSN + 1) LSN_MAX
  return $ Consumer ckpReader (Map.fromList $ zip ts (map fst topics))

{-# DEPRECATED sendMessage "" #-}
sendMessage :: Producer -> ProducerRecord -> IO ()
sendMessage (Producer client) record@ProducerRecord{..} = do
  topicID <- getCLogIDByStreamName client dataInTopic
  void $ appendRecord client topicID record Nothing

{-# DEPRECATED pollMessages "" #-}
pollMessages :: Consumer -> Int -> Int32 -> IO [ConsumerRecord]
pollMessages (Consumer reader _) maxRecords timeout = do
  void $ ckpReaderSetTimeout reader timeout
  map decodeRecord <$> ckpReaderRead reader maxRecords

{-# DEPRECATED AdminClientConfig "" #-}
newtype AdminClientConfig = AdminClientConfig { adminConfigUri :: CBytes }

{-# DEPRECATED AdminClient "" #-}
newtype AdminClient = AdminClient LDClient

{-# DEPRECATED mkAdminClient "" #-}
mkAdminClient :: AdminClientConfig -> IO AdminClient
mkAdminClient AdminClientConfig{..} = do
  client <- newLDClient adminConfigUri
  return $ AdminClient client

{-# DEPRECATED createTopic_ "" #-}
createTopic_ :: AdminClient -> StreamName -> LogAttrs -> IO ()
createTopic_ (AdminClient client) = createStream client

{-# DEPRECATED doesTopicExists_ "" #-}
doesTopicExists_ :: AdminClient -> StreamName -> IO Bool
doesTopicExists_ (AdminClient client) = doesStreamExists client
