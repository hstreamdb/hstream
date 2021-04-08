{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE RecordWildCards    #-}

module HStream.Store
  ( S.Topic
  , S.TopicAttrs (..)

    -- * Producer
  , ProducerRecord (..)
  , ProducerConfig (..)
  , Producer
  , mkProducer
  , sendMessage
  , sendMessages

    -- * Consumer
  , ConsumerRecord (..)
  , ConsumerConfig (..)
  , Consumer
  , mkConsumer
  , pollMessages
  , seek
  , commitOffsets
  , commitAllOffsets

    -- * Admin
  , AdminClientConfig (..)
  , AdminClient
  , mkAdminClient
  , createTopics
  , doesTopicExists

    -- * Logger
  , module HStream.Store.Logger

    -- * Exception
  , module HStream.Store.Exception
  ) where

import           Control.Monad           (forM, forM_, void)
import           Data.Int                (Int32, Int64)
import           Data.Map.Strict         (Map)
import qualified Data.Map.Strict         as Map
import           Data.Word               (Word32)
import           GHC.Generics            (Generic)
import           Z.Data.CBytes           (CBytes)
import qualified Z.Data.JSON             as JSON
import           Z.Data.Vector           (Bytes)

import           HStream.Store.Exception
import           HStream.Store.Logger
import qualified HStream.Store.Stream    as S

-------------------------------------------------------------------------------

data ProducerRecord = ProducerRecord
  { dataInTopic     :: S.Topic
  , dataInKey       :: Maybe CBytes
  , dataInValue     :: Bytes
  , dataInTimestamp :: Int64
  } deriving (Show, Generic, JSON.JSON)

newtype ProducerConfig = ProducerConfig { producerConfigUri :: CBytes }
  deriving (Show, Generic, JSON.JSON)

newtype Producer = Producer S.StreamClient

mkProducer :: ProducerConfig -> IO Producer
mkProducer config = do
  client <- S.newStreamClient (producerConfigUri config)
  return $ Producer client

sendMessage :: Producer -> ProducerRecord -> IO ()
sendMessage (Producer client) record@ProducerRecord{..} = do
  topicID <- S.getTopicIDByName client dataInTopic
  void $ S.append client topicID (JSON.encode record) Nothing return

-- FIXME: performance improvements
sendMessages :: Producer -> [ProducerRecord] -> IO ()
sendMessages producer xs = forM_ xs $ sendMessage producer

-------------------------------------------------------------------------------

data ConsumerRecord = ConsumerRecord
  { dataOutTopic     :: S.Topic
  , dataOutOffset    :: S.SequenceNum
  , dataOutKey       :: Maybe CBytes
  , dataOutValue     :: Bytes
  , dataOutTimestamp :: Int64
  } deriving (Show, Generic, JSON.JSON)

dataRecordToConsumerRecord :: S.DataRecord -> ConsumerRecord
dataRecordToConsumerRecord S.DataRecord{..} = do
  case JSON.decode' recordPayload of
    Left _err -> error "JSON decode error!"
    Right ProducerRecord{..} ->
      ConsumerRecord { dataOutTopic     = dataInTopic
                     , dataOutOffset    = recordLSN
                     , dataOutKey       = dataInKey
                     , dataOutValue     = dataInValue
                     , dataOutTimestamp = dataInTimestamp
                     }

data ConsumerConfig = ConsumerConfig
  { consumerConfigUri         :: CBytes
  , consumerName              :: CBytes
    -- ^ Unique identifier of one consumer
  , consumerBufferSize        :: Int64
    -- ^ specify the read buffer size for this client, fallback
    -- to the value in settings if it is -1
  , consumerCheckpointUri     :: CBytes
  , consumerCheckpointRetries :: Word32
  } deriving (Show, Generic, JSON.JSON)

data Consumer = Consumer
  { _unConsumer     :: S.StreamSyncCheckpointedReader
  , _consumerTopics :: Map S.Topic S.TopicID
  }

mkConsumer :: ConsumerConfig -> [S.Topic] -> IO Consumer
mkConsumer ConsumerConfig{..} ts = do
  client <- S.newStreamClient consumerConfigUri
  topics <- forM ts $ \t -> do
    topicID <- S.getTopicIDByName client t
    lastSN <- S.getTailSequenceNum client topicID
    return (topicID, lastSN)
  -- Note that after you create a reader form client, then the client may be
  -- "moved", which means all functions that receive client as an argument are
  -- in an undefined behaviour.
  reader <- S.newStreamReader client (fromIntegral $ length ts) consumerBufferSize
  checkpointStore <- S.newFileBasedCheckpointStore consumerCheckpointUri
  checkpointedReader <- S.newStreamSyncCheckpointReader consumerName reader checkpointStore consumerCheckpointRetries
  forM_ topics $ \(topicID, lastSN)-> do
    S.checkpointedReaderStartReading checkpointedReader topicID (lastSN + 1) maxBound
  return $ Consumer checkpointedReader (Map.fromList $ zip ts (map fst topics))

pollMessages :: Consumer -> Int -> Int32 -> IO [ConsumerRecord]
pollMessages (Consumer reader _) maxRecords timeout = do
  void $ S.checkpointedReaderSetTimeout reader timeout
  map dataRecordToConsumerRecord <$> S.checkpointedReaderRead reader maxRecords

seek :: Consumer -> S.Topic -> S.SequenceNum -> IO ()
seek (Consumer reader topics) topic sn = do
  case Map.lookup topic topics of
    Just topicID -> S.checkpointedReaderStartReading reader topicID sn maxBound
    Nothing      -> error $ "Can not find topic:" <> show topic

commitOffsets :: Consumer -> [S.Topic] -> IO ()
commitOffsets (Consumer checkpointedReader topics) ts = do
  topicIDs <- forM ts $ \t ->
    case Map.lookup t topics of
      Just topicIDs -> return topicIDs
      Nothing       -> error $ "Can not find topic:" <> show t
  S.writeLastCheckpointsSync checkpointedReader topicIDs

commitAllOffsets :: Consumer -> IO ()
commitAllOffsets (Consumer checkpointedReader ts) =
  S.writeLastCheckpointsSync checkpointedReader (Map.elems ts)

newtype AdminClientConfig = AdminClientConfig { adminConfigUri :: CBytes }
newtype AdminClient = AdminClient S.StreamClient

mkAdminClient :: AdminClientConfig -> IO AdminClient
mkAdminClient AdminClientConfig{..} = do
  client <- S.newStreamClient adminConfigUri
  return $ AdminClient client

createTopics :: AdminClient -> Map S.Topic S.TopicAttrs -> IO ()
createTopics (AdminClient client) = S.createTopicsSync client

doesTopicExists :: AdminClient -> S.Topic -> IO Bool
doesTopicExists (AdminClient client) =
  S.doesTopicExists client
