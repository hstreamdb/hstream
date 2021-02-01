{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Topic.MockStore
  ( MockMessage (..),
    MockTopicStore (..),
    MockTopicConsumer,
    MockTopicProducer,
    mkMockTopicStore,
    mkMockTopicProducer,
    mkMockTopicConsumer,
  )
where

import           HStream.Processing.Topic.Type
import           RIO
import qualified RIO.ByteString.Lazy           as BL
import qualified RIO.HashMap                   as HM
import           RIO.HashMap.Partial           as HM'
import qualified RIO.HashSet                   as HS
import qualified RIO.Text                      as T

data MockMessage
  = MockMessage
      { mmTimestamp :: Timestamp,
        mmKey :: Maybe BL.ByteString,
        mmValue :: BL.ByteString
      }

data MockTopicStore
  = MockTopicStore
      { mtsData :: TVar (HM.HashMap T.Text [MockMessage])
      }

mkMockTopicStore :: IO MockTopicStore
mkMockTopicStore = do
  s <- newTVarIO (HM.empty :: HM.HashMap T.Text [MockMessage])
  return $
    MockTopicStore
      { mtsData = s
      }

data MockTopicConsumer
  = MockTopicConsumer
      { mtcSubscribedTopics :: HS.HashSet TopicName,
        mtcTopicOffsets :: HM.HashMap T.Text Offset,
        mtcStore :: MockTopicStore
      }

instance TopicConsumer MockTopicConsumer where
  -- subscribe tc topicNames = return $ tc {mtcSubscribedTopics = HS.fromList topicNames}

  -- just ignore records num limit
  pollRecords MockTopicConsumer {..} _ pollDuration = do
    threadDelay (pollDuration * 1000)
    atomically $ do
      dataStore <- readTVar $ mtsData mtcStore
      let r =
            HM.foldlWithKey'
              ( \a k v ->
                  if HS.member k mtcSubscribedTopics
                    then
                      a
                        ++ map
                          ( \MockMessage {..} ->
                              RawConsumerRecord
                                { rcrTopic = k,
                                  rcrOffset = 0,
                                  rcrTimestamp = mmTimestamp,
                                  rcrKey = mmKey,
                                  rcrValue = mmValue
                                }
                          )
                          v
                    else a
              )
              []
              dataStore
      let newDataStore =
            HM.mapWithKey
              ( \k v ->
                  if HS.member k mtcSubscribedTopics
                    then []
                    else v
              )
              dataStore
      writeTVar (mtsData mtcStore) newDataStore
      return r

mkMockTopicConsumer :: MockTopicStore -> [TopicName] -> IO MockTopicConsumer
mkMockTopicConsumer topicStore topics =
  return
    MockTopicConsumer
      { mtcSubscribedTopics = HS.fromList topics,
        mtcTopicOffsets = HM.empty,
        mtcStore = topicStore
      }

data MockTopicProducer
  = MockTopicProducer
      { mtpStore :: MockTopicStore
      }

mkMockTopicProducer ::
  MockTopicStore ->
  IO MockTopicProducer
mkMockTopicProducer store =
  return
    MockTopicProducer
      { mtpStore = store
      }

instance TopicProducer MockTopicProducer where
  send MockTopicProducer {..} RawProducerRecord {..} =
    atomically $ do
      let record =
            MockMessage
              { mmTimestamp = rprTimestamp,
                mmKey = rprKey,
                mmValue = rprValue
              }
      dataStore <- readTVar $ mtsData mtpStore
      if HM.member rprTopic dataStore
        then do
          let td = dataStore HM'.! rprTopic
          let newDataStore = HM.insert rprTopic (td ++ [record]) dataStore
          writeTVar (mtsData mtpStore) newDataStore
        else do
          let newDataStore = HM.insert rprTopic [record] dataStore
          writeTVar (mtsData mtpStore) newDataStore
