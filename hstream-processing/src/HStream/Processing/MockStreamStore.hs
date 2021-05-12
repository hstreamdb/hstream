{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.MockStreamStore
  ( MockMessage (..),
    MockStreamStore (..),
    MockConsumer,
    MockProducer,
    StreamName,
    Offset,
    Timestamp,
    RawConsumerRecord (..),
    RawProducerRecord (..),
    mkMockStreamStore,
    mkMockProducer,
    mkMockConsumer,
    pollRecords,
    send,
  )
where

import           HStream.Processing.Type
import           RIO
import qualified RIO.ByteString.Lazy     as BL
import qualified RIO.HashMap             as HM
import           RIO.HashMap.Partial     as HM'
import qualified RIO.HashSet             as HS
import qualified RIO.Text                as T

type StreamName = T.Text

type Offset = Word64

data MockMessage = MockMessage
  { mmTimestamp :: Timestamp,
    mmKey :: Maybe BL.ByteString,
    mmValue :: BL.ByteString
  }

data MockStreamStore = MockStreamStore
  { mssData :: TVar (HM.HashMap T.Text [MockMessage])
  }

mkMockStreamStore :: IO MockStreamStore
mkMockStreamStore = do
  s <- newTVarIO (HM.empty :: HM.HashMap T.Text [MockMessage])
  return $
    MockStreamStore
      { mssData = s
      }

data MockConsumer = MockConsumer
  { mcSubscribedStreams :: HS.HashSet StreamName,
    mcStreamOffsets :: HM.HashMap T.Text Offset,
    mcStore :: MockStreamStore
  }

data RawConsumerRecord = RawConsumerRecord
  { rcrTopic :: StreamName,
    rcrOffset :: Offset,
    rcrTimestamp :: Timestamp,
    rcrKey :: Maybe BL.ByteString,
    rcrValue :: BL.ByteString
  }

data RawProducerRecord = RawProducerRecord
  { rprTopic :: StreamName,
    rprKey :: Maybe BL.ByteString,
    rprValue :: BL.ByteString,
    rprTimestamp :: Timestamp
  }

pollRecords :: MockConsumer -> Int -> Int -> IO [RawConsumerRecord]
pollRecords MockConsumer {..} _ pollDuration = do
  -- just ignore records num limit
  threadDelay (pollDuration * 1000)
  atomically $ do
    dataStore <- readTVar $ mssData mcStore
    let r =
          HM.foldlWithKey'
            ( \a k v ->
                if HS.member k mcSubscribedStreams
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
                if HS.member k mcSubscribedStreams
                  then []
                  else v
            )
            dataStore
    writeTVar (mssData mcStore) newDataStore
    return r

mkMockConsumer :: MockStreamStore -> [StreamName] -> IO MockConsumer
mkMockConsumer streamStore streams =
  return
    MockConsumer
      { mcSubscribedStreams = HS.fromList streams,
        mcStreamOffsets = HM.empty,
        mcStore = streamStore
      }

data MockProducer = MockProducer
  { mpStore :: MockStreamStore
  }

mkMockProducer ::
  MockStreamStore ->
  IO MockProducer
mkMockProducer store =
  return
    MockProducer
      { mpStore = store
      }

send :: MockProducer -> RawProducerRecord -> IO ()
send MockProducer {..} RawProducerRecord {..} =
  atomically $ do
    let record =
          MockMessage
            { mmTimestamp = rprTimestamp,
              mmKey = rprKey,
              mmValue = rprValue
            }
    dataStore <- readTVar $ mssData mpStore
    if HM.member rprTopic dataStore
      then do
        let td = dataStore HM'.! rprTopic
        let newDataStore = HM.insert rprTopic (td ++ [record]) dataStore
        writeTVar (mssData mpStore) newDataStore
      else do
        let newDataStore = HM.insert rprTopic [record] dataStore
        writeTVar (mssData mpStore) newDataStore
