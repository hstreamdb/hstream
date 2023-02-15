{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.MockStreamStore
  ( MockMessage (..),
    mkMockStreamStore,
    mkMockStoreSourceConnector,
    mkMockStoreSinkConnector,
  )
where

import           HStream.Processing.Connector
import           HStream.Processing.Type
import           RIO
import qualified RIO.ByteString.Lazy          as BL
import qualified RIO.HashMap                  as HM
import           RIO.HashMap.Partial          as HM'
import qualified RIO.HashSet                  as HS
import qualified RIO.Text                     as T

data MockMessage = MockMessage
  { mmTimestamp :: Timestamp,
    mmKey       :: Maybe BL.ByteString,
    mmValue     :: BL.ByteString
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

mkMockStoreSourceConnector :: MockStreamStore -> IO SourceConnector
mkMockStoreSourceConnector mockStore = do
  consumer <- mkMockConsumer mockStore
  return $
    SourceConnector
      { subscribeToStream = subscribeToStreamMock consumer,
        unSubscribeToStream = unSubscribeToStreamMock consumer,
        readRecords = readRecordsMock consumer,
        commitCheckpoint = commitCheckpointMock consumer
      }

mkMockStoreSourceConnectorWithoutCkp :: MockStreamStore -> IO SourceConnectorWithoutCkp
mkMockStoreSourceConnectorWithoutCkp mockStore = undefined

mkMockStoreSinkConnector :: MockStreamStore -> IO SinkConnector
mkMockStoreSinkConnector mockStore = do
  producer <- mkMockProducer mockStore
  return $
    SinkConnector
      { writeRecord = \_ _ -> writeRecordMock producer
      }

-- 实现的时候都要传一个隐式的 共享的 client 进去的，
-- 状态是可变的
data MockConsumer = MockConsumer
  { mcSubscribedStreams :: IORef (HS.HashSet StreamName),
    mcStore             :: MockStreamStore
  }

mkMockConsumer :: MockStreamStore -> IO MockConsumer
mkMockConsumer streamStore = do
  s <- newIORef HS.empty
  return
    MockConsumer
      { mcSubscribedStreams = s,
        mcStore = streamStore
      }

subscribeToStreamMock :: MockConsumer -> StreamName -> Offset -> IO ()
subscribeToStreamMock MockConsumer {..} streamName _ = do
  old <- readIORef mcSubscribedStreams
  writeIORef mcSubscribedStreams $ HS.insert streamName old

unSubscribeToStreamMock :: MockConsumer -> StreamName -> IO ()
unSubscribeToStreamMock MockConsumer {..} streamName = do
  old <- readIORef mcSubscribedStreams
  writeIORef mcSubscribedStreams $ HS.delete streamName old

readRecordsMock :: MockConsumer -> IO [SourceRecord]
readRecordsMock MockConsumer {..} = do
  threadDelay (1000 * 1000)
  streams <- readIORef mcSubscribedStreams
  atomically $ do
    dataStore <- readTVar $ mssData mcStore
    let r =
          HM.foldlWithKey'
            ( \a k v ->
                if HS.member k streams
                  then
                    a
                      ++ map
                        ( \MockMessage {..} ->
                            SourceRecord
                              { srcStream = k,
                                srcOffset = 0,
                                srcTimestamp = mmTimestamp,
                                srcKey = mmKey,
                                srcValue = mmValue
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
                if HS.member k streams
                  then []
                  else v
            )
            dataStore
    writeTVar (mssData mcStore) newDataStore
    return r

commitCheckpointMock :: MockConsumer -> StreamName -> Offset -> IO ()
commitCheckpointMock _ _ _ = return ()

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

writeRecordMock :: MockProducer -> SinkRecord -> IO ()
writeRecordMock MockProducer {..} SinkRecord {..} = do
  atomically $ do
    let record =
          MockMessage
            { mmTimestamp = snkTimestamp,
              mmKey = snkKey,
              mmValue = snkValue
            }
    dataStore <- readTVar $ mssData mpStore
    if HM.member snkStream dataStore
      then do
        let td = dataStore HM'.! snkStream
        let newDataStore = HM.insert snkStream (td ++ [record]) dataStore
        writeTVar (mssData mpStore) newDataStore
      else do
        let newDataStore = HM.insert snkStream [record] dataStore
        writeTVar (mssData mpStore) newDataStore
