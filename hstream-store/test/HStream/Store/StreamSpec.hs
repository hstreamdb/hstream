{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.StreamSpec (spec) where

import           Control.Concurrent               (threadDelay)
import           Control.Monad                    (replicateM, void)
import           Data.Int
import qualified Data.Map.Strict                  as Map
import           Test.Hspec
import           Z.Data.Vector.Base               (Bytes)

import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "StreamSpec" $ do
  base
  writeReadSpec

base :: Spec
base = describe "BaseSpec" $ do
  streamId <- S.mkStreamId S.StreamTypeStream <$> runIO (newRandomName 5)
  (logPath, _key) <- runIO $ S.getStreamLogPath streamId Nothing
  newStreamId <- S.mkStreamId S.StreamTypeStream <$> runIO (newRandomName 5)
  (newLogPath, _new_key) <- runIO $ S.getStreamLogPath newStreamId Nothing

  it "create stream" $ do
    print $ "Create a new stream: " <> S.showStreamName streamId
    S.doesStreamExist client streamId `shouldReturn` False
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createStream client streamId attrs
    S.doesStreamExist client streamId `shouldReturn` True

    S.doesStreamPartitionExist client streamId Nothing `shouldReturn` True
    S.doesStreamPartitionExist client streamId (Just "some_non_exist_key") `shouldReturn` False
    non_exist_stream <- S.mkStreamId S.StreamTypeStream <$> newRandomName 5
    S.doesStreamPartitionExist client non_exist_stream Nothing `shouldReturn` False

    ss <- S.findStreams client S.StreamTypeStream
    ss `shouldContain` [streamId]

  it "stream partition" $ do
    let keyString = "some_key"
        key = Just keyString
    streamName1 <- newRandomName 5
    streamName2 <- newRandomName 5
    let stream1 = S.mkStreamId S.StreamTypeStream streamName1
        stream2 = S.mkStreamId S.StreamTypeStream streamName2
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.empty
                                        }
    S.createStream client stream1 attrs
    S.doesStreamExist client stream1 `shouldReturn` True
    S.doesStreamPartitionExist client stream1 key `shouldReturn` False

    log_id <- S.createStreamPartition client stream1 key
    S.doesStreamPartitionExist client stream1 key `shouldReturn` True
    S.listStreamPartitions client stream1 `shouldReturn` [keyString]

    S.renameStream' client stream1 streamName2
    S.doesStreamPartitionExist client stream1 key `shouldReturn` False
    S.doesStreamPartitionExist client stream2 key `shouldReturn` True
    S.listStreamPartitions client stream1 `shouldThrow` S.isNOTFOUND
    S.listStreamPartitions client stream2 `shouldReturn` [keyString]

    S.getUnderlyingLogId client stream2 key `shouldReturn` log_id

  it "create the same stream should throw EXISTS" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createStream client streamId attrs `shouldThrow` S.isEXISTS

  it "get full path of loggroup by name or id shoule be equal" $ do
    logpath <- S.logGroupGetFullName =<< S.getLogGroup client logPath
    logid <- S.getUnderlyingLogId client streamId Nothing
    logpath' <- S.logGroupGetFullName =<< S.getLogGroupByID client logid
    logpath `shouldBe` logpath'

  it "archive stream" $ do
    S.archiveStream client streamId
    ss <- S.findStreams client S.StreamTypeStream
    ss `shouldNotContain` [streamId]
    S.unArchiveStream client streamId
    ss' <- S.findStreams client S.StreamTypeStream
    ss' `shouldContain` [streamId]

  it "rename stream" $ do
    print $ "Rename stream " <> S.showStreamName streamId <> " to " <> S.showStreamName newStreamId
    S.renameStream' client streamId (S.streamName newStreamId)
    S.doesStreamExist client streamId `shouldReturn` False
    S.doesStreamExist client newStreamId `shouldReturn` True

  it "stream replication factor" $ do
    S.getStreamReplicaFactor client newStreamId `shouldReturn` 1

  it "stream head record timestamp" $ do
    -- since there is no records in this stream
    S.getStreamPartitionHeadTimestamp client newStreamId Nothing `shouldReturn` Nothing
    logid <- S.getUnderlyingLogId client newStreamId Nothing
    _ <- S.append client logid "hello" Nothing
    let cond mv = case mv of
                    Just v  -> v > 0 && v < (maxBound :: Int64)
                    Nothing -> error "predicate failed"
    S.getStreamPartitionHeadTimestamp client newStreamId Nothing >>= (`shouldSatisfy` cond)

  it "get/set extra-attrs" $ do
    Map.lookup "greet" <$> S.getStreamExtraAttrs client newStreamId
      `shouldReturn` Just "hi"
    Map.lookup "greet" <$> S.updateStreamExtraAttrs client newStreamId (Map.singleton "greet" "hiiii")
      `shouldReturn` Just "hi"
    Map.lookup "greet" <$> S.getStreamExtraAttrs client newStreamId
      `shouldReturn` Just "hiiii"
    void $ S.updateStreamExtraAttrs client newStreamId (Map.singleton "greet" "hi")

    -- internal functions
    logGroup <- S.getLogGroup client newLogPath
    S.logGroupGetExtraAttr logGroup "greet" `shouldReturn` Just "hi"
    S.logGroupGetExtraAttr logGroup "A" `shouldReturn` Just "B"
    S.logGroupGetExtraAttr logGroup "Alice" `shouldReturn` Nothing
    S.logGroupUpdateExtraAttrs client logGroup $ Map.fromList [("greet", "hello"), ("Alice", "Bob")]

    logGroup_ <- S.getLogGroup client newLogPath
    S.logGroupGetExtraAttr logGroup_ "greet" `shouldReturn` Just "hello"
    S.logGroupGetExtraAttr logGroup_ "A" `shouldReturn` Just "B"
    S.logGroupGetExtraAttr logGroup_ "Alice" `shouldReturn` Just "Bob"

  it "remove the stream" $ do
    S.removeStream client newStreamId
    S.doesStreamExist client newStreamId `shouldReturn` False

writeReadSpec :: Spec
writeReadSpec = describe "WriteReadSpec" $ do
  it "simple write read" $ do
    streamid <- S.mkStreamId S.StreamTypeStream <$> newRandomName 5
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.empty
                                        }
    S.createStream client streamid attrs
    S.doesStreamExist client streamid `shouldReturn` True
    logid <- S.getUnderlyingLogId client streamid Nothing
    -- NOTE: wait logid avariable
    threadDelay 1000000
    sn <- S.appendCompLSN <$> S.append client logid "hello" Nothing
    sn' <- S.getTailLSN client logid
    sn `shouldBe` sn'
    reader <- S.newLDReader client 1 Nothing
    S.readerStartReading reader logid sn sn
    [record] <- S.readerRead reader 10
    S.recordPayload record `shouldBe` ("hello" :: Bytes)

  it "archive a stream should not effect exist reading" $ do
    streamid <- S.mkStreamId S.StreamTypeStream <$> newRandomName 5
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.empty
                                        }
    S.createStream client streamid attrs
    S.doesStreamExist client streamid `shouldReturn` True
    logid <- S.getUnderlyingLogId client streamid Nothing
    -- NOTE: wait logid avariable
    threadDelay 1000000
    res <- replicateM 3 (S.appendCompLSN <$> S.append client logid "hello" Nothing)
    length res `shouldBe` 3
    reader <- S.newLDReader client 1 Nothing
    S.readerStartReading reader logid (head res) (last res)
    [record] <- S.readerRead reader 1
    S.recordPayload record `shouldBe` ("hello" :: Bytes)

    S.archiveStream client streamid
    res' <- S.readerRead reader 2
    map S.recordPayload res' `shouldBe` ["hello" :: Bytes, "hello"]
