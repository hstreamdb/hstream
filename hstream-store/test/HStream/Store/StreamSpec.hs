{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.StreamSpec (spec) where

import           Data.Int
import qualified Data.Map.Strict         as Map
import           Test.Hspec

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = do
  base

base :: Spec
base = describe "HStream.Store.Stream" $ do
  streamId <- S.mkStreamId S.StreamTypeStream <$> runIO (newRandomName 5)
  logPath <- runIO $ S.getUnderlyingLogPath streamId
  newStreamId <- S.mkStreamId S.StreamTypeStream <$> runIO (newRandomName 5)
  newLogPath <- runIO $ S.getUnderlyingLogPath newStreamId

  it "create stream" $ do
    print $ "Create a new stream: " <> S.showStreamName streamId
    S.doesStreamExists client streamId `shouldReturn` False
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createStream client streamId attrs
    S.doesStreamExists client streamId `shouldReturn` True

  it "create the same stream should throw exception" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createStream client streamId attrs `shouldThrow` existsException

  it "get full path of loggroup by name or id shoule be equal" $ do
    logpath <- S.logGroupGetFullName =<< S.getLogGroup client logPath
    logid <- S.getUnderlyingLogId client streamId
    logpath' <- S.logGroupGetFullName =<< S.getLogGroupByID client logid
    logpath `shouldBe` logpath'

  it "rename stream" $ do
    print $ "Rename stream " <> S.showStreamName streamId <> " to " <> S.showStreamName newStreamId
    S.renameStream client streamId newStreamId
    S.doesStreamExists client streamId `shouldReturn` False
    S.doesStreamExists client newStreamId `shouldReturn` True

  it "stream replication factor" $ do
    S.getStreamReplicaFactor client newStreamId `shouldReturn` 1

  it "stream head record timestamp" $ do
    -- since there is no records in this stream
    S.getStreamHeadTimestamp client newStreamId `shouldReturn` Nothing
    logid <- S.getUnderlyingLogId client newStreamId
    _ <- S.append client logid "hello" Nothing
    let cond mv = case mv of
                    Just v  -> v > 0 && v < (maxBound :: Int64)
                    Nothing -> error "predicate failed"
    S.getStreamHeadTimestamp client newStreamId >>= (`shouldSatisfy` cond)

  it "get/set extra-attrs" $ do
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
    S.doesStreamExists client newStreamId `shouldReturn` False
