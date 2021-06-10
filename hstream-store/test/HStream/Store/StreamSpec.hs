{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.StreamSpec (spec) where

import qualified Data.Map.Strict         as Map
import           Test.Hspec

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = do
  base

base :: Spec
base = describe "HStream.Store.Stream" $ do
  streamName <- S.mkStreamName <$> runIO (newRandomName 5)
  logPath <- runIO $ S.streamNameToLogPath streamName
  newStreamName <- S.mkStreamName <$> runIO (newRandomName 5)
  newLogPath <- runIO $ S.streamNameToLogPath newStreamName

  it "create stream" $ do
    print $ "Create a new stream: " <> streamName
    S.doesStreamExists client streamName `shouldReturn` False
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createStream client streamName attrs
    S.doesStreamExists client streamName `shouldReturn` True

  it "get full path of loggroup by name or id shoule be equal" $ do
    logpath <- S.logGroupGetFullName =<< S.getLogGroup client logPath
    logid <- S.getCLogIDByStreamName client streamName
    logpath' <- S.logGroupGetFullName =<< S.getLogGroupByID client logid
    logpath `shouldBe` logpath'

  it "rename stream" $ do
    print $ "Rename stream " <> streamName <> " to " <> newStreamName
    S.renameStream client streamName newStreamName
    S.doesStreamExists client streamName `shouldReturn` False
    S.doesStreamExists client newStreamName `shouldReturn` True

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

  it "remove stream" $ do
    S.removeStream client newStreamName
    S.doesStreamExists client newStreamName `shouldReturn` False
