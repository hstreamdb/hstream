{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.TopicSpec (spec) where

import qualified Data.Map.Strict         as Map
import           Test.Hspec

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

newRandomTopic :: IO (S.StreamName, S.StreamName)
newRandomTopic = do
  topic <- ("/ci/stream/" <>) <$> newRandomName 5
  newTopic <- ("/ci/stream/" <>) <$> newRandomName 5
  return (topic, newTopic)

spec :: Spec
spec = describe "HStream.Store.Topic" $ do
  (topic, newTopic) <- runIO newRandomTopic

  it "create topic" $ do
    print $ "Create a new topic: " <> topic
    S.doesStreamExists client topic `shouldReturn` False
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createStream client topic attrs
    S.doesStreamExists client topic `shouldReturn` True

  it "get loggroup by name or id shoule be equal" $ do
    name <- S.logGroupGetFullyQualifiedName =<< S.getLogGroup client topic
    logid <- S.getCLogIDByStreamName client name
    name' <- S.logGroupGetFullyQualifiedName =<< S.getLogGroupByID client logid
    name `shouldBe` name'

  it "rename topic" $ do
    print $ "Rename topic " <> topic <> " to " <> newTopic
    S.renameStream client topic newTopic
    S.doesStreamExists client topic `shouldReturn` False
    S.doesStreamExists client newTopic `shouldReturn` True

  it "get/set extra-attrs" $ do
    logGroup <- S.getLogGroup client newTopic
    S.logGroupGetExtraAttr logGroup "greet" `shouldReturn` "hi"
    S.logGroupUpdateExtraAttrs client logGroup $ Map.fromList [("greet", "hello"), ("Alice", "Bob")]

    logGroup_ <- S.getLogGroup client newTopic
    S.logGroupGetExtraAttr logGroup_ "greet" `shouldReturn` "hello"
    S.logGroupGetExtraAttr logGroup_ "A" `shouldReturn` "B"
    S.logGroupGetExtraAttr logGroup_ "Alice" `shouldReturn` "Bob"

  it "remove topic" $ do
    S.removeStream client newTopic
    S.doesStreamExists client newTopic `shouldReturn` False
