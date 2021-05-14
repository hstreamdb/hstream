{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.TopicSpec (spec) where

import qualified Data.Map.Strict         as Map
import           Test.Hspec

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

newRandomTopic :: IO (S.Topic, S.Topic)
newRandomTopic = do
  topic <- ("ci/stream/" <>) <$> newRandomName 5
  newTopic <- ("ci/stream/" <>) <$> newRandomName 5
  return (topic, newTopic)

spec :: Spec
spec = describe "HStream.Store.Topic" $ do
  (topic, newTopic) <- runIO newRandomTopic

  it "create topic" $ do
    print $ "Create a new topic: " <> topic
    S.doesTopicExists client topic `shouldReturn` False
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createTopic client topic attrs
    S.doesTopicExists client topic `shouldReturn` True

  it "rename topic" $ do
    print $ "Rename topic " <> topic <> " to " <> newTopic
    S.renameTopic client topic newTopic
    S.doesTopicExists client topic `shouldReturn` False
    S.doesTopicExists client newTopic `shouldReturn` True

  it "get/set extra-attrs" $ do
    logGroup <- S.getLogGroup client newTopic
    S.logGroupGetExtraAttr logGroup "greet" `shouldReturn` "hi"
    S.logGroupUpdateExtraAttrs client logGroup $ Map.fromList [("greet", "hello"), ("Alice", "Bob")]
    logGroup_ <- S.getLogGroup client newTopic
    S.logGroupGetExtraAttr logGroup_ "greet" `shouldReturn` "hello"
    S.logGroupGetExtraAttr logGroup_ "A" `shouldReturn` "B"
    S.logGroupGetExtraAttr logGroup_ "Alice" `shouldReturn` "Bob"

  it "remove topic" $ do
    S.removeTopic client newTopic
    S.doesTopicExists client newTopic `shouldReturn` False
