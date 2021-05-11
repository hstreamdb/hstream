{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.TopicSpec (spec) where

import qualified Data.Map.Strict  as Map
import           System.IO.Unsafe (unsafePerformIO)
import           System.Random    (newStdGen, randomRs)
import           Test.Hspec
import           Z.Data.CBytes    (CBytes)
import qualified Z.Data.CBytes    as CBytes

import qualified HStream.Store    as S

client :: S.LDClient
client = unsafePerformIO $ S.newLDClient "/data/store/logdevice.conf"
{-# NOINLINE client #-}

topic :: S.Topic
topic = unsafePerformIO $ ("ci/stream/" <>) <$> newRandomName 5
{-# NOINLINE topic #-}

newTopic :: S.Topic
newTopic = unsafePerformIO $ ("ci/stream/" <>) <$> newRandomName 5
{-# NOINLINE newTopic #-}

spec :: Spec
spec = describe "HStream.Store.Topic" $ do

  it ("create topic: " <> show (CBytes.toText topic)) $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [ ("greet", "hi")
                                                                         , ("A", "B")
                                                                         ]
                                        }
    S.createTopic client topic attrs
    S.doesTopicExists client topic `shouldReturn` True

  it ("rename topic to " <> show (CBytes.toText newTopic)) $ do
    S.renameTopic client topic newTopic
    S.doesTopicExists client topic `shouldReturn` False
    S.doesTopicExists client newTopic `shouldReturn` True

  it "get/set extra attrs" $ do
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

newRandomName :: Int -> IO CBytes
newRandomName n = CBytes.pack . take n . randomRs ('a', 'z') <$> newStdGen
