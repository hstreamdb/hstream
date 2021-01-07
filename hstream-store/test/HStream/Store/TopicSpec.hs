{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.TopicSpec (spec) where

import           Control.Concurrent      (threadDelay)
import           System.IO.Unsafe        (unsafePerformIO)
import           Test.Hspec
import qualified Z.Data.CBytes           as ZC

import qualified HStream.Store           as S
import qualified HStream.Store.Exception as E

spec :: Spec
spec = describe "HStream.Store.Topic" $ do
  simpleSpec

client :: S.StreamClient
client = unsafePerformIO $ S.newStreamClient "/data/store/logdevice.conf"
{-# NOINLINE client #-}

simpleSpec :: Spec
simpleSpec = context "Simple Create & Delete" $ do
  it "create & delete topic directory" $ do
    attrs <- S.newTopicAttributes
    let topicDirName = "stream"
    td <- S.makeTopicDirectory client  ("org/" <> topicDirName) attrs True
    S.topicDirectoryGetName td `shouldReturn` topicDirName

  it "create & delete topic group sync" $ do
    attrs <- S.newTopicAttributes
    S.setTopicReplicationFactor attrs 3
    let topicGroupName = "some-topic"
    let topicGroup = "org/stream/" <> topicGroupName
    let start = S.mkTopicID 1000
        end   = S.mkTopicID 1000
    group <- S.makeTopicGroupSync client topicGroup start end attrs True
    -- Since even after makeTopicGroupSync returns success, it may take
    -- some time for the update to propagate to all servers, here we wait
    -- 1 seconds to wait this removing really done.
    threadDelay 1000000

    S.topicGroupGetRange group `shouldReturn` (S.mkTopicID 1000, S.mkTopicID 1000)
    S.topicGroupGetName group `shouldReturn` topicGroupName

    S.removeTopicGroupSync client topicGroup
    -- The reason is the same as makeTopicGroupSync
    threadDelay 1000000

    S.getTopicGroupSync client topicGroup `shouldThrow` notFoundException

notFoundException :: Selector E.NOTFOUND
notFoundException = const True
