{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.TopicSpec (spec) where

import           System.IO.Unsafe        (unsafePerformIO)
import           System.Random           (newStdGen, randomRs)
import           Test.Hspec
import           Z.Data.CBytes           (CBytes, pack)

import qualified HStream.Store.Exception as E
import qualified HStream.Store.Stream    as S

client :: S.StreamClient
client = unsafePerformIO $ S.newStreamClient "/data/store/logdevice.conf"
{-# NOINLINE client #-}

spec :: Spec
spec = describe "HStream.Store.Topic" $ do
  simpleSpec

simpleSpec :: Spec
simpleSpec = context "Simple Create & Delete" $ do
  it "create & delete topic directory" $ do
    let attrs = S.TopicAttrs { S.replicationFactor = 0 }
    topicDirName <- newRandomName 5
    let topicDir = "ci/" <> topicDirName
    dir <- S.makeTopicDirectorySync client topicDir attrs True
    S.syncTopicConfigVersion client =<< S.topicDirectoryGetVersion dir

    dir' <- S.getTopicDirectorySync client topicDir
    S.topicDirectoryGetName dir' `shouldReturn` topicDirName

    version <- S.removeTopicDirectorySync' client topicDir True
    S.syncTopicConfigVersion client version
    S.getTopicDirectorySync client topicDir `shouldThrow` notFoundException

  it "create & delete topic group sync" $ do
    let attrs = S.TopicAttrs { S.replicationFactor = 2 }
    topicGroupName <- newRandomName 5
    let topicGroup = "ci/stream/tmp/" <> topicGroupName
    let start = S.mkTopicID 3000
        end   = S.mkTopicID 3000
    group <- S.makeTopicGroupSync client topicGroup start end attrs True
    S.syncTopicConfigVersion client =<< S.topicGroupGetVersion group

    group' <- S.getTopicGroupSync client topicGroup
    S.topicGroupGetRange group' `shouldReturn` (start, end)
    S.topicGroupGetName group' `shouldReturn` topicGroupName

    version <- S.removeTopicGroupSync' client topicGroup
    S.syncTopicConfigVersion client version
    S.getTopicGroupSync client topicGroup `shouldThrow` notFoundException

  it "rename and remove asyc" $ do
    let attrs = S.TopicAttrs { S.replicationFactor = 2 }
    topicGroupName <- newRandomName 5
    let topicGroup = "ci/stream/tmp/" <> topicGroupName
    let start = S.mkTopicID 3000
        end   = S.mkTopicID 3000
    group <- S.makeTopicGroupSync client topicGroup start end attrs True
    S.syncTopicConfigVersion client =<< S.topicGroupGetVersion group

    group' <- S.getTopicGroupSync client topicGroup
    S.topicGroupGetRange group' `shouldReturn` (start, end)
    S.topicGroupGetName group' `shouldReturn` topicGroupName

    topicGroupName' <- newRandomName 5
    let topicGroup' = "ci/stream/tmp/" <> topicGroupName'
    version <- S.renameTopicGroup client topicGroup topicGroup'
    S.syncTopicConfigVersion client version
    group'' <- S.getTopicGroupSync client topicGroup'
    S.topicGroupGetRange group'' `shouldReturn` (start, end)
    S.topicGroupGetName group'' `shouldReturn` topicGroupName'
    S.getTopicGroupSync client topicGroup `shouldThrow` notFoundException

    version' <- S.removeTopicGroup client topicGroup'
    -- version' <- S.removeTopicGroupSync' client topicGroup'
    S.syncTopicConfigVersion client version'
    S.getTopicGroupSync client topicGroup' `shouldThrow` notFoundException


notFoundException :: Selector E.NOTFOUND
notFoundException = const True

newRandomName :: Int -> IO CBytes
newRandomName n = pack . take n . randomRs ('a', 'z') <$> newStdGen
