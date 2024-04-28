{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Kafka.Common.ConfigSpec where

import qualified Data.Map.Strict             as M
import           HStream.Kafka.Server.Config
import           Test.Hspec

spec :: Spec
spec = describe "KafkaConfigTest" $ do

  it "mergeConfigs" $ do
    let kc1 = mkKafkaBrokerConfigs $ M.fromList
         [
             ("auto.create.topics.enable", "false"),
             ("num.partitions", "2"),
             ("offsets.topic.replication.factor", "1")
         ]
    let kc2 = mkKafkaBrokerConfigs $ M.fromList
         [
             ("num.partitions", "3"),
             ("default.replication.factor", "2"),
             ("offsets.topic.replication.factor", "2")
         ]
    let expected = mkKafkaBrokerConfigs $ M.fromList
         [
             -- values not set by kc2 will be set by kc1
             ("auto.create.topics.enable", "false"),
             -- values set by kc2 will overrid values set by kc1
             ("num.partitions", "3"),
             ("default.replication.factor", "2"),
             ("offsets.topic.replication.factor", "2")
         ]
    let kc = mergeBrokerConfigs kc1 kc2
    kc `shouldBe` expected
