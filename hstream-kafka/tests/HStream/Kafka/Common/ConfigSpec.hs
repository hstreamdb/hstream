{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Kafka.Common.ConfigSpec where

import qualified Data.Map.Strict             as M
import           HStream.Kafka.Server.Config
import           Test.Hspec

spec :: Spec
spec = describe "KafkaConfigTest" $ do

  it "updateConfigs" $ do
    let kc1 = mkKafkaBrokerConfigs $ M.fromList
         [
             ("auto.create.topics.enable", "false"),
             ("num.partitions", "2"),
             ("offsets.topic.replication.factor", "1")
         ]
    let updates = M.fromList
         [
             ("num.partitions", "3"),
             ("default.replication.factor", "2"),
             ("offsets.topic.replication.factor", "2"),
             -- unknown properties should be ignored
             ("unknown", "c")
         ]
    let expected = mkKafkaBrokerConfigs $ M.fromList
         [
             -- properties not include in updates will remain unchanged
             ("auto.create.topics.enable", "false"),
             -- properties includes in updates will be overridde
             ("num.partitions", "3"),
             ("default.replication.factor", "2"),
             ("offsets.topic.replication.factor", "2")
         ]
    let kc = updateConfigs kc1 updates
    case kc of
      Left e    -> fail $ "updateConfigs failed: " <> show e
      Right cfg -> cfg `shouldBe` expected
