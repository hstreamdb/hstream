{-# LANGUAGE DuplicateRecordFields #-}

module HStream.Kafka.Server.Config.KafkaConfig where
import qualified Data.Text as T

data KafkaConfigResource
  = UNKNOWN
  | TOPIC
  | BROKER
  deriving (Show, Eq)

instance Enum KafkaConfigResource where
  toEnum 0 = UNKNOWN
  toEnum 2 = TOPIC
  toEnum 4 = BROKER
  toEnum _ = UNKNOWN

  fromEnum UNKNOWN = 0
  fromEnum TOPIC = 2
  fromEnum BROKER = 4

-- class KafkaConfig cfg where
--   configName :: cfg -> T.Text
--   configValue :: cfg -> Maybe T.Text

data KafkaConfigDef
  = KafkaConfigDef
  { name :: T.Text
  , defaultValue :: Maybe T.Text
  , isSentitive :: Bool
  }

data KafkaTopicConfig
  = KafkaTopicConfig
  {
  }
