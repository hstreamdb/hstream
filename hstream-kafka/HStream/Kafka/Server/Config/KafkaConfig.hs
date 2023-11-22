{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.Kafka.Server.Config.KafkaConfig where
import qualified Control.Monad  as M
import           Data.Int       (Int32)
import qualified Data.Map       as Map
import qualified Data.Text      as T
import qualified Data.Text.Read as T
import qualified Data.Vector    as V

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
  fromEnum TOPIC   = 2
  fromEnum BROKER  = 4

class Eq kc => KafkaConfig kc where
  name :: kc -> T.Text
  value :: kc -> Maybe T.Text
  isSentitive :: kc -> Bool

  -- in current implement, all configs should be read-only
  readOnly :: kc -> Bool
  readOnly = const True

  fromText :: T.Text -> Either T.Text kc
  defaultConfig :: kc

  isDefaultValue :: kc -> Bool
  isDefaultValue = (defaultConfig == )

data KafkaConfigInstance = forall kc. KafkaConfig kc => KafkaConfigInstance kc

---------------------------------------------------------------------------
-- Kafka Topic Config
---------------------------------------------------------------------------
data CleanupPolicy = CleanupPolicyDelete | CleanupPolicyCompact deriving (Eq)
instance KafkaConfig CleanupPolicy where
  name = const "cleanup.policy"
  value CleanupPolicyDelete  = Just "delete"
  value CleanupPolicyCompact = Just "compact"
  isSentitive = const False
  fromText "delete"  = Right CleanupPolicyDelete
  fromText "compact" = Right CleanupPolicyCompact
  fromText _         = Left $ "invalid cleanup.policy value"
  defaultConfig = CleanupPolicyDelete

newtype RetentionMs = RetentionMs Int32 deriving (Eq)
instance KafkaConfig RetentionMs where
  name = const "retention.ms"
  value (RetentionMs v) = Just . T.pack $ show v
  isSentitive = const False
  fromText textVal = case (T.signed T.decimal) textVal of
                  Left msg          -> Left (T.pack msg)
                  Right (intVal, _) -> Right (RetentionMs intVal)
  defaultConfig = RetentionMs 604800000

data KafkaTopicConfigs
  = KafkaTopicConfigs
  { cleanupPolicy :: CleanupPolicy
  , retentionMs   :: RetentionMs
  }

-- TODO: build from KafkaTopicConfigs
mkKafkaTopicConfigs :: Map.Map T.Text (Maybe T.Text) -> Either T.Text KafkaTopicConfigs
mkKafkaTopicConfigs configs =
  KafkaTopicConfigs
    <$> lookupConfig (defaultConfig @CleanupPolicy) configs
    <*> lookupConfig (defaultConfig @RetentionMs) configs

---------------------------------------------------------------------------
-- Kafka Broker Config
---------------------------------------------------------------------------
newtype AutoCreateTopicsEnable = AutoCreateTopicsEnable { _value :: Bool } deriving (Eq, Show)
instance KafkaConfig AutoCreateTopicsEnable where
  name = const "auto.create.topics.enable"
  value (AutoCreateTopicsEnable True)  = Just "true"
  value (AutoCreateTopicsEnable False) = Just "false"
  isSentitive = const False
  fromText "true"  = Right (AutoCreateTopicsEnable True)
  fromText "false" = Right (AutoCreateTopicsEnable False)
  fromText v       = Left $ "invalid bool value:" <> v
  defaultConfig = AutoCreateTopicsEnable True

newtype KafkaBrokerConfigs
  = KafkaBrokerConfigs
  { autoCreateTopicsEnable :: AutoCreateTopicsEnable
  } deriving (Show, Eq)

-- TODO: generate from KafkaBrokerConfigs
allBrokerConfigs :: KafkaBrokerConfigs -> V.Vector KafkaConfigInstance
allBrokerConfigs KafkaBrokerConfigs{..} = V.fromList
  [ KafkaConfigInstance $ autoCreateTopicsEnable
  ]

---------------------------------------------------------------------------
-- Config Helpers
---------------------------------------------------------------------------
#define MK_CONFIG_PAIR(configType) \
  let dc = defaultConfig @configType in (name dc, (KafkaConfigInstance dc, fmap KafkaConfigInstance . fromText @configType))

lookupConfig :: KafkaConfig kc => kc -> Map.Map T.Text (Maybe T.Text) -> Either T.Text kc
lookupConfig dc configs =
  case M.join . Map.lookup (name dc) $ configs of
    Nothing        -> Right dc
    Just textValue -> fromText textValue

-- TODO: build from KafkaTopicConfigs
allTopicConfigs :: Map.Map T.Text (KafkaConfigInstance, T.Text -> Either T.Text KafkaConfigInstance)
allTopicConfigs = Map.fromList
  [ MK_CONFIG_PAIR(CleanupPolicy)
  , MK_CONFIG_PAIR(RetentionMs)
  ]

getTopicConfig :: T.Text -> Maybe T.Text -> Either T.Text KafkaConfigInstance
getTopicConfig configName configValue =
  case Map.lookup configName allTopicConfigs of
    Nothing -> Left $ "unsupported config name:" <> configName
    Just (dc, fromText') ->
      case configValue of
        Nothing        -> Right dc
        Just textValue -> fromText' textValue
