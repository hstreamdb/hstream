{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.Kafka.Server.Config.KafkaConfig where

import qualified Control.Monad   as M
import qualified Data.Aeson.Key  as Y
import qualified Data.Aeson.Text as Y
import           Data.Int        (Int32)
import           Data.List       (intercalate)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text       as T
import qualified Data.Text.Lazy  as TL
import qualified Data.Text.Read  as T
import qualified Data.Vector     as V
import qualified Data.Yaml       as Y
import qualified GHC.Generics    as G

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
  value :: kc -> T.Text
  isSentitive :: kc -> Bool

  -- in current implement, all configs should be read-only
  readOnly :: kc -> Bool
  readOnly = const True

  fromText :: T.Text -> Either T.Text kc
  defaultConfig :: kc

  isDefaultValue :: kc -> Bool
  isDefaultValue = (defaultConfig == )

data KafkaConfigInstance = forall kc. KafkaConfig kc => KafkaConfigInstance kc

instance Eq KafkaConfigInstance where
  (==) (KafkaConfigInstance x) (KafkaConfigInstance y) = value x == value y

instance KafkaConfig KafkaConfigInstance where
  name (KafkaConfigInstance x) = name x
  value (KafkaConfigInstance x) = value x
  isSentitive (KafkaConfigInstance x) = isSentitive x
  fromText = fromText
  defaultConfig = defaultConfig

instance Show KafkaConfigInstance where
  show (KafkaConfigInstance kc) = T.unpack . showConfig $ kc

-------------------------------------------------------------------------------
-- Kafka Broker Config
--
-- https://kafka.apache.org/documentation/#brokerconfigs
-------------------------------------------------------------------------------

showConfig :: KafkaConfig a => a -> T.Text
showConfig c = name c <> "=" <> value c

#define SHOWCONFIG(configType) \
instance Show configType where { show = T.unpack . showConfig }

newtype AutoCreateTopicsEnable = AutoCreateTopicsEnable { _value :: Bool } deriving (Eq)
instance KafkaConfig AutoCreateTopicsEnable where
  name = const "auto.create.topics.enable"
  value (AutoCreateTopicsEnable True)  = "true"
  value (AutoCreateTopicsEnable False) = "false"
  isSentitive = const False
  fromText "true"  = Right (AutoCreateTopicsEnable True)
  fromText "false" = Right (AutoCreateTopicsEnable False)
  fromText v       = Left $ "invalid bool value:" <> v
  defaultConfig = AutoCreateTopicsEnable True
SHOWCONFIG(AutoCreateTopicsEnable)

newtype NumPartitions = NumPartitions { _value :: Int } deriving (Eq)
instance KafkaConfig NumPartitions where
  name = const "num.partitions"
  value (NumPartitions v)  = T.pack $ show v
  isSentitive = const False
  fromText t = NumPartitions <$> textToIntE t
  defaultConfig = NumPartitions 1
SHOWCONFIG(NumPartitions)

newtype DefaultReplicationFactor = DefaultReplicationFactor { _value :: Int } deriving (Eq)
instance KafkaConfig DefaultReplicationFactor where
  name = const "default.replication.factor"
  value (DefaultReplicationFactor v)  = T.pack $ show v
  isSentitive = const False
  fromText t = DefaultReplicationFactor <$> textToIntE t
  defaultConfig = DefaultReplicationFactor 1
SHOWCONFIG(DefaultReplicationFactor)

newtype OffsetsTopicReplicationFactor = OffsetsTopicReplicationFactor { _value :: Int } deriving (Eq)
instance KafkaConfig OffsetsTopicReplicationFactor where
  name = const "offsets.topic.replication.factor"
  value (OffsetsTopicReplicationFactor v)  = T.pack $ show v
  isSentitive = const False
  fromText t = OffsetsTopicReplicationFactor <$> textToIntE t
  defaultConfig = OffsetsTopicReplicationFactor 1
SHOWCONFIG(OffsetsTopicReplicationFactor)

newtype GroupInitialRebalanceDelayMs = GroupInitialRebalanceDelayMs { _value :: Int } deriving (Eq)
instance KafkaConfig GroupInitialRebalanceDelayMs where
  name = const "group.initial.rebalance.delay.ms"
  value (GroupInitialRebalanceDelayMs v)  = T.pack $ show v
  isSentitive = const False
  fromText t = GroupInitialRebalanceDelayMs <$> textToIntE t
  defaultConfig = GroupInitialRebalanceDelayMs 3000
SHOWCONFIG(GroupInitialRebalanceDelayMs)

newtype FetchMaxBytes = FetchMaxBytes { _value :: Int } deriving (Eq)
instance KafkaConfig FetchMaxBytes where
  name = const "fetch.max.bytes"
  value (FetchMaxBytes v)  = T.pack $ show v
  isSentitive = const False
  fromText t = FetchMaxBytes <$> textToIntE t
  defaultConfig = FetchMaxBytes 57671680{- 55*1024*1024 -}
SHOWCONFIG(FetchMaxBytes)

data KafkaBrokerConfigs = KafkaBrokerConfigs
  { autoCreateTopicsEnable     :: !AutoCreateTopicsEnable
  , numPartitions              :: !NumPartitions
  , defaultReplicationFactor   :: !DefaultReplicationFactor
  , offsetsTopicReplication    :: !OffsetsTopicReplicationFactor
  , groupInitialRebalanceDelay :: !GroupInitialRebalanceDelayMs
  , fetchMaxBytes              :: !FetchMaxBytes
  } deriving (Eq, G.Generic)
instance KafkaConfigs KafkaBrokerConfigs

instance Show KafkaBrokerConfigs where
  show cfgs = let props = Map.elems $ dumpConfigs cfgs
                  fmtProps' = intercalate ", " $ map show props
               in "{" <> fmtProps' <> "}"

parseBrokerConfigs :: Y.Object -> Y.Parser KafkaBrokerConfigs
parseBrokerConfigs obj =
  case mkConfigs @KafkaBrokerConfigs lk of
    Left msg -> error (T.unpack msg)
    Right v  -> pure v
  where
    lk :: Lookup
    lk configName = Y.parseMaybe (obj Y..:) (Y.fromText configName) >>= \case
        Y.String v -> Just v
        x -> Just . TL.toStrict . Y.encodeToLazyText $ x

allBrokerConfigs :: KafkaBrokerConfigs -> V.Vector KafkaConfigInstance
allBrokerConfigs = V.fromList . Map.elems . dumpConfigs

updateBrokerConfigs :: KafkaBrokerConfigs -> Map T.Text T.Text -> Either T.Text KafkaBrokerConfigs
updateBrokerConfigs = updateConfigs

mkKafkaBrokerConfigs :: Map T.Text T.Text -> KafkaBrokerConfigs
mkKafkaBrokerConfigs mp =
  case mkConfigs (mp Map.!?) of
    Left msg -> errorWithoutStackTrace (T.unpack msg)
    Right v  ->  v

-------------------------------------------------------------------------------
-- Kafka Topic Config
-------------------------------------------------------------------------------

data CleanupPolicy = CleanupPolicyDelete | CleanupPolicyCompact deriving (Eq)
instance KafkaConfig CleanupPolicy where
  name = const "cleanup.policy"
  value CleanupPolicyDelete  = "delete"
  value CleanupPolicyCompact = "compact"
  isSentitive = const False
  fromText "delete"  = Right CleanupPolicyDelete
  fromText "compact" = Right CleanupPolicyCompact
  fromText _         = Left $ "invalid cleanup.policy value"
  defaultConfig = CleanupPolicyDelete

newtype RetentionMs = RetentionMs Int32 deriving (Eq)
instance KafkaConfig RetentionMs where
  name = const "retention.ms"
  value (RetentionMs v) = T.pack $ show v
  isSentitive = const False
  fromText textVal = case (T.signed T.decimal) textVal of
                  Left msg          -> Left (T.pack msg)
                  Right (intVal, _) -> Right (RetentionMs intVal)
  defaultConfig = RetentionMs 604800000

data KafkaTopicConfigs
  = KafkaTopicConfigs
  { cleanupPolicy :: CleanupPolicy
  , retentionMs   :: RetentionMs
  } deriving (G.Generic)
instance KafkaConfigs KafkaTopicConfigs

mkKafkaTopicConfigs :: Map.Map T.Text (Maybe T.Text) -> Either T.Text KafkaTopicConfigs
mkKafkaTopicConfigs configs = mkConfigs @KafkaTopicConfigs lk
  where lk x = M.join (Map.lookup x configs)

allTopicConfigs :: ConfigMap
allTopicConfigs = dumpConfigs (defaultConfigs @KafkaTopicConfigs)

getTopicConfig :: T.Text -> Map.Map T.Text (Maybe T.Text) -> Either T.Text KafkaConfigInstance
getTopicConfig configName configValues = do
  let lk x = M.join (Map.lookup x configValues)
  computedMap <- dumpConfigs <$> mkConfigs @KafkaTopicConfigs lk
  case Map.lookup configName computedMap of
    Nothing  -> Left $ "unsupported config name:" <> configName
    Just cfg -> Right cfg

---------------------------------------------------------------------------
-- Config Helpers
---------------------------------------------------------------------------

type Lookup = T.Text -> Maybe T.Text
type ConfigMap = Map.Map T.Text KafkaConfigInstance

class KafkaConfigs a where
  mkConfigs      :: Lookup -> Either T.Text a
  dumpConfigs    :: a -> ConfigMap
  defaultConfigs :: a
  -- Update current configs. New properties will be added and existing properties will be overwrite.
  -- Unknow properties will be ignored.
  updateConfigs  :: a -> Map T.Text T.Text -> Either T.Text a

  default mkConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => Lookup -> Either T.Text a
  mkConfigs lk = G.to <$> gmkConfigs lk

  default dumpConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => a -> ConfigMap
  dumpConfigs = gdumpConfigs . G.from

  default defaultConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => a
  defaultConfigs = G.to gdefaultConfigs

  default updateConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => a -> Map T.Text T.Text -> Either T.Text a
  updateConfigs x mp = G.to <$> gupdateConfigs (G.from x) mp

class GKafkaConfigs f where
  gmkConfigs      :: Lookup -> Either T.Text (f p)
  gdumpConfigs    :: (f p) -> ConfigMap
  gdefaultConfigs :: f p
  gupdateConfigs  :: f p -> Map T.Text T.Text -> Either T.Text (f p)

instance KafkaConfig c => GKafkaConfigs (G.K1 i c) where
  gmkConfigs lk = G.K1 <$> case lk (name @c defaultConfig) of
    Nothing        -> Right (defaultConfig @c)
    Just textValue -> fromText @c textValue
  gdumpConfigs (G.K1 x) = (Map.singleton (name x) (KafkaConfigInstance x))
  gdefaultConfigs = G.K1 (defaultConfig @c)
  gupdateConfigs (G.K1 x) mp = G.K1 <$> case Map.lookup (name x) mp of
    Nothing        -> Right x
    Just textValue -> fromText @c textValue

instance GKafkaConfigs f => GKafkaConfigs (G.M1 i c f) where
  gmkConfigs lk = G.M1 <$> (gmkConfigs lk)
  gdumpConfigs (G.M1 x) = gdumpConfigs x
  gdefaultConfigs = G.M1 gdefaultConfigs
  gupdateConfigs (G.M1 x) mp = G.M1 <$> gupdateConfigs x mp

instance (GKafkaConfigs a, GKafkaConfigs b) => GKafkaConfigs (a G.:*: b) where
  gmkConfigs lk = (G.:*:) <$> (gmkConfigs lk) <*> (gmkConfigs lk)
  gdumpConfigs (x G.:*: y) = Map.union (gdumpConfigs x) (gdumpConfigs y)
  gdefaultConfigs = gdefaultConfigs G.:*: gdefaultConfigs
  gupdateConfigs (x G.:*: y) mp = (G.:*:) <$> gupdateConfigs x mp <*> gupdateConfigs y mp

-------------------------------------------------------------------------------
-- Utils
-------------------------------------------------------------------------------

textToIntE :: T.Text -> Either T.Text Int
textToIntE v =
  case (T.signed T.decimal) v of
    Left msg          -> Left (T.pack msg)
    Right (intVal, _) -> Right intVal

intToText :: Show a => a -> T.Text
intToText v = T.pack (show v)
