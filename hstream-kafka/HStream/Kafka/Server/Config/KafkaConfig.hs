{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.Kafka.Server.Config.KafkaConfig where
import qualified Control.Monad   as M
import qualified Data.Aeson.Key  as Y
import qualified Data.Aeson.Text as Y
import           Data.Int        (Int32)
import qualified Data.Map        as Map
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
instance Eq KafkaConfigInstance where
  (==) (KafkaConfigInstance x) (KafkaConfigInstance y) = value x == value y
instance KafkaConfig KafkaConfigInstance where
  name (KafkaConfigInstance x) = name x
  value (KafkaConfigInstance x) = value x
  isSentitive (KafkaConfigInstance x) = isSentitive x
  fromText = fromText
  defaultConfig = defaultConfig

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
  } deriving (G.Generic)
instance KafkaConfigs KafkaTopicConfigs

mkKafkaTopicConfigs :: Map.Map T.Text (Maybe T.Text) -> Either T.Text KafkaTopicConfigs
mkKafkaTopicConfigs configs = mkConfigs @KafkaTopicConfigs lk
  where lk x = M.join (Map.lookup x configs)

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

newtype NumPartitions = NumPartitions { _value :: Int } deriving (Eq, Show)
instance KafkaConfig NumPartitions where
  name = const "num.partitions"
  value (NumPartitions v)  = Just . T.pack $ show v
  isSentitive = const False
  fromText t = NumPartitions <$> textToIntE t
  defaultConfig = NumPartitions 1

newtype DefaultReplicationFactor = DefaultReplicationFactor { _value :: Int } deriving (Eq, Show)
instance KafkaConfig DefaultReplicationFactor where
  name = const "default.replication.factor"
  value (DefaultReplicationFactor v)  = Just . T.pack $ show v
  isSentitive = const False
  fromText t = DefaultReplicationFactor <$> textToIntE t
  defaultConfig = DefaultReplicationFactor 1

data KafkaBrokerConfigs
  = KafkaBrokerConfigs
  { autoCreateTopicsEnable   :: AutoCreateTopicsEnable
  , numPartitions            :: NumPartitions
  , defaultReplicationFactor :: DefaultReplicationFactor
  } deriving (Show, Eq, G.Generic)
instance KafkaConfigs KafkaBrokerConfigs

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

mergeBrokerConfigs :: KafkaBrokerConfigs -> KafkaBrokerConfigs -> KafkaBrokerConfigs
mergeBrokerConfigs = mergeConfigs

---------------------------------------------------------------------------
-- Config Helpers
---------------------------------------------------------------------------
type Lookup = T.Text -> Maybe T.Text
type ConfigMap = Map.Map T.Text KafkaConfigInstance

class KafkaConfigs a where
  mkConfigs :: Lookup -> Either T.Text a
  dumpConfigs :: a -> ConfigMap
  defaultConfigs :: a
  mergeConfigs :: a -> a -> a

  default mkConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => Lookup -> Either T.Text a
  mkConfigs lk = G.to <$> gmkConfigs lk

  default dumpConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => a -> ConfigMap
  dumpConfigs = gdumpConfigs . G.from

  default defaultConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => a
  defaultConfigs = G.to gdefaultConfigs

  default mergeConfigs :: (G.Generic a, GKafkaConfigs (G.Rep a)) => a -> a -> a
  mergeConfigs x y = G.to (gmergeConfigs (G.from x) (G.from y))

class GKafkaConfigs f where
  gmkConfigs :: Lookup -> Either T.Text (f p)
  gdumpConfigs :: (f p) -> ConfigMap
  gdefaultConfigs :: f p
  gmergeConfigs :: f p -> f p -> f p

instance KafkaConfig c => GKafkaConfigs (G.K1 i c) where
  gmkConfigs lk = G.K1 <$> case lk (name @c defaultConfig) of
    Nothing        -> Right (defaultConfig @c)
    Just textValue -> fromText @c textValue
  gdumpConfigs (G.K1 x) = (Map.singleton (name x) (KafkaConfigInstance x))
  gdefaultConfigs = G.K1 (defaultConfig @c)
  gmergeConfigs (G.K1 x) (G.K1 y) = G.K1 (if isDefaultValue x then y else x)

instance GKafkaConfigs f => GKafkaConfigs (G.M1 i c f) where
  gmkConfigs lk = G.M1 <$> (gmkConfigs lk)
  gdumpConfigs (G.M1 x) = gdumpConfigs x
  gdefaultConfigs = G.M1 gdefaultConfigs
  gmergeConfigs (G.M1 x) (G.M1 y) = G.M1 (gmergeConfigs x y)

instance (GKafkaConfigs a, GKafkaConfigs b) => GKafkaConfigs (a G.:*: b) where
  gmkConfigs lk = (G.:*:) <$> (gmkConfigs lk) <*> (gmkConfigs lk)
  gdumpConfigs (x G.:*: y) = Map.union (gdumpConfigs x) (gdumpConfigs y)
  gdefaultConfigs = gdefaultConfigs G.:*: gdefaultConfigs
  gmergeConfigs (x1 G.:*: y1) (x2 G.:*: y2) = (gmergeConfigs x1 x2) G.:*: (gmergeConfigs y1 y2)

#define MK_CONFIG_PAIR(configType) \
  let dc = defaultConfig @configType in (name dc, (KafkaConfigInstance dc, fmap KafkaConfigInstance . fromText @configType))

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
-- Utils
---------------------------------------------------------------------------
textToIntE :: T.Text -> Either T.Text Int
textToIntE v =
  case (T.signed T.decimal) v of
    Left msg          -> Left (T.pack msg)
    Right (intVal, _) -> Right intVal

intToText :: Show a => a -> T.Text
intToText v = T.pack (show v)
