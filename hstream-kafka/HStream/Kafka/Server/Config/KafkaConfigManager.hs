{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternGuards         #-}

module HStream.Kafka.Server.Config.KafkaConfigManager where
import qualified Data.Aeson                              as J
import           Data.Bifunctor                          (Bifunctor (bimap))
import qualified Data.Map                                as Map
import           Data.Maybe                              (fromJust, fromMaybe)
import qualified Data.Set                                as Set
import qualified Data.Text                               as T
import qualified Data.Vector                             as V
import qualified HStream.Kafka.Common.Utils              as K
import qualified HStream.Kafka.Server.Config.KafkaConfig as KC
import           HStream.Kafka.Server.Handler.Topic      (validateTopicName)
import qualified HStream.Store                           as S
import qualified HStream.Utils                           as Utils
import qualified Kafka.Protocol                          as K
import qualified Kafka.Protocol.Error                    as K

data KafkaConfigManager
  = KafkaConfigManager
  { ldClient           :: S.LDClient
  , kafkaBrokerConfigs :: KC.KafkaBrokerConfigs
  }

mkKafkaConfigManager :: S.LDClient -> KC.KafkaBrokerConfigs -> IO KafkaConfigManager
mkKafkaConfigManager ldClient kafkaBrokerConfigs =
  return $ KafkaConfigManager {..}

listTopicConfigs :: KafkaConfigManager -> T.Text -> K.KaArray T.Text -> IO K.DescribeConfigsResult
listTopicConfigs KafkaConfigManager{..} topic keys
  | Left (code, msg) <- validateTopicName topic = return $ getErrorResponse KC.TOPIC topic code (fromJust msg)
  | otherwise = do
     let streamId = S.transToTopicStreamName topic
     S.doesStreamExist ldClient streamId >>= \case
       False -> return $ getErrorResponse KC.TOPIC topic K.UNKNOWN_TOPIC_OR_PARTITION "topic not found"
       True -> do
         configs <- S.getStreamExtraAttrs ldClient streamId
         let keys' = fromMaybe (V.fromList $ Map.keys KC.allTopicConfigs) (K.unKaArray keys)
             configs' = convertConfigs configs
         case V.mapM (getConfig configs') keys' of
           Left msg -> return $ getErrorResponse KC.TOPIC topic K.INVALID_CONFIG msg
           Right configsInResp -> return $ K.DescribeConfigsResult
                   { configs=K.NonNullKaArray configsInResp
                   , errorCode=0
                   , resourceName=topic
                   , errorMessage=Nothing
                   , resourceType=fromIntegral . fromEnum $ KC.TOPIC
                   }
  where
    convertConfigs = Map.fromList . map (bimap Utils.cBytesToText (J.decode . Utils.cBytesToLazyByteString)) . Map.toList
    getConfig :: Map.Map T.Text (Maybe T.Text) -> T.Text -> Either T.Text K.DescribeConfigsResourceResult
    getConfig configs configName = getResultFromInstance <$> KC.getTopicConfig configName configs

getErrorResponse :: KC.KafkaConfigResource
                 -> T.Text
                 -> K.ErrorCode
                 -> T.Text
                 -> K.DescribeConfigsResult
getErrorResponse rt rn code msg = K.DescribeConfigsResult
            { configs=K.NonNullKaArray V.empty
            , errorCode=code
            , resourceName=rn
            , errorMessage=Just msg
            , resourceType=fromIntegral . fromEnum $ rt
            }

getResultFromInstance :: KC.KafkaConfigInstance -> K.DescribeConfigsResourceResult
getResultFromInstance (KC.KafkaConfigInstance cfg) =
  K.DescribeConfigsResourceResult
    { isSensitive=KC.isSentitive cfg
    , isDefault=KC.isDefaultValue cfg
    , readOnly=KC.readOnly cfg
    , name=KC.name cfg
    , value= Just $ KC.value cfg
    }

listBrokerConfigs :: KafkaConfigManager -> T.Text -> K.KaArray T.Text -> IO K.DescribeConfigsResult
listBrokerConfigs KafkaConfigManager{..} brokerId keys = do
  let keySet = Set.fromList (K.kaArrayToList keys)
      filteredConfigs = if Set.null keySet
        then KC.allBrokerConfigs kafkaBrokerConfigs
        else V.filter filterF $ KC.allBrokerConfigs kafkaBrokerConfigs
      filterF (KC.KafkaConfigInstance cfg) = Set.member (KC.name cfg) keySet
      configs = V.map getResultFromInstance filteredConfigs
  return $ K.DescribeConfigsResult
                { configs=K.NonNullKaArray configs
                , errorCode=0
                , resourceName=brokerId
                , errorMessage=Nothing
                , resourceType=fromIntegral . fromEnum $ KC.BROKER
                }
