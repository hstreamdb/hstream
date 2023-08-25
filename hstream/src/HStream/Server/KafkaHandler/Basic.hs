module HStream.Server.KafkaHandler.Basic
  ( handleApiversionsV0
  , handleApiversionsV1
  , handleApiversionsV2
  ) where

import qualified Data.Vector                   as V

import qualified Kafka.Protocol.Error          as K
import qualified Kafka.Protocol.Message.Struct as K
import qualified Kafka.Protocol.Service        as K

handleApiversionsV0
  :: K.RequestContext -> K.ApiVersionsRequestV0 -> IO K.ApiVersionsResponseV0
handleApiversionsV0 _ _ = do
  let apiKeys = Just $ V.fromList K.supportedApiVersions
  pure $ K.ApiVersionsResponseV0 K.NONE apiKeys

handleApiversionsV1
  :: K.RequestContext -> K.ApiVersionsRequestV1 -> IO K.ApiVersionsResponseV1
handleApiversionsV1 _ _ = do
  let apiKeys = Just $ V.fromList K.supportedApiVersions
  pure $ K.ApiVersionsResponseV1 K.NONE apiKeys 0{- throttle_time_ms -}

handleApiversionsV2
  :: K.RequestContext -> K.ApiVersionsRequestV2 -> IO K.ApiVersionsResponseV2
handleApiversionsV2 = handleApiversionsV1
