module Kafka.Protocol.Message
  ( RequestHeader
  , getRequestHeader

  , module Kafka.Protocol.Message.Struct
  ) where

import           Data.Int
import           Data.Text                      (Text)

import           Kafka.Protocol.Encoding.Parser
import           Kafka.Protocol.Message.Struct

-- TODO: Support Optional Tagged Fields
data RequestHeader = RequestHeader
  { requestApiKey        :: !Int16
  , requestApiVersion    :: !Int16
  , requestCorrelationId :: !Int32
  , requestClientId      :: !(Maybe Text)
  } deriving (Show)

getRequestHeader :: Parser RequestHeader
getRequestHeader =
      RequestHeader
  <$> getInt16
  <*> getInt16
  <*> getInt32
  <*> getNullableString
