module Kafka.Protocol.Message
  ( RequestHeader (..)
  , ResponseHeader (..)

  , module Kafka.Protocol.Message.Struct
  ) where

import           Data.Int
import           Data.Text                     (Text)
import           GHC.Generics

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message.Struct

-- TODO: Support Optional Tagged Fields
data RequestHeader = RequestHeader
  { requestApiKey        :: !ApiKey
  , requestApiVersion    :: !Int16
  , requestCorrelationId :: !Int32
  , requestClientId      :: !(Maybe Text)
  } deriving (Show, Eq, Generic)

instance Serializable RequestHeader

newtype ResponseHeader = ResponseHeader
  { responseCorrelationId :: Int32
  } deriving (Show, Eq, Generic)

instance Serializable ResponseHeader
