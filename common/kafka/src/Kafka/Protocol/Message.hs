module Kafka.Protocol.Message
  ( RequestHeader
  ) where

import           Data.Int
import           Data.Text               (Text)
import           GHC.Generics

import           Kafka.Protocol.Encoding

-- TODO: Support Optional Tagged Fields
data RequestHeader = RequestHeader
  { requestApiKey        :: !Int16
  , requestApiVersion    :: !Int16
  , requestCorrelationId :: !Int32
  , requestClientId      :: !(Maybe Text)
  } deriving (Show, Eq, Generic)

instance Serializable RequestHeader
