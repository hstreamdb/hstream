module HStream.Kafka.Common.KafkaException
  ( ErrorCodeException (..)
  ) where

import qualified Control.Exception    as E
import qualified Kafka.Protocol.Error as K

-------------------------------------------------------------------------------

newtype ErrorCodeException = ErrorCodeException K.ErrorCode deriving Show
instance E.Exception ErrorCodeException
