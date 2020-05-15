module HStream.LogStore.Exception where

import           Control.Exception (Exception)
import           Type.Reflection   (Typeable)

data LogStoreException
  = LogStoreIOException String
  | LogStoreLogNotFoundException String
  | LogStoreLogAlreadyExistsException String
  | LogStoreUnsupportedOperationException String
  | LogStoreDecodeException String
  | LogStoreUnknownException String
  deriving (Show, Typeable)

instance Exception LogStoreException
