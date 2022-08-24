module DiffFlow.Error where

import           Control.Exception
import           Data.Text         (Text)

data DiffFlowError
  = BasicTypesError Text
  | BuildGraphError Text
  | RunShardError   Text
  | ImpossibleError
  | UnknownError    Text
  deriving Show

instance Exception DiffFlowError
