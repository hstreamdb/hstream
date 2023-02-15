{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Error
  ( HStreamProcessingError (..),
  )
where

import           RIO

data HStreamProcessingError
  = TaskTopologyBuildError Text
  | UnSupportedMessageStoreError Text
  | UnSupportedStateStoreError Text
  | TypeCastError Text
  | UnExpectedStateStoreType Text
  | OperationError Text
  | ImpossibleError
  | UnknownError Text
  deriving (Show)

instance Exception HStreamProcessingError
