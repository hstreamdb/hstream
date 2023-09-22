module HStream.Common.Server.Types
  ( Timestamp
  , ServerId
  ) where

import           Data.Int
import           Data.Word

-------------------------------------------------------------------------------

type Timestamp = Int64  -- ms

type ServerId = Word32
