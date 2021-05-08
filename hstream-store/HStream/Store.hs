{-# LANGUAGE PatternSynonyms #-}

module HStream.Store
  ( LDClient
  , LSN
  , pattern LSN_MAX
  , pattern LSN_MIN
  , pattern LSN_INVALID

  , newLDClient
  , getMaxPayloadSize
  , setClientSettings
  , getClientSettings
  , getTailLSN

    -- * Stream
  , module HStream.Store.Stream

    -- * Logger
  , module HStream.Store.Logger

    -- * Exception
  , module HStream.Store.Exception
  ) where

import           HStream.Store.Exception
import           HStream.Store.Internal.LogDevice
import           HStream.Store.Internal.Types
import           HStream.Store.Logger
import           HStream.Store.Stream
