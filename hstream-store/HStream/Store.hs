{-# LANGUAGE PatternSynonyms #-}

module HStream.Store
  ( LDClient
  , LDReader
  , LDSyncCkpReader
  , LSN
  , pattern LSN_MAX
  , pattern LSN_MIN
  , pattern LSN_INVALID
  , FindKeyAccuracy (..)

  , newLDClient
  , getMaxPayloadSize
  , setClientSetting
  , setClientSettings
  , getClientSetting
  , getTailLSN
  , trim
  , findTime
  , logIdHasGroup

    -- * Stream
  , module HStream.Store.Stream

    -- * Exception
  , module HStream.Store.Exception

  ) where

import           Control.Monad                    (forM_)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           GHC.Stack                        (HasCallStack)
import           Z.Data.CBytes                    (CBytes)

import           HStream.Store.Exception
import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.LogDevice
import           HStream.Store.Internal.Types
import           HStream.Store.Stream

setClientSettings :: HasCallStack => LDClient -> Map CBytes CBytes -> IO ()
setClientSettings client settings = forM_ (Map.toList settings) $ \(k, v) -> do
  setClientSetting client k v
