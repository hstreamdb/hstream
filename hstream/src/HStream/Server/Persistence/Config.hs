{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}
module HStream.Server.Persistence.Config where

import           Data.Aeson   (FromJSON, ToJSON)
import           GHC.Generics (Generic)

data HServerConfig = HServerConfig
  { hserverMinServers :: Int
  } deriving (Eq, Show, Generic, FromJSON, ToJSON)
