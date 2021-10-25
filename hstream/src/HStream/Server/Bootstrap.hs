{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Bootstrap
  ( startServer
  ) where

import qualified Data.Text.Lazy                as TL
import           ZooKeeper.Types

import           HStream.Server.Initialization (initNodePath)
import           HStream.Server.Persistence
import           HStream.Server.Types          (ServerOpts (..))

startServer :: ZHandle -> ServerOpts -> IO () -> IO ()
startServer zk ServerOpts {..} myApp = do
  let serverUri         = TL.pack $ _serverAddress <> ":" <> show _serverPort
      serverInternalUri = TL.pack $ _serverAddress <> ":" <> show _serverInternalPort
  initNodePath zk _serverName serverUri serverInternalUri
  setNodeStatus zk _serverName Working
  myApp
