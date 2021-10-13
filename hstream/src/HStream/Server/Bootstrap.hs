{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
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

--------------------------------------------------------------------------------

startServer :: ZHandle -> ServerOpts -> IO () -> IO ()
startServer zk ServerOpts {..} myApp = do
  initNodePath zk _serverName (TL.pack _serverAddress) (fromIntegral _serverPort) (fromIntegral _serverInternalPort)
  setNodeStatus zk _serverName Working
  myApp
