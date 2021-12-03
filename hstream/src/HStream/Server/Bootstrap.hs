{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Bootstrap
  ( startServer
  ) where

import qualified Data.Text                                as T
import           ZooKeeper.Types


import           HStream.Server.Initialization            (initNodePath)
import           HStream.Server.Persistence
import           HStream.Server.Persistence.ClusterConfig (checkConfigConsistent)
import           HStream.Server.Types                     (ServerOpts (..))

--------------------------------------------------------------------------------

startServer :: ZHandle -> ServerOpts -> IO () -> IO ()
startServer zk opts@ServerOpts {..} myApp = do
  initNodePath zk _serverID (T.pack _serverAddress) (fromIntegral _serverPort) (fromIntegral _serverInternalPort)
  checkConfigConsistent opts zk
  setNodeStatus zk _serverID Working
  myApp
