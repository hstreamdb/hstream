{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence.Config where

import           Control.Exception                (SomeException, try)
import           Data.Aeson                       (FromJSON, ToJSON)
import           GHC.Generics                     (Generic)
import           System.Exit                      (exitFailure)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (zooCreate, zooGetChildren)
import           ZooKeeper.Types

import           Control.Monad                    (void, when)
import qualified HStream.Logger                   as Log
import           HStream.Server.Persistence.Utils (configPath,
                                                   decodeZNodeValue')
import           HStream.Server.Types             (ServerOpts (..))
import           HStream.Utils                    (valueToBytes)

data HServerConfig = HServerConfig
  { hserverMinServers :: Int
  } deriving (Eq, Show, Generic, FromJSON, ToJSON)

getHServerConfig :: CBytes -> ZHandle -> IO HServerConfig
getHServerConfig name zk =
  decodeZNodeValue' zk (configPath <> "/" <> name)

-- Note: A distributed lock is required when trying to insert hserver config.
-- However, the problem is encountered only when the nodes is empty, or in other
-- words, when we inserting the first config. As a consequence, we does not use a
-- standard distributed lock but take a really simple one based on 'zooCreate'.
-- It just makes sense because herd effect is not serious in the situation.
checkConfigConsistent :: ServerOpts -> ZHandle -> IO ()
checkConfigConsistent opts@ServerOpts {..} zk = do
  nodes <- unStrVec . strsCompletionValues <$> zooGetChildren zk configPath
  let serverConfig = HServerConfig { hserverMinServers = _serverMinNum }
  case nodes of
    [] -> insertFirstConfig serverConfig
    _  -> do
      HServerConfig {..} <- getHServerConfig (head nodes) zk
      when (hserverMinServers /= _serverMinNum) $ do
        Log.fatal . Log.buildString $
          "Server config min-servers is set to "
          <> show _serverMinNum <> ", which does not match "
          <> show hserverMinServers <> " in zookeeper"
        exitFailure
      insertConfig serverConfig
  where
    insertConfig serverConfig = void $
      zooCreate zk (configPath <> "/" <> _serverName) (Just $ valueToBytes serverConfig) zooOpenAclUnsafe ZooEphemeral
    insertFirstConfig serverConfig = do
      result <- try $ zooCreate zk (configPath <> "first") Nothing zooOpenAclUnsafe ZooEphemeral
      case result of
        Right _                   -> insertConfig serverConfig
        Left (_ :: SomeException) -> checkConfigConsistent opts zk
