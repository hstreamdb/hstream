module HStream.Server.Persistence.ClusterConfig where

import           Control.Exception
import           Control.Monad              (void, when)
import           Z.Data.CBytes              (CBytes)
import qualified Z.Data.CBytes              as CB
import           ZooKeeper                  (zooCreate, zooGetChildren)
import           ZooKeeper.Types

import qualified HStream.Logger             as Log
import           HStream.Server.Persistence
import           HStream.Server.Types       (ClusterConfig (..),
                                             LoadBalanceMode, ServerOpts (..))
import           HStream.Utils              (valueToBytes)

checkConfigConsistent :: ServerOpts -> ZHandle -> IO ()
checkConfigConsistent opts@ServerOpts {..} zk = do
  nodes <- unStrVec . strsCompletionValues <$> zooGetChildren zk configPath
  let serverConfig = ClusterConfig {loadBalanceMode = _loadBalanceMode}
  case nodes of
    [] -> insertFirstConfig serverConfig
    x:_  -> do
      ClusterConfig {..} <- getClusterConfigWithId x zk
      when (loadBalanceMode /= _loadBalanceMode) $
        Log.warning . Log.buildString $ "Inconsistent load balance mode setting, will stay" <> show loadBalanceMode
      insertConfig serverConfig
  where
    insertConfig serverConfig = void $ zooCreate
      zk
      (configPath <> "/" <> CB.pack (show _serverID))
      (Just $ valueToBytes serverConfig)
      zooOpenAclUnsafe
      ZooEphemeral
    insertFirstConfig serverConfig = do
      result <- try $ zooCreate zk
                                (configPath <> "first")
                                Nothing
                                zooOpenAclUnsafe
                                ZooEphemeral
      case result of
        Right _                    -> insertConfig serverConfig
        Left  (_ :: SomeException) -> checkConfigConsistent opts zk

getClusterConfig :: ZHandle -> IO ClusterConfig
getClusterConfig zk = do
  config:_ <- unStrVec . strsCompletionValues <$> zooGetChildren zk configPath
  getClusterConfigWithId config zk

getClusterConfigWithId :: CBytes -> ZHandle -> IO ClusterConfig
getClusterConfigWithId name zk = decodeZNodeValue' zk (configPath <> "/" <> name)

getLoadBalanceMode :: ZHandle -> IO LoadBalanceMode
getLoadBalanceMode = fmap loadBalanceMode . getClusterConfig
