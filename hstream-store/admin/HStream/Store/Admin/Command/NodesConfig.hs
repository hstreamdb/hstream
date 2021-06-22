module HStream.Store.Admin.Command.NodesConfig
  ( runNodesConfigCmd
  , showConfig
  , bootstrap
  , removeConfig
  ) where

import           Control.Monad             (when)
import           Data.Aeson.Encode.Pretty  (encodePretty)
import qualified Data.ByteString.Lazy      as BSL
import qualified Data.Map.Strict           as Map
import           Data.Text                 (Text)
import           Data.Text.Encoding        (decodeUtf8)
import qualified Data.Text.IO              as TIO

import qualified HStream.Store.Admin.API   as AA
import           HStream.Store.Admin.Types

runNodesConfigCmd :: AA.HeaderConfig AA.AdminAPI -> NodesConfigOpts -> IO ()
runNodesConfigCmd s (NodesConfigShow c)       = TIO.putStrLn =<< showConfig s c
runNodesConfigCmd s (NodesConfigBootstrap ps) = bootstrap s ps
runNodesConfigCmd s (NodesConfigRemove c)     = removeConfig s c

showConfig :: AA.HeaderConfig AA.AdminAPI -> SimpleNodesFilter -> IO Text
showConfig conf s = do
  config <- AA.sendAdminApiRequest conf $
    runSimpleNodesFilter s $ \case
      Nothing -> AA.getNodesConfig (AA.NodesFilter Nothing Nothing Nothing)
      Just nf -> AA.getNodesConfig nf

  let format = decodeUtf8 . BSL.toStrict . encodePretty
  case config of
    Left c   -> return $ format c
    Right [] -> return ""
    Right cs -> return $ format $
      AA.NodesConfigResponse (concatMap AA.nodesConfigResponse_nodes cs)
                             (AA.nodesConfigResponse_version $ head cs)

bootstrap :: AA.HeaderConfig AA.AdminAPI -> [ReplicationPropertyPair] -> IO ()
bootstrap conf ps = do
  let replicationProperty = Map.fromList $ map unReplicationPropertyPair ps
  when (Map.null replicationProperty) $ errorWithoutStackTrace "Empty replication property!"
  resp <- AA.sendAdminApiRequest conf $
    AA.bootstrapCluster $ AA.BootstrapClusterRequest replicationProperty
  putStrLn $ "Successfully bootstrapped the cluster, new nodes configuration version: " <> show (AA.bootstrapClusterResponse_new_nodes_configuration_version resp)

removeConfig :: AA.HeaderConfig AA.AdminAPI -> SimpleNodesFilter -> IO ()
removeConfig conf s = do
  resp <- AA.sendAdminApiRequest conf $
    AA.removeNodes $ AA.RemoveNodesRequest (fromSimpleNodesFilter s)
  putStrLn $ "Successfully removed the node, new nodes configuration version "
    <> show (AA.removeNodesResponse_new_nodes_configuration_version resp)
