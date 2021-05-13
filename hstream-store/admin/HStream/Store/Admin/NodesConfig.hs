module HStream.Store.Admin.NodesConfig
  ( showConfig
  , bootstrap
  ) where

import           Control.Monad             (when)
import           Data.Aeson.Encode.Pretty  (encodePretty)
import qualified Data.ByteString.Lazy      as BSL
import qualified Data.Map.Strict           as Map
import           Data.Text                 (Text)
import           Data.Text.Encoding        (decodeUtf8)

import qualified HStream.Store.Admin.API   as AA
import           HStream.Store.Admin.Types

showConfig :: AA.SocketConfig AA.AdminAPI -> SimpleNodesFilter -> IO Text
showConfig socketConfig s = do
  config <- AA.withSocketChannel @AA.AdminAPI socketConfig $
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

bootstrap :: AA.SocketConfig AA.AdminAPI -> [ReplicationPropertyPair] -> IO ()
bootstrap socketConfig ps = do
  let replicationProperty = Map.fromList $ map unReplicationPropertyPair ps
  when (Map.null replicationProperty) $ errorWithoutStackTrace "Empty replication property!"
  resp <- AA.withSocketChannel @AA.AdminAPI socketConfig $
    AA.bootstrapCluster $ AA.BootstrapClusterRequest replicationProperty
  putStrLn $ "Successfully bootstrapped the cluster, new nodes configuration version: " <> show (AA.bootstrapClusterResponse_new_nodes_configuration_version resp)
