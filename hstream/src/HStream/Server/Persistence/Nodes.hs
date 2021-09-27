{-# LANGUAGE OverloadedStrings #-}
module HStream.Server.Persistence.Nodes where

import qualified Data.Aeson                       as Aeson
import           Data.Functor                     (void, (<&>))
import           GHC.Stack                        (HasCallStack)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (zooGet, zooSet)
import           ZooKeeper.Types                  (DataCompletion (DataCompletion),
                                                   ZHandle)

import           HStream.Server.Persistence.Utils
import           HStream.Server.Types             (NodeInfo (..), NodeStatus)
import           HStream.Utils                    (bytesToLazyByteString,
                                                   valueToBytes)

getNodeStatus :: HasCallStack => ZHandle -> CBytes -> IO NodeStatus
getNodeStatus zk name = getNodeInfo zk name <&> nodeStatus

setNodeStatus :: HasCallStack => ZHandle -> CBytes -> NodeStatus -> IO ()
setNodeStatus zk name status = do
  nodeInfo <- getNodeInfo zk name
  let nodeInfo' = nodeInfo { nodeStatus = status }
  void $ zooSet zk (serverRootPath <> "/" <> name) (Just $ valueToBytes nodeInfo') Nothing

getNodeInfo :: HasCallStack => ZHandle -> CBytes -> IO NodeInfo
getNodeInfo zk name = do
  (DataCompletion val _ ) <- zooGet zk (serverRootPath <> "/" <> name)
  case Aeson.decode' . bytesToLazyByteString =<< val of
    Just info -> return info
    Nothing   ->
      error "Failed to get node status, no status found or data corrupted"
