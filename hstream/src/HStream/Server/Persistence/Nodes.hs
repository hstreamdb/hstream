{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence.Nodes (
    NodeStatus (..)
  , NodeInfo (..)

  , getNodeStatus
  , getServerHost
  , getServerPort
  , getServerInternalPort
  , getServerUri
  , getServerNode
  , setNodeStatus
  , getServerInternalAddr

  , getServerIds
  , getServerNodes
  ) where

import           Data.Aeson                        (FromJSON, ToJSON)
import           Data.Functor                      (void, (<&>))
import qualified Data.Text                         as T
import           GHC.Generics                      (Generic)
import           GHC.Stack                         (HasCallStack)
import qualified Z.Data.CBytes                     as CB
import           Z.IO.Network                      (SocketAddr, ipv4)
import           ZooKeeper                         (zooGetChildren, zooSet)
import           ZooKeeper.Types                   (StringVector (StringVector),
                                                    StringsCompletion (StringsCompletion),
                                                    ZHandle)

import           Data.Word                         (Word32)
import           HStream.Server.HStreamApi         (ServerNode (..))
import           HStream.Server.Persistence.Common ()
import           HStream.Server.Persistence.Utils  (decodeZNodeValue',
                                                    serverRootPath)
import           HStream.Server.Types              (ServerID)
import           HStream.Utils                     (textToCBytes, valueToBytes)

data NodeStatus = Starting | Ready | Working
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data NodeInfo = NodeInfo
  { nodeStatus         :: NodeStatus
  , serverHost         :: T.Text
  , serverPort         :: Word32
  , serverInternalPort :: Word32
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

getNodeStatus :: ZHandle -> ServerID -> IO NodeStatus
getNodeStatus zk sID = getNodeInfo zk sID <&> nodeStatus

getServerHost :: ZHandle -> ServerID -> IO T.Text
getServerHost zk sID = getNodeInfo zk sID <&> serverHost

getServerPort :: ZHandle -> ServerID -> IO Word32
getServerPort zk sID = getNodeInfo zk sID <&> serverPort

getServerInternalPort :: ZHandle -> ServerID -> IO Word32
getServerInternalPort zk sID = getNodeInfo zk sID <&> serverInternalPort

getServerUri :: ZHandle -> ServerID -> IO T.Text
getServerUri zk sID = do
  host <- getServerHost zk sID
  port <- getServerPort zk sID
  return $ host <> ":" <> T.pack (show port)

-- FIXME: It only supports IPv4 addresses and can throw 'InvalidArgument' exception.
getServerInternalAddr :: HasCallStack => ZHandle -> ServerID -> IO SocketAddr
getServerInternalAddr zk sID = do
  NodeInfo {..} <- getNodeInfo zk sID
  return (ipv4 (textToCBytes serverHost) (fromIntegral serverInternalPort))

setNodeStatus :: HasCallStack => ZHandle -> ServerID -> NodeStatus -> IO ()
setNodeStatus zk sID status = do
  nodeInfo <- getNodeInfo zk sID
  let nodeInfo' = nodeInfo { nodeStatus = status }
  void $ zooSet zk (serverRootPath <> "/" <> CB.pack (show sID)) (Just $ valueToBytes nodeInfo') Nothing

getNodeInfo :: ZHandle -> ServerID -> IO NodeInfo
getNodeInfo zk sID = do
  decodeZNodeValue' zk (serverRootPath <> "/" <> CB.pack (show sID))

getServerNode :: ZHandle -> ServerID -> IO ServerNode
getServerNode zk sID = do
  host <- getServerHost zk sID
  port <- getServerPort zk sID
  return $ ServerNode
           { serverNodeId   = sID
           , serverNodeHost = host
           , serverNodePort = port
           }

getServerIds :: ZHandle -> IO [ServerID]
getServerIds zk = do
  (StringsCompletion (StringVector servers))
    <- zooGetChildren zk serverRootPath
  return (read . CB.unpack <$> servers)

getServerNodes :: ZHandle -> IO [ServerNode]
getServerNodes zk = getServerIds zk >>= mapM (getServerNode zk)
