{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence.Nodes (
    NodeInfo (..)

  , getServerHost
  , getServerPort
  , getServerInternalPort
  , getServerUri
  , getServerNode
  , getServerNode'
  , getServerInternalAddr

  , getServerNodes
  , getClusterStatus
  ) where

import           Data.Aeson                        (FromJSON, ToJSON)
import           Data.Functor                      ((<&>))
import qualified Data.HashMap.Strict               as HM
import           Data.List                         ((\\))
import qualified Data.Text                         as T
import           Data.Word                         (Word32)
import           GHC.Generics                      (Generic)
import           GHC.Stack                         (HasCallStack)
import qualified Z.Data.CBytes                     as CB
import           Z.IO.Network                      (SocketAddr, ipv4)
import           ZooKeeper                         (zooGetChildren)
import           ZooKeeper.Types                   (StringVector (StringVector),
                                                    StringsCompletion (StringsCompletion),
                                                    ZHandle)

import           HStream.Server.HStreamApi         (NodeState (..),
                                                    ServerNode (..),
                                                    ServerNodeStatus (..))
import           HStream.Server.Persistence.Common ()
import           HStream.Server.Persistence.Utils  (decodeZNodeValue',
                                                    serverRootPath)
import           HStream.Server.Types              (ServerID)
import           HStream.Utils                     (cBytesToIntegral,
                                                    integralToCBytes,
                                                    mkEnumerated, textToCBytes)

data NodeInfo = NodeInfo
  { serverHost         :: T.Text
  , serverPort         :: Word32
  , serverInternalPort :: Word32
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

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

getNodeInfo :: ZHandle -> ServerID -> IO NodeInfo
getNodeInfo zk sID = do
  decodeZNodeValue' zk (serverRootPath <> "/" <> integralToCBytes sID)

getServerNode :: ZHandle -> ServerID -> IO ServerNode
getServerNode zk sID = do
  host <- getServerHost zk sID
  port <- getServerPort zk sID
  return $ ServerNode
           { serverNodeId   = sID
           , serverNodeHost = host
           , serverNodePort = port
           }

getServerNode' :: ZHandle -> CB.CBytes -> IO ServerNode
getServerNode' zk sID = do
  NodeInfo {..} <- decodeZNodeValue' zk (serverRootPath <> "/" <> sID)
  return $ ServerNode
    { serverNodeId   = cBytesToIntegral sID
    , serverNodeHost = serverHost
    , serverNodePort = serverPort
    }

getServerIds :: ZHandle -> IO [ServerID]
getServerIds zk = do
  (StringsCompletion (StringVector servers))
    <- zooGetChildren zk serverRootPath
  return (cBytesToIntegral <$> servers)

getServerNodes :: ZHandle -> IO [ServerNode]
getServerNodes zk = getServerIds zk >>= mapM (getServerNode zk)

getClusterStatus :: ZHandle -> IO (HM.HashMap ServerID ServerNodeStatus)
getClusterStatus zk = do
  hmap <- decodeZNodeValue' zk serverRootPath
  StringsCompletion (StringVector aliveNodes) <- zooGetChildren zk serverRootPath
  return $ foldr (HM.update updateState) hmap (HM.keys hmap \\ map cBytesToIntegral aliveNodes)
  where
    updateState x = Just x {serverNodeStatusState = mkEnumerated NodeStateDead}
