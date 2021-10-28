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

  , getReadyServers
  , getServerInternalAddr
  ) where

import           Control.Exception                 (SomeException, try)
import           Control.Monad                     (forM)
import           Data.Aeson                        (FromJSON, ToJSON)
import           Data.Functor                      (void, (<&>))
import qualified Data.Text.Lazy                    as TL
import           GHC.Generics                      (Generic)
import           GHC.Stack                         (HasCallStack)
import           Z.Data.CBytes                     (CBytes)
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
import           HStream.Utils                     (lazyTextToCBytes,
                                                    valueToBytes)

data NodeStatus = Starting | Ready | Working
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data NodeInfo = NodeInfo
  { nodeStatus         :: NodeStatus
  , serverHost         :: TL.Text
  , serverPort         :: Word32
  , serverInternalPort :: Word32
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

getNodeStatus :: ZHandle -> CBytes -> IO NodeStatus
getNodeStatus zk name = getNodeInfo zk name <&> nodeStatus

getServerHost :: ZHandle -> CBytes -> IO TL.Text
getServerHost zk name = getNodeInfo zk name <&> serverHost

getServerPort :: ZHandle -> CBytes -> IO Word32
getServerPort zk name = getNodeInfo zk name <&> serverPort

getServerInternalPort :: ZHandle -> CBytes -> IO Word32
getServerInternalPort zk name = getNodeInfo zk name <&> serverInternalPort

getServerUri :: ZHandle -> CBytes -> IO TL.Text
getServerUri zk name = do
  host <- getServerHost zk name
  port <- getServerPort zk name
  return $ host <> ":" <> TL.pack (show port)

getServerInternalAddr :: ZHandle -> CBytes -> IO SocketAddr
getServerInternalAddr zk name = do
  NodeInfo {..} <- getNodeInfo zk name
  return (ipv4 (lazyTextToCBytes serverHost) (fromIntegral serverInternalPort))

setNodeStatus :: HasCallStack => ZHandle -> CBytes -> NodeStatus -> IO ()
setNodeStatus zk name status = do
  nodeInfo <- getNodeInfo zk name
  let nodeInfo' = nodeInfo { nodeStatus = status }
  void $ zooSet zk (serverRootPath <> "/" <> name) (Just $ valueToBytes nodeInfo') Nothing

getNodeInfo :: ZHandle -> CBytes -> IO NodeInfo
getNodeInfo zk name = decodeZNodeValue' zk (serverRootPath <> "/" <> name)

getServerNode :: ZHandle -> CBytes -> IO ServerNode
getServerNode zk name = do
  host <- getServerHost zk name
  port <- getServerPort zk name
  return $ ServerNode
           { serverNodeId = 0
           , serverNodeHost = host
           , serverNodePort = port
           }

getReadyServers :: ZHandle -> IO Int
getReadyServers zk = do
  (StringsCompletion (StringVector servers)) <- zooGetChildren zk serverRootPath
  (sum <$>) . forM servers $ \name -> do
    (e' :: Either SomeException NodeStatus) <- try $ getNodeStatus zk name
    case e' of
      Right Ready   -> return (1 :: Int)
      Right Working -> return (1 :: Int)
      _             -> return (0 :: Int)
