{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence.Nodes (
    NodeStatus (..)
  , NodeInfo (..)

  , getNodeStatus
  , getServerUri
  , setNodeStatus

  , getReadyServers
  ) where

import           Control.Exception                 (SomeException, try)
import           Control.Monad                     (forM)
import           Data.Aeson                        (FromJSON, ToJSON)
import           Data.Functor                      (void, (<&>))
import qualified Data.Text.Lazy                    as TL
import           GHC.Generics                      (Generic)
import           GHC.Stack                         (HasCallStack)
import           Z.Data.CBytes                     (CBytes)
import           ZooKeeper                         (zooGetChildren, zooSet)
import           ZooKeeper.Types                   (StringVector (StringVector),
                                                    StringsCompletion (StringsCompletion),
                                                    ZHandle)

import           HStream.Server.Persistence.Common ()
import           HStream.Server.Persistence.Utils  (decodeZNodeValue',
                                                    serverRootPath)
import           HStream.Utils                     (valueToBytes)

data NodeStatus = Starting | Ready | Working
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data NodeInfo = NodeInfo
  { nodeStatus        :: NodeStatus
  , serverUri         :: TL.Text
  , serverInternalUri :: TL.Text
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

getNodeStatus :: ZHandle -> CBytes -> IO NodeStatus
getNodeStatus zk name = getNodeInfo zk name <&> nodeStatus

getServerUri :: ZHandle -> CBytes -> IO TL.Text
getServerUri zk name = getNodeInfo zk name <&> serverUri

setNodeStatus :: HasCallStack => ZHandle -> CBytes -> NodeStatus -> IO ()
setNodeStatus zk name status = do
  nodeInfo <- getNodeInfo zk name
  let nodeInfo' = nodeInfo { nodeStatus = status }
  void $ zooSet zk (serverRootPath <> "/" <> name) (Just $ valueToBytes nodeInfo') Nothing

getNodeInfo :: ZHandle -> CBytes -> IO NodeInfo
getNodeInfo zk name = decodeZNodeValue' zk (serverRootPath <> "/" <> name)

getReadyServers :: ZHandle -> IO Int
getReadyServers zk = do
  (StringsCompletion (StringVector servers)) <- zooGetChildren zk serverRootPath
  (sum <$>) . forM servers $ \name -> do
    (e' :: Either SomeException NodeStatus) <- try $ getNodeStatus zk name
    case e' of
      Right Ready   -> return (1 :: Int)
      Right Working -> return (1 :: Int)
      _             -> return (0 :: Int)
