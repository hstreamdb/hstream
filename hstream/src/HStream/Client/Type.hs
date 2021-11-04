module HStream.Client.Type where

import           Control.Concurrent        (MVar)
import qualified Data.Map                  as Map
import qualified Data.Text                 as T
import qualified HStream.Server.HStreamApi as API
import           Network.Socket            (PortNumber)
import           Z.IO.Network              (SocketAddr)

data ClientContext = ClientContext
  { cctxServerHost                 :: String
  , cctxServerPort                 :: PortNumber
  , availableServers               :: MVar [SocketAddr]
  , currentServer                  :: MVar SocketAddr
  , producers                      :: MVar (Map.Map T.Text API.ServerNode)
  , clientId                       :: String
  , availableServersUpdateInterval :: Int
  }
