module HStream.Client.Type where

import           Control.Concurrent (MVar)
import           Network.Socket     (PortNumber)
import           Z.IO.Network       (SocketAddr)

data ClientContext = ClientContext
  { cctxServerHost                 :: String
  , cctxServerPort                 :: PortNumber
  , availableServers               :: MVar [SocketAddr]
  , currentServer                  :: MVar SocketAddr
  , clientId                       :: String
  , availableServersUpdateInterval :: Int
  }
