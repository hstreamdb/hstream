-- TODO

module HStream.Store.RPC.MessagePack
  ( serve
  ) where

import qualified Z.IO.Network         as Z
import qualified Z.IO.RPC.MessagePack as MP

serve :: Z.TCPServerConfig -> IO ()
serve tcpConf = MP.serveRPC (Z.startTCPServer tcpConf) $ MP.simpleRouter []
