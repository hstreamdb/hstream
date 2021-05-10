{-# LANGUAGE TypeApplications #-}

module Main where

import qualified Admin.AdminAPI.Client        as Admin
import           Data.Text                    (Text)
import           Fb303.FacebookService.Client (getVersion)
import           Thrift.Channel.SocketChannel (SocketConfig (..), localhost,
                                               withSocketChannel)
import           Thrift.Protocol.Id           (binaryProtocolId)

main :: IO Text
main = do
  -- change to your lodevice admin server host & port
  let (host, port) = (localhost, 47440)

  let socketConfig = SocketConfig host port binaryProtocolId
  withSocketChannel @Admin.AdminAPI socketConfig getVersion
