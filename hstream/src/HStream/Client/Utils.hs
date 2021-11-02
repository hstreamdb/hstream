{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Utils
  ( clientDefaultRequest
  , mkClientNormalRequest
  , requestTimeout
  , extractSelect
  , mkGRPCClientConf
  , serverNodeToSocketAddr
  ) where

import qualified Data.ByteString.Char8         as BSC
import           Data.Char                     (toUpper)
import qualified Data.Map                      as Map
import qualified Data.Text                     as T
import           HStream.Server.HStreamApi     (ServerNode (..))
import           HStream.Utils                 (textToCBytes)
import           Network.GRPC.HighLevel.Client
import           Proto3.Suite.Class            (HasDefault, def)
import           Z.IO.Network.SocketAddr       (SocketAddr (..), ipv4)

clientDefaultRequest :: HasDefault a => ClientRequest 'Normal a b
clientDefaultRequest = mkClientNormalRequest def

requestTimeout :: Int
requestTimeout = 1000

mkClientNormalRequest :: a -> ClientRequest 'Normal a b
mkClientNormalRequest x = ClientNormalRequest x requestTimeout (MetadataMap Map.empty)

extractSelect :: [String] -> T.Text
extractSelect = T.pack .
  unwords . reverse . ("CHANGES;" :) .
  dropWhile ((/= "EMIT") . map toUpper) .
  reverse .
  dropWhile ((/= "SELECT") . map toUpper)

mkGRPCClientConf :: SocketAddr -> ClientConfig
mkGRPCClientConf = \case
  SocketAddrIPv4 v4 port ->
    ClientConfig
    { clientServerHost = Host . BSC.pack . show $ v4
    , clientServerPort = Port $ fromIntegral port
    , clientArgs = []
    , clientSSLConfig = Nothing
    , clientAuthority = Nothing
    }
  SocketAddrIPv6 v6 port _flow _scope ->
    ClientConfig
    { clientServerHost = Host . BSC.pack . show $ v6
    , clientServerPort = Port $ fromIntegral port
    , clientArgs = []
    , clientSSLConfig = Nothing
    , clientAuthority = Nothing
    }

-- FIXME: It only supports IPv4 addresses and can throw 'InvalidArgument' exception.
serverNodeToSocketAddr :: ServerNode -> SocketAddr
serverNodeToSocketAddr ServerNode{..} = do
  ipv4 (textToCBytes serverNodeHost) (fromIntegral serverNodePort)
