{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Utils
  ( clientDefaultRequest
  , mkClientNormalRequest
  , requestTimeout
  , extractSelect
  , mkGRPCClientConf
  ) where

import qualified Data.ByteString               as BS
import qualified Data.ByteString.Char8         as BSC
import           Data.Char                     (toUpper)
import qualified Data.Map                      as Map
import qualified Data.Text.Lazy                as TL
import           Network.GRPC.HighLevel.Client
import           Network.URI
import           Proto3.Suite.Class            (HasDefault, def)

clientDefaultRequest :: HasDefault a => ClientRequest 'Normal a b
clientDefaultRequest = mkClientNormalRequest def

requestTimeout :: Int
requestTimeout = 1000

mkClientNormalRequest :: a -> ClientRequest 'Normal a b
mkClientNormalRequest x = ClientNormalRequest x requestTimeout (MetadataMap Map.empty)

extractSelect :: [String] -> TL.Text
extractSelect = TL.pack .
  unwords . reverse . ("CHANGES;" :) .
  dropWhile ((/= "EMIT") . map toUpper) .
  reverse .
  dropWhile ((/= "SELECT") . map toUpper)

mkGRPCClientConf :: BS.ByteString -> ClientConfig
mkGRPCClientConf uri = ClientConfig {
    clientServerHost = Host serverHost
  , clientServerPort = Port serverPort
  , clientArgs = []
  , clientSSLConfig = Nothing
  , clientAuthority = Nothing
  }
  where
    (Just parsedUri) = parseURI ("hstream://" <> BSC.unpack uri)
    Just (URIAuth _ host_ port_) = uriAuthority parsedUri
    serverHost = BSC.pack host_
    serverPort = read (tail port_)
