{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Utils
  ( clientDefaultRequest
  , mkClientNormalRequest'
  , requestTimeout
  , extractSelect
  , printResult
  ) where

import qualified Data.ByteString.Char8         as BSC
import           Data.Char                     (toUpper)
import qualified Data.Map                      as Map
import qualified Data.Text                     as T
import           Network.GRPC.HighLevel.Client
import           Proto3.Suite.Class            (HasDefault, def)
import           Z.IO.Network.SocketAddr       (SocketAddr (..), ipv4)

import           HStream.Server.HStreamApi     (ServerNode (..))
import           HStream.Utils                 (Format (formatResult),
                                                mkClientNormalRequest,
                                                textToCBytes)

clientDefaultRequest :: HasDefault a => ClientRequest 'Normal a b
clientDefaultRequest = mkClientNormalRequest' def

requestTimeout :: Int
requestTimeout = 1000

mkClientNormalRequest' :: a -> ClientRequest 'Normal a b
mkClientNormalRequest' = mkClientNormalRequest requestTimeout

extractSelect :: [String] -> T.Text
extractSelect = T.pack .
  unwords . reverse . ("CHANGES;" :) .
  dropWhile ((/= "EMIT") . map toUpper) .
  reverse .
  dropWhile ((/= "SELECT") . map toUpper)

--------------------------------------------------------------------------------

printResult :: Format a => a -> IO ()
printResult resp = putStr $ formatResult resp
