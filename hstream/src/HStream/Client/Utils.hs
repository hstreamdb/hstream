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
  , waitForServerToStart) where

import           Control.Concurrent               (threadDelay)
import           Data.Char                        (toUpper)
import qualified Data.Text                        as T
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import           Proto3.Suite.Class               (HasDefault, def)

import qualified HStream.Server.HStreamApi        as API
import           HStream.Utils                    (Format (formatResult),
                                                   SocketAddr (..),
                                                   mkClientNormalRequest,
                                                   mkGRPCClientConfWithSSL)

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

waitForServerToStart :: Int -> SocketAddr -> Maybe ClientSSLConfig -> IO (Maybe ())
waitForServerToStart t addr clientSSLConfig = withGRPCClient (mkGRPCClientConfWithSSL addr clientSSLConfig) $ \client -> do
  api <- API.hstreamApiClient client
  loop t api
  where
    interval = 1000000
    loop timeout api@API.HStreamApi{..} = do
     resp <- hstreamApiEcho (mkClientNormalRequest' $ API.EchoRequest "")
     case resp of
       ClientNormalResponse {} -> return $ Just ()
       _                       -> do
         let newTimeout = timeout - interval
         threadDelay interval
         putStrLn "Waiting for server to start..."
         if newTimeout <= 0 then return Nothing
           else loop newTimeout api

--------------------------------------------------------------------------------

printResult :: Format a => a -> IO ()
printResult resp = putStr $ formatResult resp
