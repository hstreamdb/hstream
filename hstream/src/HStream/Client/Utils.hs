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
  , waitForServerToStart
  , calculateShardId
  , dropPlanToResType) where

import           Control.Concurrent               (threadDelay)
import           Crypto.Hash.MD5                  (hash)
import qualified Data.ByteString                  as BS
import           Data.Char                        (toUpper)
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as BS
import           Data.Word                        (Word64)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import           Proto3.Suite.Class               (HasDefault, def)

import           HStream.Client.Types             (ResourceType (..))
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (DropObject (..))
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

calculateShardId :: T.Text -> [API.Shard] -> Maybe Word64
calculateShardId key (API.Shard{..}:ss) =
  case (compareHash result start, compareHash result end) of
    (GT, LT) -> Just shardShardId
    (EQ, _)  -> Just shardShardId
    (_, EQ)  -> Just shardShardId
    _        -> calculateShardId key ss
  where
    compareHash x y = if BS.length x == BS.length y
      then x `compare` y
      else BS.length x `compare` BS.length y
    start = BS.encodeUtf8 shardStartHashRangeKey
    end = BS.encodeUtf8 shardEndHashRangeKey
    result = hash (BS.encodeUtf8 key)
calculateShardId _ _ = Nothing

dropPlanToResType :: DropObject -> ResourceType
dropPlanToResType (DConnector cid ) = ResConnector cid
dropPlanToResType DView{}           = undefined
dropPlanToResType DStream{}         = undefined
--------------------------------------------------------------------------------

printResult :: Format a => a -> IO ()
printResult resp = putStr $ formatResult resp
