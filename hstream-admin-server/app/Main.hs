{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

import           Control.Monad.IO.Class   (liftIO)
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text)
import           GHC.Generics             (Generic)
import           Network.Wai.Handler.Warp (run)
import           Servant                  (Capture, Delete, Get, JSON,
                                           PlainText, Post, Proxy (..), ReqBody,
                                           type (:>), (:<|>) (..))
import           Servant.Server           (Handler, Server, serve)
import qualified Z.Data.CBytes            as ZDC

import           HStream.Connector.HStore as HCH
import           HStream.Processing.Type  as HPT
import           HStream.Store            as HS

-- For testing
-- curl -X 'DELETE' \
--   'http://localhost:8000/streams/string' \
--   -H 'accept: */*' \
--   -H 'Content-Type: application/json' \
--   -d '{
--   "name": "string",
--   "replicationFactor": 0,
--   "beginTimestamp": 0
-- }'

logDeviceConfigPath :: ZDC.CBytes
logDeviceConfigPath = "/data/store/logdevice.conf"

scDefaultStreamRepFactor :: Int
scDefaultStreamRepFactor = 3

main :: IO ()
main = do
  ldClient <- liftIO (HS.newLDClient logDeviceConfigPath)
  run 8000 $ serve streamAPI $ streamServer ldClient

streamAPI :: Proxy StreamsAPI
streamAPI = Proxy

data StreamBO = StreamBO
  {
    name              :: Text,
    replicationFactor :: Int,
    beginTimestamp    :: Int
  } deriving (Eq, Show, Generic)
instance ToJSON StreamBO
instance FromJSON StreamBO

type StreamsAPI =
  "streams" :> Get '[JSON] [StreamBO]
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  :<|> "streams" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "streams" :> Capture "name" String :> Get '[JSON] Bool

streams :: [StreamBO]
streams = [StreamBO "test" 0, StreamBO "test2" 1]

queryStreamHandler :: HS.LDClient -> String -> Handler Bool
queryStreamHandler ldClient name = do
  res <- liftIO $ doesStreamExists ldClient (mkStreamName $ ZDC.pack name)
  return True

removeStreamHandler :: HS.LDClient -> String -> Handler Bool
removeStreamHandler ldClient name = do
  liftIO $ removeStream ldClient (mkStreamName $ ZDC.pack name)
  return True

createStreamHandler :: HS.LDClient -> StreamBO -> Handler StreamBO
createStreamHandler ldClient stream = do
  liftIO $ createStream ldClient (HCH.transToStreamName $ name stream)
          (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
  return stream

fetchStreamHandler :: HS.LDClient -> Handler [StreamBO]
fetchStreamHandler ldClient = do
  streamNames <- liftIO $ findStreams ldClient True
  liftIO (print (show streamNames))
  return streams

streamServer :: HS.LDClient -> Server StreamsAPI
streamServer ldClient = (fetchStreamHandler ldClient) :<|> (createStreamHandler ldClient) :<|> (removeStreamHandler ldClient) :<|> (queryStreamHandler ldClient)
