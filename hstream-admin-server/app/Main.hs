{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

import           Control.Monad.IO.Class         (liftIO)
import           Data.Aeson                     (ToJSON, FromJSON)
import qualified Data.Map.Strict                as Map
import           Data.Proxy                     (Proxy (..))
import           Data.Text                      (Text)
import           GHC.Generics                   (Generic)
import           Network.Wai.Handler.Warp       (run)
import           Servant.API                    
import           Servant.Server
import qualified Z.Data.CBytes                  as ZDC

import           HStream.Processing.Type        as HPT
import           HStream.Store                  as HS
import           HStream.Connector.HStore       as HCH

-- For testing
-- curl -X 'POST' \
--   'http://localhost:8000/stream' \
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
main = run 8000 (serve streamAPI streamServer)

streamAPI :: Proxy StreamsAPI
streamAPI = Proxy

data StreamBO = StreamBO
  {
    name              :: Text,
    replicationFactor :: Int
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
  liftIO (print "query stream")
  res <- liftIO $ doesStreamExists ldClient (mkStreamName $ ZDC.pack name)
  return True

removeStreamHandler :: HS.LDClient -> String -> Handler Bool
removeStreamHandler ldClient name = do
  liftIO (print "remove stream")
  liftIO $ removeStream ldClient (mkStreamName $ ZDC.pack name)
  return True

createStreamHandler :: HS.LDClient -> StreamBO -> Handler StreamBO
createStreamHandler ldClient stream = do
  liftIO (print "create stream")
  liftIO $ createStream ldClient (HCH.transToStreamName $ name stream)
          (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
  return stream

fetchStreamHandler :: HS.LDClient -> Handler [StreamBO]
fetchStreamHandler ldClient = do
  liftIO (print "fetch stream")
  streamNames <- liftIO $ findStreams ldClient True
  liftIO (print (show streamNames))
  return streams

streamServer :: Server StreamsAPI
streamServer = do
  ldClient <- liftIO (HS.newLDClient logDeviceConfigPath) 
  (fetchStreamHandler ldClient) :<|> (createStreamHandler ldClient) :<|> (removeStreamHandler ldClient) :<|> (queryStreamHandler ldClient)