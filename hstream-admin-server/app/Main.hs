{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Monad.IO.Class      (liftIO)
import           Network.Wai.Handler.Warp    (run)
import           Servant                     (Proxy (..))
import           Servant.Server              (Server, serve)
import qualified Z.Data.CBytes               as ZDC

import           HStream.Admin.Server.Stream (StreamsAPI, streamServer)
import           HStream.Store               as HS

-- For testing
-- curl -X 'DELETE' \
--   'http://localhost:8000/streams/string' \
--   -H 'accept: */*' \
--   -H 'Content-Type: application/json' \
--   -d '{
--   "name": "string",
--   "replicationFactor": 3,
--   "beginTimestamp": 0
-- }'

logDeviceConfigPath :: ZDC.CBytes
logDeviceConfigPath = "/data/store/logdevice.conf"

type API = StreamsAPI

api :: Proxy API
api = Proxy

apiServer :: HS.LDClient -> Server StreamsAPI
apiServer = streamServer

main :: IO ()
main = do
  ldClient <- liftIO (HS.newLDClient logDeviceConfigPath)
  run 8000 $ serve api $ apiServer ldClient
