{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

import           Control.Monad.IO.Class      (liftIO)
import           Control.Exception            (catch)
import           Control.Monad            (void)
import           Network.Wai.Handler.Warp    (run)
import           Servant                     (Proxy (..), (:<|>) (..))
import           Servant.Server              (Server, serve)
import qualified Z.Data.CBytes               as ZDC
import qualified ZooKeeper                   as ZK
import qualified ZooKeeper.Exception         as ZK
import qualified ZooKeeper.Types             as ZK


import           HStream.Admin.Server.Query  (QueriesAPI, queryServer)
import           HStream.Admin.Server.Stream (StreamsAPI, streamServer)
import           HStream.Server.Persistence  as HSP
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

zkHost = "127.0.0.1"
zkPort = "2181"

initZooKeeper :: ZK.ZHandle -> IO ()
initZooKeeper zk = catch (initializeAncestors zk) (\e -> void $ return (e :: ZK.ZNODEEXISTS))

type API =
  StreamsAPI
  :<|> QueriesAPI

api :: Proxy API
api = Proxy

apiServer :: HS.LDClient -> Maybe ZK.ZHandle -> Server API
apiServer ldClient zk =
  (streamServer ldClient)
  :<|> (queryServer ldClient zk)

main :: IO ()
main = ZK.withResource (HSP.defaultHandle (zkHost <> ":" <> zkPort)) $
    \zk -> do
      initZooKeeper zk
      ldClient <- liftIO (HS.newLDClient logDeviceConfigPath)
      run 8000 $ serve api $ apiServer ldClient (Just zk)
