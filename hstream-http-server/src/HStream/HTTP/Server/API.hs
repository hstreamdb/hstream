{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators   #-}

module HStream.HTTP.Server.API (
  API, api, apiServer, ServerConfig(..), apiSwagger
) where

import           Data.ByteString               (ByteString)
import qualified Data.ByteString.Lazy.Char8    as BL8
import           Data.Swagger                  (Swagger)
import           Servant                       (Proxy (..), (:<|>) (..))
import           Servant.Server                (Server)
import           Servant.Swagger               (toSwagger)
import qualified Z.Data.CBytes                 as ZDC
import qualified ZooKeeper                     as ZK
import qualified ZooKeeper.Exception           as ZK
import qualified ZooKeeper.Types               as ZK

import           HStream.HTTP.Server.Connector (ConnectorsAPI, connectorServer)
import           HStream.HTTP.Server.Node      (NodesAPI, nodeServer)
import           HStream.HTTP.Server.Overview  (OverviewAPI, overviewServer)
import           HStream.HTTP.Server.Query     (QueriesAPI, queryServer)
import           HStream.HTTP.Server.Stream    (StreamsAPI, streamServer)
import qualified HStream.Store                 as HS

data ServerConfig = ServerConfig
  { _serverHost          :: ZDC.CBytes
  , _serverPort          :: Int
  , _zkHost              :: ZDC.CBytes
  , _zkPort              :: ZDC.CBytes
  , _logdeviceConfigPath :: ZDC.CBytes
  , _checkpointRootPath  :: ZDC.CBytes
  , _streamRepFactor     :: Int
  , _ldAdminHost         :: ByteString
  , _ldAdminPort         :: Int
  } deriving (Show)

type API =
  StreamsAPI
  :<|> QueriesAPI
  :<|> NodesAPI
  :<|> ConnectorsAPI
  :<|> OverviewAPI

api :: Proxy API
api = Proxy

apiServer :: HS.LDClient -> Maybe ZK.ZHandle -> ServerConfig -> Server API
apiServer ldClient zk ServerConfig{..} = do
  (streamServer ldClient)
  :<|> (queryServer ldClient zk (_streamRepFactor, _checkpointRootPath))
  :<|> (nodeServer _ldAdminHost _ldAdminPort)
  :<|> (connectorServer ldClient zk)
  :<|> (overviewServer ldClient zk _ldAdminHost _ldAdminPort)

apiSwagger :: Swagger
apiSwagger = toSwagger api
