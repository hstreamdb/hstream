{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators   #-}

module HStream.HTTP.Server.API (
  API, api, apiServer, ServerConfig(..), apiSwagger
) where

import           Data.ByteString               (ByteString)
import           Data.Swagger                  (Swagger)
import           Network.GRPC.LowLevel.Client  (Client)
import           Servant                       (Proxy (..), (:<|>) (..))
import           Servant.Server                (Server)
import           Servant.Swagger               (toSwagger)
import qualified Z.Data.CBytes                 as ZDC
import qualified ZooKeeper.Types               as ZK

import           HStream.HTTP.Server.Connector (ConnectorsAPI, connectorServer)
import           HStream.HTTP.Server.Node      (NodesAPI, nodeServer)
import           HStream.HTTP.Server.Overview  (OverviewAPI, overviewServer)
import           HStream.HTTP.Server.Query     (QueriesAPI, queryServer)
import           HStream.HTTP.Server.Stream    (StreamsAPI, streamServer)
import           HStream.HTTP.Server.View      (ViewsAPI, viewServer)
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
  , _hstreamHost         :: ByteString
  , _hstreamPort         :: Int
  } deriving (Show)

type API =
  StreamsAPI
  :<|> QueriesAPI
  :<|> NodesAPI
  :<|> ConnectorsAPI
  :<|> OverviewAPI
  :<|> ViewsAPI

api :: Proxy API
api = Proxy

apiServer :: HS.LDClient -> Maybe ZK.ZHandle -> Client -> ServerConfig -> Server API
apiServer ldClient zk hClient ServerConfig{..} = do
  (streamServer ldClient)
  :<|> (queryServer hClient)
  :<|> (nodeServer _ldAdminHost _ldAdminPort)
  :<|> (connectorServer hClient)
  :<|> (overviewServer ldClient zk _ldAdminHost _ldAdminPort)
  :<|> (viewServer hClient)

apiSwagger :: Swagger
apiSwagger = toSwagger api
