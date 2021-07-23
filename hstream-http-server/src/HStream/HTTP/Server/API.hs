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
  , _logdeviceConfigPath :: ZDC.CBytes
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

apiServer :: HS.LDClient -> Client -> Server API
apiServer ldClient hClient = do
  (streamServer ldClient)
  :<|> (queryServer hClient)
  :<|> (nodeServer hClient)
  :<|> (connectorServer hClient)
  :<|> (overviewServer hClient ldClient)
  :<|> (viewServer hClient)

apiSwagger :: Swagger
apiSwagger = toSwagger api
