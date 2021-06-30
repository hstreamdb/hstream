module HStream.Store.Admin.API
  ( sendAdminApiRequest
  , getNodesAdminAddr
  , buildLDClientRes
  , withResource

  , module Thrift.Protocol
  , module Thrift.Protocol.Id
  , module Thrift.Protocol.ApplicationException.Types
  , module Thrift.Channel
  , module Thrift.Channel.HeaderChannel
  , module Thrift.Channel.SocketChannel
  , module Thrift.Monad
  , module Thrift.Server.CppServer
  , FBUtil.withEventBaseDataplane

  , module Admin.AdminAPI.Client
  , module Admin.AdminAPI.Service
  , module AdminCommands.Types
  , module ClusterMembership.Types
  , module Exceptions.Types
  , module Fb303.FacebookService.Client
  , module Fb303.Types
  , module Logtree.Types
  , module Maintenance.Types
  , module Nodes.Types
  , module Safety.Types
  , module Settings.Types
  , module Common.Types
  ) where

import           Admin.AdminAPI.Client
import           Admin.AdminAPI.Service
import           AdminCommands.Types
import           ClusterMembership.Types
import           Common.Types
import           Exceptions.Types
import           Fb303.FacebookService.Client
import           Fb303.Types
import           Logtree.Types
import           Maintenance.Types
import           Nodes.Types
import           Safety.Types
import           Settings.Types
import           Thrift.Channel
import           Thrift.Channel.HeaderChannel
import           Thrift.Channel.SocketChannel
import           Thrift.Codegen
import           Thrift.Monad
import           Thrift.Protocol
import           Thrift.Protocol.ApplicationException.Types
import           Thrift.Protocol.Id
import           Thrift.Server.CppServer

import qualified Data.Map.Strict                            as Map
import           Data.Maybe                                 (fromJust)
import qualified Data.Text.Encoding                         as DText
import qualified Util.EventBase                             as FBUtil
import           Z.Data.CBytes                              (CBytes)
import           Z.Foreign                                  (fromByteString,
                                                             withPrimVectorSafe)
import           Z.IO.Buffered                              (writeOutput)
import qualified Z.IO.Environment                           as Env
import qualified Z.IO.FileSystem                            as FS
import           Z.IO.Resource                              (Resource, liftIO,
                                                             withResource)

import qualified HStream.Store                              as S

sendAdminApiRequest
  :: HeaderConfig AdminAPI
  ->(forall p. (Protocol p) => ThriftM p HeaderWrappedChannel AdminAPI a)
  -> IO a
sendAdminApiRequest conf m =
  FBUtil.withEventBaseDataplane $ \evb ->
    withHeaderChannel' evb conf True False m

getNodesAdminAddr :: HeaderConfig AdminAPI -> NodesFilter -> IO [SocketAddress]
getNodesAdminAddr conf nodesFilter = do
  config <- sendAdminApiRequest conf (getNodesConfig nodesFilter)
  return $ map fromJust . filter (== Nothing) $
    map (maybe Nothing addresses_admin . nodeConfig_other_addresses) (nodesConfigResponse_nodes config)

buildLDClientRes
  :: HeaderConfig AdminAPI
  -> Map.Map CBytes CBytes
  -> Resource S.LDClient
buildLDClientRes conf settings = do
  liftIO $ S.setLogDeviceDbgLevel S.C_DBG_CRITICAL
  d <- liftIO Env.getTempDir
  (path, file) <- FS.mkstemp d "ld_conf_" False
  liftIO $ do
    config <- sendAdminApiRequest conf dumpServerConfigJson
    let content = fromByteString $ DText.encodeUtf8 config
    withPrimVectorSafe content (writeOutput file)
    client <- S.newLDClient path
    S.setClientSettings client settings
    return client
