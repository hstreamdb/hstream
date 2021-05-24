module HStream.Store.Admin.API
  ( sendAdminApiRequest
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
  , module Thrift.Protocol
  , module Thrift.Protocol.Id
  , module Thrift.Protocol.ApplicationException.Types
  , module Thrift.Channel
  , module Thrift.Channel.HeaderChannel
  , module Thrift.Channel.SocketChannel
  , module Thrift.Monad
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
import qualified Util.EventBase                             as FBUtil

sendAdminApiRequest
  :: HeaderConfig AdminAPI
  ->(forall p. (Protocol p) => ThriftM p HeaderWrappedChannel AdminAPI a)
  -> IO a
sendAdminApiRequest conf m =
  FBUtil.withEventBaseDataplane $ \evb ->
    withHeaderChannel evb conf m
