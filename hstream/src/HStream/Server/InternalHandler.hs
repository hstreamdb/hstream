{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.InternalHandler where

import           Network.GRPC.HighLevel.Generated

import           HStream.Server.HStreamInternal
import           HStream.Server.Types             (ServerContext (..))
import           HStream.Utils                    (returnErrResp)

internalHandlers :: ServerContext -> IO (HStreamInternal ServerRequest ServerResponse)
internalHandlers _ = pure HStreamInternal {
  -- TODO : add corresponding implementation and subscription api
    hstreamInternalCreateQueryStream   = unimplemented
  , hstreamInternalRestartQuery        = unimplemented
  , hstreamInternalTerminateQueries    = unimplemented
  , hstreamInternalCreateConnector     = unimplemented
  , hstreamInternalStartConnector    = unimplemented
  , hstreamInternalStopConnector  = unimplemented

  }
  where
    unimplemented = const (returnErrResp StatusUnimplemented "unimplemented method called")
