{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.InternalHandler where

import           Network.GRPC.HighLevel.Generated

import           HStream.Server.HStreamInternal   (HStreamInternal (..))
import           HStream.Server.Types             (ServerContext)
import           HStream.Utils                    (returnErrResp)

internalHandlers :: ServerContext -> IO (HStreamInternal ServerRequest ServerResponse)
internalHandlers _ = pure HStreamInternal {
  -- TODO : add corresponding implementation and subscription api
    hstreamInternalCreateQueryStream   = unimplemented
  , hstreamInternalRestartQuery        = unimplemented
  , hstreamInternalTerminateQueries    = unimplemented
  , hstreamInternalCreateSinkConnector = unimplemented
  , hstreamInternalRestartConnector    = unimplemented
  , hstreamInternalTerminateConnector  = unimplemented
  }
  where
    unimplemented = const (returnErrResp StatusInternal "unimplemented method called")
