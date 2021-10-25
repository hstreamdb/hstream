{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.InternalHandler where

import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Server.HStreamInternal
import           HStream.Server.LoadBalance       (getRanking)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnErrResp, returnResp)

internalHandlers :: ServerContext -> IO (HStreamInternal ServerRequest ServerResponse)
internalHandlers ctx = pure HStreamInternal {
  -- TODO : add corresponding implementation and subscription api
    hstreamInternalCreateQueryStream   = unimplemented
  , hstreamInternalRestartQuery        = unimplemented
  , hstreamInternalTerminateQueries    = unimplemented
  , hstreamInternalCreateSinkConnector = unimplemented
  , hstreamInternalRestartConnector    = unimplemented
  , hstreamInternalTerminateConnector  = unimplemented

  , hstreamInternalGetNodesRanking     = getNodesRankingHandler ctx
  }
  where
    unimplemented = const (returnErrResp StatusInternal "unimplemented method called")

getNodesRankingHandler :: ServerContext
                       -> ServerRequest 'Normal Empty GetNodesRankingResponse
                       -> IO (ServerResponse 'Normal GetNodesRankingResponse)
getNodesRankingHandler ServerContext{..} (ServerNormalRequest _meta _) = do
  nodes <- getRanking >>= mapM (P.getServerNode zkHandle)
  let resp = GetNodesRankingResponse $ V.fromList nodes
  returnResp resp
