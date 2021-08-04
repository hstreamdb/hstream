{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Query where

import           Control.Concurrent               (readMVar)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find)
import           Data.String                      (IsString (fromString))
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (Enumerated),
                                                   HasDefault (def))
import qualified Z.Data.Text                      as ZT

import qualified HStream.SQL.Codegen              as HSC
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   handleQueryTerminate)
import qualified HStream.Server.Persistence       as P
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (cBytesToLazyText,
                                                   lazyTextToCBytes,
                                                   returnErrResp, returnResp)

hstreamQueryToQuery :: P.PersistentQuery -> Query
hstreamQueryToQuery q@P.PersistentQuery {..} = def
  { queryId     = cBytesToLazyText queryId
  , queryStatus = pStatusToStatus queryStatus
  , queryCreatedTime    = queryCreatedTime
  , queryQueryStatement = TL.pack $ ZT.unpack queryBindedSql
  , queryQueryType      = Just def
      { queryTypeSinkStreamName    = cBytesToLazyText $ P.getQuerySink q
      , queryTypeSourceStreamNames = V.fromList $ cBytesToLazyText <$> P.getRelatedStreams q}
  }
  where
    mkEnum = Enumerated . Right
    pStatusToStatus = \case
      P.Created         -> mkEnum Query_StatusCreated
      P.Creating        -> mkEnum Query_StatusCreating
      P.Running         -> mkEnum Query_StatusRunning
      P.CreationAbort   -> mkEnum Query_StatusCreationAbort
      P.ConnectionAbort -> mkEnum Query_StatusConnectionAbort
      P.Terminated      -> mkEnum Query_StatusTerminated

listQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListQueriesRequest ListQueriesResponse
  -> IO (ServerResponse 'Normal ListQueriesResponse)
listQueriesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  queries <- P.withMaybeZHandle zkHandle P.getQueries
  let records = map hstreamQueryToQuery queries
  let resp = ListQueriesResponse . V.fromList $ records
  returnResp resp

getQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal GetQueryRequest Query
  -> IO (ServerResponse 'Normal Query)
getQueryHandler ServerContext{..} (ServerNormalRequest _metadata GetQueryRequest{..}) = do
  query <- do
    queries <- P.withMaybeZHandle zkHandle P.getQueries
    return $ find (\P.PersistentQuery{..} -> cBytesToLazyText queryId == getQueryRequestId) queries
  case query of
    Just q -> returnResp $ hstreamQueryToQuery q
    _      -> returnErrResp StatusInternal "Query does not exist"

terminateQueriesHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler sc@ServerContext{..} (ServerNormalRequest _metadata TerminateQueriesRequest{..}) = defaultExceptionHandle $ do
  qids <-
    if terminateQueriesRequestAll
      then HM.keys <$> readMVar runningQueries
      else return . V.toList $ lazyTextToCBytes <$> terminateQueriesRequestQueryId
  terminatedQids <- handleQueryTerminate sc (HSC.ManyQueries qids)
  if length terminatedQids < length qids
    then returnErrResp StatusAborted ("Only the following queries are terminated " <> fromString (show terminatedQids))
    else returnResp $ TerminateQueriesResponse (V.fromList $ cBytesToLazyText <$> terminatedQids)

deleteQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteQueryRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteQueryHandler ServerContext{..} (ServerNormalRequest _metadata DeleteQueryRequest{..}) =
  defaultExceptionHandle $ do
    P.withMaybeZHandle zkHandle $ P.removeQuery (lazyTextToCBytes deleteQueryRequestId)
    returnResp Empty
