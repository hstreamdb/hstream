{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE RecordWildCards        #-}

module HStream.Server.Handler.Redirect where

import           Control.Concurrent               (readMVar)
import           Control.Monad                    ((>=>))
import qualified Data.ByteString.Char8            as BS
import qualified Data.HashMap.Internal            as HM
import           Data.List                        (groupBy, (\\))
import           Data.Maybe                       (catMaybes)
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (HasDefault (def))
import           Z.IO.Network                     (SocketAddr)

import           Data.String                      (fromString)
import           HStream.Client.Utils             (mkClientNormalRequest,
                                                   mkGRPCClientConf)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.HStreamInternal   (HStreamInternal (..),
                                                   hstreamInternalClient)
import           HStream.Server.Handler.Connector (createSinkConnectorHandler,
                                                   terminateConnectorHandler)
import           HStream.Server.Handler.Query     (createQueryStreamHandler,
                                                   terminateQueriesHandler)
import           HStream.Server.LoadBalance       (getRanking)
import           HStream.Server.Persistence
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (cBytesToLazyText,
                                                   lazyTextToCBytes,
                                                   returnErrResp, returnResp)

createQueryStreamHandlerLeader :: ServerContext
  -> ServerRequest 'Normal CreateQueryStreamRequest CreateQueryStreamResponse
  -> IO (ServerResponse 'Normal CreateQueryStreamResponse)
createQueryStreamHandlerLeader sc sr@(ServerNormalRequest _ req) =
  ifRedirect sc req (createQueryStreamHandler sc sr)

createQueryStreamHandler' :: ServerContext
  -> ServerRequest 'Normal CreateQueryStreamRequest CreateQueryStreamResponse
  -> IO (ServerResponse 'Normal CreateQueryStreamResponse)
createQueryStreamHandler' sc@ServerContext{..} sr@(ServerNormalRequest _ req) = do
  leader <- readMVar leaderID
  ifIO (leader == serverID)
    (createQueryStreamHandlerLeader sc sr)
    $ do
      addr <- getServerInternalAddr zkHandle leader
      handleServerResp =<< redirect addr req

createQueryStreamHandlerInternal :: ServerContext
  -> ServerRequest 'Normal CreateQueryStreamRequest CreateQueryStreamResponse
  -> IO (ServerResponse 'Normal CreateQueryStreamResponse)
createQueryStreamHandlerInternal sc@ServerContext{..} sr = do
  leader <- readMVar leaderID
  ifIO (leader == serverID)
    (createQueryStreamHandlerLeader sc sr)
    (createQueryStreamHandler       sc sr)

createSinkConnectorHandlerLeader :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createSinkConnectorHandlerLeader sc sr@(ServerNormalRequest _ req) = do
  ifRedirect sc req (createSinkConnectorHandler sc sr)

createSinkConnectorHandler' :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createSinkConnectorHandler' sc@ServerContext{..} sr@(ServerNormalRequest _ req) = do
  leader <- readMVar leaderID
  ifIO (leader == serverID)
    (createSinkConnectorHandlerLeader sc sr)
    $ do
      addr <- getServerInternalAddr zkHandle leader
      handleServerResp =<< redirect addr req

createSinkConnectorHandlerInternal :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createSinkConnectorHandlerInternal sc@ServerContext{..} sr = do
  leader <- readMVar leaderID
  ifIO (leader == serverID)
    (createSinkConnectorHandlerLeader sc sr)
    (createSinkConnectorHandler       sc sr)

terminateConnectorHandler' :: ServerContext
  -> ServerRequest 'Normal TerminateConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
terminateConnectorHandler' sc sr@(ServerNormalRequest _ req) =
  ifRedirect sc req (terminateConnectorHandler sc sr)

terminateQueriesHandler' :: ServerContext
  -> ServerRequest 'Normal TerminateQueriesRequest TerminateQueriesResponse
  -> IO (ServerResponse 'Normal TerminateQueriesResponse)
terminateQueriesHandler' sc sr@(ServerNormalRequest _ req) =
  ifRedirect sc req (terminateQueriesHandler sc sr)

--------------------------------------------------------------------------------

class RedirectableHandler request response | request -> response where
  redirect :: SocketAddr -> request -> IO (ClientResult 'Normal response)
  ifRedirect :: ServerContext -> request -> IO (ServerResponse 'Normal response)
    -> IO (ServerResponse 'Normal response)

instance RedirectableHandler TerminateQueriesRequest TerminateQueriesResponse where
  redirect addr req =
    withGRPCClient (mkGRPCClientConf addr) $ \client -> do
      HStreamInternal {..} <- hstreamInternalClient client
      hstreamInternalTerminateQueries (mkClientNormalRequest req)
  ifRedirect ServerContext {..} TerminateQueriesRequest {..} action = do
    queriesOnThisServer <- HM.keys <$> readMVar runningQueries
    let queries = V.toList (lazyTextToCBytes <$> terminateQueriesRequestQueryId)
    resps <-
      if terminateQueriesRequestAll
        then getRanking >>= mapM
              ( P.getServerInternalAddr zkHandle
                  >=> (\x -> redirect x def {terminateQueriesRequestAll = True})
              )
        else do
          distributed <- howToDistribute (queries \\ queriesOnThisServer)
          mapM (\(x, y) -> redirect x def {terminateQueriesRequestQueryId = V.fromList y}) distributed
    serverResp <- action
    success <- mconcat . catMaybes <$> mapM getSuccessful resps
    case serverResp of
      ServerNormalResponse (Just x@TerminateQueriesResponse {..}) mm sc sd ->
        return $ ServerNormalResponse
          ( Just x { terminateQueriesResponseQueryId = terminateQueriesResponseQueryId <> success } ) mm sc sd
      ServerNormalResponse Nothing mm sc sd ->
        return $ ServerNormalResponse Nothing mm sc (sd <> fromString (show success))
    where
      howToDistribute qids = do
        qHServers <-
          mapM (flip P.getQuery zkHandle >=> P.getServerInternalAddr zkHandle . P.queryHServer) qids
        return $
          map (\(x : xs) -> (fst x, map snd (x : xs))) $
            groupBy (\x y -> fst x == fst y) (zip qHServers (cBytesToLazyText <$> qids))
      getSuccessful :: ClientResult 'Normal TerminateQueriesResponse -> IO (Maybe (V.Vector TL.Text))
      getSuccessful = \case
        ClientNormalResponse x _ _ StatusOk _ ->
          return . Just $ terminateQueriesResponseQueryId x
        ClientErrorResponse err -> do
          Log.e (fromString (show err))
          return Nothing
        _ -> return Nothing

instance RedirectableHandler CreateSinkConnectorRequest Connector where
  redirect addr req =
    withGRPCClient (mkGRPCClientConf addr) $ \client -> do
      HStreamInternal {..} <- hstreamInternalClient client
      hstreamInternalCreateSinkConnector (mkClientNormalRequest req)
  ifRedirect ServerContext {..} req action = do
    bestCandidate <- head <$> getRanking
    ifIO (bestCandidate == serverID) action $ do
      addr <- getServerInternalAddr zkHandle bestCandidate
      handleServerResp =<< redirect addr req

instance RedirectableHandler TerminateConnectorRequest Empty where
  redirect addr req =
    withGRPCClient (mkGRPCClientConf addr) $ \client -> do
      HStreamInternal {..} <- hstreamInternalClient client
      hstreamInternalTerminateConnector (mkClientNormalRequest req)
  ifRedirect ServerContext {..} req@TerminateConnectorRequest {..} action = do
    server <-
      connectorHServer
        <$> getConnector (lazyTextToCBytes terminateConnectorRequestConnectorId) zkHandle
    ifIO (server == serverID) action $ do
      addr <- getServerInternalAddr zkHandle server
      handleServerResp =<< redirect addr req

instance RedirectableHandler CreateQueryStreamRequest CreateQueryStreamResponse where
  redirect addr req =
    withGRPCClient (mkGRPCClientConf addr) $ \client -> do
      HStreamInternal {..} <- hstreamInternalClient client
      hstreamInternalCreateQueryStream (mkClientNormalRequest req)
  ifRedirect ServerContext {..} req action = do
    bestCandidate <- head <$> getRanking
    ifIO (bestCandidate == serverID) action $ do
      addr <- getServerInternalAddr zkHandle bestCandidate
      handleServerResp =<< redirect addr req

ifIO :: Bool -> IO a -> IO a -> IO a
ifIO p action1 action2
  | p         = action1
  | otherwise = action2

handleServerResp :: ClientResult 'Normal response -> IO (ServerResponse 'Normal response)
handleServerResp result = do
  case result of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> returnResp x
    ClientNormalResponse _resp _meta1 _meta2 _status _details -> do
      returnErrResp StatusInternal $ StatusDetails (BS.pack $ "Impossible happened..." <> show _status)
    -- TODO: This requires more precise handling
    ClientErrorResponse err -> returnErrResp StatusInternal $ StatusDetails (BS.pack $ show err)
