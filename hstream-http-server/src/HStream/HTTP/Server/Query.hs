{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.Query (
  QueriesAPI, queryServer
) where

import           Control.Concurrent               (forkIO, killThread)
import           Control.Exception                (SomeException, catch, try)
import           Control.Monad                    (void)
import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.Int                         (Int64)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import           Data.Swagger                     (ToSchema)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Client     (Client)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   PlainText, Post, ReqBody,
                                                   type (:>), (:<|>) (..))
import           Servant.Server                   (Handler, Server)
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log
import           Z.IO.Time                        (SystemTime (..),
                                                   getSystemTime')
import qualified ZooKeeper.Types                  as ZK

import qualified HStream.Connector.HStore         as HCH
import           HStream.Processing.Connector     (subscribeToStream)
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Type          (Offset (..))
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (runTaskWrapper)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils.Converter          (cbytesToText, textToCBytes)

-- BO is short for Business Object
data QueryBO = QueryBO
  { id          :: T.Text
  , status      :: Maybe Int
  , createdTime :: Maybe Int64
  , queryText   :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON QueryBO
instance FromJSON QueryBO
instance ToSchema QueryBO

type QueriesAPI =
  "queries" :> Get '[JSON] [QueryBO]
  :<|> "queries" :> "restart" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "queries" :> "cancel" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "queries" :> ReqBody '[JSON] QueryBO :> Post '[JSON] QueryBO
  :<|> "queries" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "queries" :> Capture "name" String :> Get '[JSON] (Maybe QueryBO)

queryResponseToQueryBO :: GetQueryResponse -> QueryBO
queryResponseToQueryBO (GetQueryResponse id status createdTime queryText _) =
  QueryBO (TL.toStrict id) (Just $ fromIntegral status) (Just createdTime) (TL.toStrict queryText)


hstreamQueryToQueryBO :: HSP.Query -> QueryBO
hstreamQueryToQueryBO (HSP.Query queryId (HSP.Info sqlStatement createdTime) _ (HSP.Status status _)) =
  QueryBO (cbytesToText queryId) (Just $ fromEnum status) (Just createdTime) (T.pack $ ZT.unpack sqlStatement)

hstreamQueryNameIs :: T.Text -> HSP.Query -> Bool
hstreamQueryNameIs name (HSP.Query queryId _ _ _) = (cbytesToText queryId) == name

removeQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
removeQueryHandler ldClient zkHandle name = liftIO $ catch
  ((HSP.withMaybeZHandle zkHandle $ HSP.removeQuery (ZDC.pack name)) >> return True)
  (\(e :: SomeException) -> return False)

-- TODO: we should remove the duplicate code in HStream/Admin/Server/Query.hs and HStream/Server/Handler.hs
createQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> Client -> (Int, ZDC.CBytes) -> QueryBO -> Handler QueryBO
createQueryHandler ldClient zkHandle hClient (streamRepFactor, checkpointRootPath) (QueryBO qid _ _ queryText) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createQueryRequest = CreateQueryRequest { createQueryRequestId = TL.pack $ T.unpack qid
                                              , createQueryRequestQueryText = TL.pack $ T.unpack queryText
                                              }
  resp <- hstreamApiCreateQuery (ClientNormalRequest createQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@CreateQueryResponse{} _meta1 _meta2 _status _details -> return $ QueryBO qid Nothing Nothing queryText
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ QueryBO qid Nothing Nothing queryText

fetchQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> Client -> Handler [QueryBO]
fetchQueryHandler ldClient zkHandle hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let fetchQueryRequest = FetchQueryRequest {}
  resp <- hstreamApiFetchQuery (ClientNormalRequest fetchQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@FetchQueryResponse{} _meta1 _meta2 _status _details -> do
      case x of
        FetchQueryResponse {fetchQueryResponseResponses = queries} -> do
          return $ V.toList $ V.map queryResponseToQueryBO queries
        _ -> return []
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

getQueryHandler :: Maybe ZK.ZHandle -> String -> Handler (Maybe QueryBO)
getQueryHandler zkHandle name = do
  query <- liftIO $ do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    return $ find (hstreamQueryNameIs (T.pack name)) queries
  return $ hstreamQueryToQueryBO <$> query

-- Question: What else should I do to really restart the query? Unsubscribe and Stop CkpReader?
restartQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
restartQueryHandler ldClient zkHandle name = do
  res <- liftIO $ do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack name)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Running)
        return True
      Nothing -> return False
  return res

cancelQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
cancelQueryHandler ldClient zkHandle name = do
  res <- liftIO $ do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack name)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Terminated)
        return True
      Nothing -> return False
  return res

queryServer :: HS.LDClient -> Maybe ZK.ZHandle -> Client -> (Int, ZDC.CBytes) -> Server QueriesAPI
queryServer ldClient zkHandle hClient (streamRepFactor, checkpointRootPath) =
  (fetchQueryHandler ldClient zkHandle hClient)
  :<|> (restartQueryHandler ldClient zkHandle)
  :<|> (cancelQueryHandler ldClient zkHandle)
  :<|> (createQueryHandler ldClient zkHandle hClient (streamRepFactor, checkpointRootPath))
  :<|> (removeQueryHandler ldClient zkHandle)
  :<|> (getQueryHandler zkHandle)
