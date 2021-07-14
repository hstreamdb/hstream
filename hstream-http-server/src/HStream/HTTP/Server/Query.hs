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

import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.Int                         (Int64)
import qualified Data.Map.Strict                  as Map
import           Data.Swagger                     (ToSchema)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Client     (Client)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   Post, ReqBody, type (:>),
                                                   (:<|>) (..))
import           Servant.Server                   (Handler, Server)
import qualified Z.IO.Logger                      as Log

import           HStream.Server.HStreamApi

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

queryToQueryBO :: Query -> QueryBO
queryToQueryBO (Query id' status createdTime queryText) =
  QueryBO (TL.toStrict id') (Just $ fromIntegral status) (Just createdTime) (TL.toStrict queryText)

createQueryHandler :: Client -> QueryBO -> Handler QueryBO
createQueryHandler hClient (QueryBO qid _ _ queryText) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createQueryRequest = CreateQueryRequest { createQueryRequestId = TL.pack $ T.unpack qid
                                              , createQueryRequestQueryText = TL.pack $ T.unpack queryText
                                              }
  resp <- hstreamApiCreateQuery (ClientNormalRequest createQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    -- TODO: should return querybo; but we need to update hstream api first
    ClientNormalResponse _ _meta1 _meta2 _status _details -> return $ QueryBO qid Nothing Nothing queryText
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ QueryBO qid Nothing Nothing queryText

listQueriesHandler :: Client -> Handler [QueryBO]
listQueriesHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let listQueriesRequest = ListQueriesRequest {}
  resp <- hstreamApiListQueries (ClientNormalRequest listQueriesRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@ListQueriesResponse{} _meta1 _meta2 _status _details -> do
      case x of
        ListQueriesResponse {listQueriesResponseQueries = queries} -> do
          return $ V.toList $ V.map queryToQueryBO queries
        _ -> return []
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

deleteQueryHandler :: Client -> String -> Handler Bool
deleteQueryHandler hClient qid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let deleteQueryRequest = DeleteQueryRequest { deleteQueryRequestId = TL.pack qid }
  resp <- hstreamApiDeleteQuery (ClientNormalRequest deleteQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse x _meta1 _meta2 StatusInternal _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False
    _ -> return False

getQueryHandler :: Client -> String -> Handler (Maybe QueryBO)
getQueryHandler hClient qid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let getQueryRequest = GetQueryRequest { getQueryRequestId = TL.pack qid }
  resp <- hstreamApiGetQuery (ClientNormalRequest getQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return $ Just $ queryToQueryBO x
    ClientNormalResponse x _meta1 _meta2 StatusInternal _details -> return Nothing
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

restartQueryHandler :: Client -> String -> Handler Bool
restartQueryHandler hClient qid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let restartQueryRequest = RestartQueryRequest { restartQueryRequestId = TL.pack qid }
  resp <- hstreamApiRestartQuery (ClientNormalRequest restartQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse x _meta1 _meta2 StatusInternal _details -> return False
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False

cancelQueryHandler :: Client -> String -> Handler Bool
cancelQueryHandler hClient qid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let cancelQueryRequest = CancelQueryRequest { cancelQueryRequestId = TL.pack qid }
  resp <- hstreamApiCancelQuery (ClientNormalRequest cancelQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse x _meta1 _meta2 StatusInternal _details -> return False
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False

queryServer :: Client -> Server QueriesAPI
queryServer hClient =
  (listQueriesHandler hClient)
  :<|> (restartQueryHandler hClient)
  :<|> (cancelQueryHandler hClient)
  :<|> (createQueryHandler hClient)
  :<|> (deleteQueryHandler hClient)
  :<|> (getQueryHandler hClient)
