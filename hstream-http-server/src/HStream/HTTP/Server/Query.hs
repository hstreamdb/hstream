{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.Query
  ( QueriesAPI, queryServer
  , listQueriesHandler
  , QueryBO(..)
  ) where

import           Control.Monad                    (void)
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
import           Proto3.Suite.Class               (def)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   Post, ReqBody, type (:>),
                                                   (:<|>) (..))
import           Servant.Server                   (Handler, Server)

import           HStream.HTTP.Server.Utils
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Utils                    (TaskStatus (..))

-- BO is short for Business Object
data QueryBO = QueryBO
  { id          :: T.Text
  , status      :: Maybe TaskStatus
  , createdTime :: Maybe Int64
  , queryText   :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON   QueryBO
instance FromJSON QueryBO
instance ToSchema QueryBO

type QueriesAPI
  =    "queries" :> Get '[JSON] [QueryBO]
  :<|> "queries" :> "restart" :> Capture "name" String :> Post '[JSON] ()
  :<|> "queries" :> "cancel"  :> Capture "name" String :> Post '[JSON] ()
  :<|> "queries" :> ReqBody '[JSON] QueryBO :> Post '[JSON] QueryBO
  :<|> "queries" :> Capture "name" String :> Delete '[JSON] ()
  :<|> "queries" :> Capture "name" String :> Get '[JSON] QueryBO

queryToQueryBO :: Query -> QueryBO
queryToQueryBO Query{..} = QueryBO
  { id          = TL.toStrict queryId
  , status      = Just (TaskStatus queryStatus)
  , createdTime = Just queryCreatedTime
  , queryText   = TL.toStrict queryQueryText
  }

-- FIXME: This is broken
createQueryHandler :: Client -> QueryBO -> Handler QueryBO
createQueryHandler hClient (QueryBO qid _ _ queryText) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createQueryRequest = def
        { createQueryRequestId = TL.fromStrict qid
        , createQueryRequestQueryText = TL.fromStrict queryText
        }
  resp <- hstreamApiCreateQuery (ClientNormalRequest createQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    -- TODO: should return querybo; but we need to update hstream api first
    ClientNormalResponse _ _meta1 _meta2 _status _details -> return $ QueryBO qid Nothing Nothing queryText
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ QueryBO qid Nothing Nothing queryText

listQueriesHandler :: Client -> Handler [QueryBO]
listQueriesHandler hClient = do
  resp <- liftIO $ do
    Log.debug "Send list queries request to HStream server. "
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiListQueries (mkClientNormalRequest def)
  V.toList . V.map queryToQueryBO . listQueriesResponseQueries <$> getServerResp' resp

deleteQueryHandler :: Client -> String -> Handler ()
deleteQueryHandler hClient qid = do
  resp <- liftIO $ do
    Log.debug $ "Send delete query request to HStream server. "
             <> "Query ID: " <> Log.buildString qid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiDeleteQuery . mkClientNormalRequest $ def
      { deleteQueryRequestId = TL.pack qid }
  void $ getServerResp' resp

getQueryHandler :: Client -> String -> Handler QueryBO
getQueryHandler hClient qid = do
  resp <- liftIO $ do
    Log.debug $ "Send get query request to HStream server. "
             <> "Query ID: " <> Log.buildString qid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiGetQuery . mkClientNormalRequest $ def
      { getQueryRequestId = TL.pack qid }
  queryToQueryBO <$> getServerResp' resp

restartQueryHandler :: Client -> String -> Handler ()
restartQueryHandler hClient qid = do
  resp <- liftIO $ do
    Log.debug $ "Send restart query request to HStream server. "
             <> "Query ID: " <> Log.buildString qid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiRestartQuery . mkClientNormalRequest $ def
      { restartQueryRequestId = TL.pack qid }
  void $ getServerResp' resp

cancelQueryHandler :: Client -> String -> Handler ()
cancelQueryHandler hClient qid = do
  resp <- liftIO $ do
    Log.debug $ "Send cancel query request to HStream server. "
            <> "Query ID: " <> Log.buildString qid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiTerminateQueries . mkClientNormalRequest $ def
      { terminateQueriesRequestQueryId = V.singleton $ TL.pack qid
      , terminateQueriesRequestAll     = False
      }
  void $ getServerResp' resp

queryServer :: Client -> Server QueriesAPI
queryServer hClient
  =    listQueriesHandler  hClient
  :<|> restartQueryHandler hClient
  :<|> cancelQueryHandler  hClient
  :<|> createQueryHandler  hClient
  :<|> deleteQueryHandler  hClient
  :<|> getQueryHandler     hClient
