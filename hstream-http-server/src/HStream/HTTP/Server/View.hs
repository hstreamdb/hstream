{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.View (
  ViewsAPI, viewServer, listViewsHandler, ViewBO(..)
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
import qualified Proto3.Suite                     as PB
import           Servant                          (Capture, Delete, Get, JSON,
                                                   Post, ReqBody, type (:>),
                                                   (:<|>) (..))
import           Servant.Server                   (Handler, Server)

import           HStream.Server.HStreamApi

-- BO is short for Business Object
data ViewBO = ViewBO
  { id          :: Maybe T.Text
  , status      :: Maybe (PB.Enumerated Status)
  , createdTime :: Maybe Int64
  , sql         :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON ViewBO
instance FromJSON ViewBO
instance ToSchema ViewBO
instance ToJSON (PB.Enumerated Status)
instance FromJSON (PB.Enumerated Status)

type ViewsAPI =
  "views" :> Get '[JSON] [ViewBO]
  :<|> "views" :> ReqBody '[JSON] ViewBO :> Post '[JSON] ViewBO
  :<|> "views" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "views" :> Capture "name" String :> Get '[JSON] (Maybe ViewBO)

viewToViewBO :: View -> ViewBO
viewToViewBO (View id' status createdTime sql _) =
  ViewBO (Just $ TL.toStrict id') (Just status) (Just createdTime) (TL.toStrict sql)

createViewHandler :: Client -> ViewBO -> Handler ViewBO
createViewHandler hClient (ViewBO _ _ _ sql) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createViewRequest = CreateViewRequest { createViewRequestSql = TL.pack $ T.unpack sql }
  resp <- hstreamApiCreateView (ClientNormalRequest createViewRequest 100 (MetadataMap $ Map.empty))
  -- TODO: should return viewBO; but we need to update the hstream server first.
  case resp of
    ClientNormalResponse view _meta1 _meta2 _status _details -> return $ viewToViewBO view
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ ViewBO Nothing Nothing Nothing sql

listViewsHandler :: Client -> Handler [ViewBO]
listViewsHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let listViewsRequest = ListViewsRequest {}
  resp <- hstreamApiListViews (ClientNormalRequest listViewsRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse ListViewsResponse{listViewsResponseViews = views} _meta1 _meta2 _status _details -> return $ V.toList $ V.map viewToViewBO views
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

deleteViewHandler :: Client -> String -> Handler Bool
deleteViewHandler hClient vid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let deleteViewRequest = DeleteViewRequest { deleteViewRequestViewId = TL.pack vid }
  resp <- hstreamApiDeleteView (ClientNormalRequest deleteViewRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False
    _ -> return False

getViewHandler :: Client -> String -> Handler (Maybe ViewBO)
getViewHandler hClient vid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let getViewRequest = GetViewRequest { getViewRequestViewId = TL.pack vid }
  resp <- hstreamApiGetView (ClientNormalRequest getViewRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return $ Just $ viewToViewBO x
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> return Nothing
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing
    _ -> return Nothing

viewServer :: Client -> Server ViewsAPI
viewServer hClient =
  (listViewsHandler hClient)
  :<|> (createViewHandler hClient)
  :<|> (deleteViewHandler hClient)
  :<|> (getViewHandler hClient)
