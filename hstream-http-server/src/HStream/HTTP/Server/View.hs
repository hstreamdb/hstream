{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.View (
  ViewsAPI, viewServer, listViewsHandler, ViewBO(..)
) where

import           Control.Monad                (void)
import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int64)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Vector                  as V
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Delete, Get, JSON, Post,
                                               ReqBody, type (:>), (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils
import qualified HStream.Logger               as Log
import           HStream.Server.HStreamApi
import           HStream.Utils                (TaskStatus (..))

-- BO is short for Business Object
data ViewBO = ViewBO
  { id          :: Maybe T.Text
  , status      :: Maybe TaskStatus
  , createdTime :: Maybe Int64
  , sql         :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON   ViewBO
instance FromJSON ViewBO
instance ToSchema ViewBO

viewToViewBO :: View -> ViewBO
viewToViewBO (View id' status createdTime sql _) =
  ViewBO (Just id') (Just . TaskStatus $ status) (Just createdTime) sql

type ViewsAPI
  =    "views" :> Get '[JSON] [ViewBO]
  :<|> "views" :> ReqBody '[JSON] ViewBO :> Post '[JSON] ViewBO
  :<|> "views" :> Capture "name" String :> Delete '[JSON] ()
  :<|> "views" :> Capture "name" String :> Get '[JSON] ViewBO

createViewHandler :: Client -> ViewBO -> Handler ViewBO
createViewHandler hClient (ViewBO _ _ _ sql) = do
  resp <- liftIO $ do
    Log.debug $ "Send create view request to HStream server. "
             <> "SQL Statement: " <> Log.buildText sql
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiCreateView $ mkClientNormalRequest def
      { createViewRequestSql = T.pack $ T.unpack sql }
  viewToViewBO <$> getServerResp' resp

listViewsHandler :: Client -> Handler [ViewBO]
listViewsHandler hClient = do
  resp <- liftIO $ do
    Log.debug "Send list views request to HStream server. "
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiListViews $ mkClientNormalRequest def
  V.toList . V.map viewToViewBO . listViewsResponseViews <$> getServerResp' resp

deleteViewHandler :: Client -> String -> Handler ()
deleteViewHandler hClient vid = do
  resp <- liftIO $ do
    Log.debug $ "Send delete view request to HStream server. "
             <> "View ID: " <> Log.buildString vid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiDeleteView $ mkClientNormalRequest def
      { deleteViewRequestViewId = T.pack vid }
  void $ getServerResp' resp

getViewHandler :: Client -> String -> Handler ViewBO
getViewHandler hClient vid = do
  resp <- liftIO $ do
    Log.debug $ "Send get view request to HStream server. "
             <> "View ID: " <> Log.buildString vid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiGetView $ mkClientNormalRequest def
      { getViewRequestViewId = T.pack vid }
  viewToViewBO <$> getServerResp' resp

viewServer :: Client -> Server ViewsAPI
viewServer hClient = listViewsHandler  hClient
                :<|> createViewHandler hClient
                :<|> deleteViewHandler hClient
                :<|> getViewHandler    hClient
