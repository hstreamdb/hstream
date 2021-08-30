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

import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int64)
import           Data.Maybe                   (isJust)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Text.Lazy               as TL
import qualified Data.Vector                  as V
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Delete, Get, JSON, Post,
                                               ReqBody, type (:>), (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils    (getServerResp,
                                               mkClientNormalRequest)
import           HStream.Server.HStreamApi
import           HStream.Utils                (TaskStatus (..))

-- BO is short for Business Object
data ViewBO = ViewBO
  { id          :: Maybe T.Text
  , status      :: Maybe TaskStatus
  , createdTime :: Maybe Int64
  , sql         :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON ViewBO
instance FromJSON ViewBO
instance ToSchema ViewBO

type ViewsAPI =
  "views" :> Get '[JSON] [ViewBO]
  :<|> "views" :> ReqBody '[JSON] ViewBO :> Post '[JSON] ViewBO
  :<|> "views" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "views" :> Capture "name" String :> Get '[JSON] (Maybe ViewBO)

viewToViewBO :: View -> ViewBO
viewToViewBO (View id' status createdTime sql _) =
  ViewBO (Just $ TL.toStrict id') (Just . TaskStatus $ status) (Just createdTime) (TL.toStrict sql)

createViewHandler :: Client -> ViewBO -> Handler ViewBO
createViewHandler hClient (ViewBO _ _ _ sql) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiCreateView
    (mkClientNormalRequest def {createViewRequestSql = TL.pack $ T.unpack sql})
  maybe (ViewBO Nothing Nothing Nothing sql) viewToViewBO <$> getServerResp resp

listViewsHandler :: Client -> Handler [ViewBO]
listViewsHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiListViews (mkClientNormalRequest def)
  maybe [] (V.toList . V.map viewToViewBO . listViewsResponseViews)
    <$> getServerResp resp

deleteViewHandler :: Client -> String -> Handler Bool
deleteViewHandler hClient vid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiDeleteView
    (mkClientNormalRequest def { deleteViewRequestViewId = TL.pack vid })
  isJust <$> getServerResp resp

getViewHandler :: Client -> String -> Handler (Maybe ViewBO)
getViewHandler hClient vid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiGetView (mkClientNormalRequest def { getViewRequestViewId = TL.pack vid })
  (viewToViewBO <$>) <$> getServerResp resp

viewServer :: Client -> Server ViewsAPI
viewServer hClient =
  listViewsHandler hClient
  :<|> createViewHandler hClient
  :<|> deleteViewHandler hClient
  :<|> getViewHandler hClient
