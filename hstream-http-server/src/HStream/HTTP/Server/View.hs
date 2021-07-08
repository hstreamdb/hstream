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
  ViewsAPI, viewServer
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

import           HStream.Server.HStreamApi

-- BO is short for Business Object
data ViewBO = ViewBO
  { id          :: Maybe T.Text
  , status      :: Maybe Int
  , createdTime :: Maybe Int64
  , sql         :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON ViewBO
instance FromJSON ViewBO
instance ToSchema ViewBO

type ViewsAPI =
  "views" :> Get '[JSON] [ViewBO]
  :<|> "views" :> ReqBody '[JSON] ViewBO :> Post '[JSON] ViewBO

viewToViewBO :: View -> ViewBO
viewToViewBO (View id status createdTime sql _) =
  ViewBO (Just $ TL.toStrict id) (Just $ fromIntegral status) (Just createdTime) (TL.toStrict sql)

-- TODO: we should remove the duplicate code in HStream/Admin/Server/View.hs and HStream/Server/Handler.hs
createViewHandler :: Client -> ViewBO -> Handler ViewBO
createViewHandler hClient (ViewBO _ _ _ sql) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createViewRequest = CreateViewRequest { createViewRequestSql = TL.pack $ T.unpack sql }
  resp <- hstreamApiCreateView (ClientNormalRequest createViewRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@View{} _meta1 _meta2 _status _details -> return $ ViewBO Nothing Nothing Nothing sql
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ ViewBO Nothing Nothing Nothing sql

listViewHandler :: Client -> Handler [ViewBO]
listViewHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let listViewsRequest = ListViewsRequest {}
  resp <- hstreamApiListViews (ClientNormalRequest listViewsRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@ListViewsResponse{} _meta1 _meta2 _status _details -> do
      case x of
        ListViewsResponse {listViewsResponseViews = views} -> do
          return $ V.toList $ V.map viewToViewBO views
        _ -> return []
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

viewServer :: Client -> Server ViewsAPI
viewServer hClient =
  (listViewHandler hClient)
  :<|> (createViewHandler hClient)
