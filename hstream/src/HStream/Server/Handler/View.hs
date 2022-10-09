{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.View
  ( -- * For grpc-haskell
    listViewsHandler
  , getViewHandler
  , deleteViewHandler
    -- * For hs-grpc-server
  , handleListView
  , handleGetView
  , handleDeleteView
  ) where

import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.View         as Core
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (returnResp)

listViewsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListViewsRequest ListViewsResponse
  -> IO (ServerResponse 'Normal ListViewsResponse)
listViewsHandler serverContext (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List View Request"
  Core.listViews serverContext >>= returnResp . ListViewsResponse . V.fromList

handleListView :: ServerContext -> G.UnaryHandler ListViewsRequest ListViewsResponse
handleListView sc _ _ = catchDefaultEx $
  ListViewsResponse . V.fromList <$> Core.listViews sc

getViewHandler
  :: ServerContext
  -> ServerRequest 'Normal GetViewRequest View
  -> IO (ServerResponse 'Normal View)
getViewHandler sc (ServerNormalRequest _metadata GetViewRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Get View Request. "
           <> "View ID:" <> Log.buildString (T.unpack getViewRequestViewId)
  returnResp =<< Core.getView sc getViewRequestViewId

handleGetView :: ServerContext -> G.UnaryHandler GetViewRequest View
handleGetView sc _ GetViewRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Get View Request. "
           <> "View ID:" <> Log.buildString (T.unpack getViewRequestViewId)
  Core.getView sc getViewRequestViewId

deleteViewHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteViewRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteViewHandler sc (ServerNormalRequest _metadata DeleteViewRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete View Request. "
           <> "View ID:" <> Log.buildString (T.unpack deleteViewRequestViewId)
  returnResp =<< Core.deleteView sc deleteViewRequestViewId deleteViewRequestIgnoreNonExist

handleDeleteView :: ServerContext -> G.UnaryHandler DeleteViewRequest Empty
handleDeleteView sc _ DeleteViewRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Delete View Request. "
           <> "View ID:" <> Log.buildString (T.unpack deleteViewRequestViewId)
  Core.deleteView sc deleteViewRequestViewId deleteViewRequestIgnoreNonExist
