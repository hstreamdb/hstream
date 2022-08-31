{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.View where

import           Data.List                        (find)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.Types          as M
import qualified HStream.Server.Core.View         as CoreView
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData          as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (returnErrResp, returnResp)

listViewsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListViewsRequest ListViewsResponse
  -> IO (ServerResponse 'Normal ListViewsResponse)
listViewsHandler serverContext (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List View Request"
  CoreView.listViews serverContext >>= returnResp . ListViewsResponse . V.fromList

getViewHandler
  :: ServerContext
  -> ServerRequest 'Normal GetViewRequest View
  -> IO (ServerResponse 'Normal View)
getViewHandler ServerContext{..} (ServerNormalRequest _metadata GetViewRequest{..}) = do
  Log.debug $ "Receive Get View Request. "
    <> "View ID:" <> Log.buildString (T.unpack getViewRequestViewId)
  query <- do
    viewQueries <- filter P.isViewQuery <$> M.listMeta zkHandle
    return $
      find (\P.PersistentQuery {..} -> queryId == getViewRequestViewId) viewQueries
  case query of
    Just q -> returnResp $ CoreView.hstreamQueryToView q
    _      -> do
      Log.warning $ "Cannot Find View with ID: " <> Log.buildString (T.unpack getViewRequestViewId)
      returnErrResp StatusNotFound "View does not exist"

deleteViewHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteViewRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteViewHandler sc (ServerNormalRequest _metadata DeleteViewRequest{..}) = defaultExceptionHandle $ do
    Log.debug $ "Receive Delete View Request. "
             <> "View ID:" <> Log.buildString (T.unpack deleteViewRequestViewId)
    returnResp =<< CoreView.deleteView sc deleteViewRequestViewId deleteViewRequestIgnoreNonExist
