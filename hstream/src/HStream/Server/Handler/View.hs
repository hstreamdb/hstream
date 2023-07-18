{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
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
  , executeViewQueryHandler
  , executeViewQueryWithNamespaceHandler
    -- * For hs-grpc-server
  , handleListView
  , handleGetView
  , handleDeleteView
  , handleExecuteViewQuery
  , handleExecuteViewQueryWithNamespace
  ) where

import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Logger                   as Log
#ifdef HStreamEnableSchema
import qualified HStream.Server.Core.ViewNew      as Core
#else
import qualified HStream.Server.Core.View         as Core
#endif
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (ResourceType (ResView),
                                                   returnResp,
                                                   validateNameAndThrow)

listViewsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListViewsRequest ListViewsResponse
  -> IO (ServerResponse 'Normal ListViewsResponse)
listViewsHandler serverContext (ServerNormalRequest _metadata _) = defaultExceptionHandle $ do
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
           <> "View ID:" <> Log.build getViewRequestViewId
  validateNameAndThrow ResView getViewRequestViewId
  returnResp =<< Core.getView sc getViewRequestViewId

handleGetView :: ServerContext -> G.UnaryHandler GetViewRequest View
handleGetView sc _ GetViewRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Get View Request. "
           <> "View ID:" <> Log.build getViewRequestViewId
  validateNameAndThrow ResView getViewRequestViewId
  Core.getView sc getViewRequestViewId

deleteViewHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteViewRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteViewHandler sc (ServerNormalRequest _metadata DeleteViewRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete View Request. "
           <> "View ID:" <> Log.buildString (T.unpack deleteViewRequestViewId)
  validateNameAndThrow ResView deleteViewRequestViewId
  returnResp =<< Core.deleteView sc deleteViewRequestViewId deleteViewRequestIgnoreNonExist

handleDeleteView :: ServerContext -> G.UnaryHandler DeleteViewRequest Empty
handleDeleteView sc _ DeleteViewRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Delete View Request. "
           <> "View ID:" <> Log.buildString (T.unpack deleteViewRequestViewId)
  validateNameAndThrow ResView deleteViewRequestViewId
  Core.deleteView sc deleteViewRequestViewId deleteViewRequestIgnoreNonExist

executeViewQueryHandler
  :: ServerContext -> ServerRequest 'Normal ExecuteViewQueryRequest ExecuteViewQueryResponse
  -> IO (ServerResponse 'Normal ExecuteViewQueryResponse)
executeViewQueryHandler sc (ServerNormalRequest _metadata ExecuteViewQueryRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Execute View Query Request. "
           <> "SQL Statement:" <> Log.build executeViewQueryRequestSql
  returnResp . ExecuteViewQueryResponse =<< Core.executeViewQuery sc executeViewQueryRequestSql

handleExecuteViewQuery
  :: ServerContext -> G.UnaryHandler ExecuteViewQueryRequest ExecuteViewQueryResponse
handleExecuteViewQuery sc _ ExecuteViewQueryRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Execute View Query Request. "
           <> "SQL Statement:" <> Log.build executeViewQueryRequestSql
  ExecuteViewQueryResponse <$> Core.executeViewQuery sc executeViewQueryRequestSql

executeViewQueryWithNamespaceHandler
  :: ServerContext -> ServerRequest 'Normal ExecuteViewQueryWithNamespaceRequest ExecuteViewQueryResponse
  -> IO (ServerResponse 'Normal ExecuteViewQueryResponse)
executeViewQueryWithNamespaceHandler sc (ServerNormalRequest _metadata ExecuteViewQueryWithNamespaceRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Execute View Query Request. "
           <> "SQL Statement:" <> Log.build executeViewQueryWithNamespaceRequestSql
  returnResp . ExecuteViewQueryResponse =<<
    Core.executeViewQueryWithNamespace sc executeViewQueryWithNamespaceRequestSql
                                          executeViewQueryWithNamespaceRequestNamespace

handleExecuteViewQueryWithNamespace
  :: ServerContext -> G.UnaryHandler ExecuteViewQueryWithNamespaceRequest ExecuteViewQueryResponse
handleExecuteViewQueryWithNamespace sc _ ExecuteViewQueryWithNamespaceRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Execute View Query Request. "
           <> "SQL Statement:" <> Log.build executeViewQueryWithNamespaceRequestSql
  ExecuteViewQueryResponse <$>
    Core.executeViewQueryWithNamespace sc executeViewQueryWithNamespaceRequestSql
                                          executeViewQueryWithNamespaceRequestNamespace
