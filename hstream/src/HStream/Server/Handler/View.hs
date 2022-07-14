{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.View where

import qualified Data.ByteString.Char8            as BSC
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (atomicModifyIORef')
import           Data.List                        (find)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           Control.Monad                    (void)
import qualified Data.Map.Strict                  as Map
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.View         as CoreView
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.Handler.Common    (handleCreateAsSelect)
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import qualified HStream.Server.Shard             as SD
import           HStream.Server.Types
import qualified HStream.SQL.Codegen              as HSC
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (TaskStatus (..),
                                                   cBytesToText, returnErrResp,
                                                   returnResp, textToCBytes)

createViewHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateViewRequest View
  -> IO (ServerResponse 'Normal View)
createViewHandler sc@ServerContext{..} (ServerNormalRequest _ CreateViewRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create View Request: " <> Log.buildString (T.unpack createViewRequestSql)
  plan <- HSC.streamCodegen createViewRequestSql
  case plan of
    HSC.CreateViewPlan schema sources sink taskBuilder _repFactor materialized -> do
      create (transToStreamName sink)
      (qid, timestamp) <- handleCreateAsSelect sc taskBuilder createViewRequestSql (P.ViewQuery (textToCBytes <$> sources) (textToCBytes sink) schema) S.StreamTypeView
      atomicModifyIORef' P.groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
      returnResp $ View { viewViewId = cBytesToText qid
                        , viewStatus = getPBStatus Running
                        , viewCreatedTime = timestamp
                        , viewSql = createViewRequestSql
                        , viewSchema = V.fromList $ T.pack <$> schema
                        }
    _ -> returnErrResp StatusInvalidArgument (StatusDetails $ BSC.pack "inconsistent method called")
  where
    attrs = (S.def{ S.logReplicationFactor = S.defAttr1 scDefaultStreamRepFactor })
    create sName = do
      S.createStream scLDClient sName attrs
      let extrAttr = Map.fromList [(SD.shardStartKey, SD.keyToCBytes minBound), (SD.shardEndKey, SD.keyToCBytes maxBound), (SD.shardEpoch, "1")]
      void $ S.createStreamPartitionWithExtrAttr scLDClient sName (Just "view") extrAttr

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
    viewQueries <- filter P.isViewQuery <$> P.getQueries zkHandle
    return $
      find (\P.PersistentQuery {..} -> cBytesToText queryId == getViewRequestViewId) viewQueries
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
