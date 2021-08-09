{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.View where

import qualified Data.ByteString.Char8            as BSC
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.Text                      as ZT

import qualified HStream.Connector.HStore         as HCH
import qualified HStream.SQL.Codegen              as HSC
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   dropHelper,
                                                   handleCreateAsSelect)
import qualified HStream.Server.Persistence       as P
import qualified HStream.Store                    as HS
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (cBytesToLazyText,
                                                   returnErrResp, returnResp,
                                                   textToCBytes)

hstreamQueryToView :: P.PersistentQuery -> View
hstreamQueryToView (P.PersistentQuery queryId sqlStatement createdTime (P.ViewQuery _ _ schema) status _) =
  View { viewViewId = cBytesToLazyText queryId
       , viewStatus = fromIntegral $ fromEnum status
       , viewCreatedTime = createdTime
       , viewSql = TL.pack $ ZT.unpack sqlStatement
       , viewSchema = V.fromList $ TL.pack <$> schema
       }
hstreamQueryToView _ = error "Impossible happened..."

createViewHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateViewRequest View
  -> IO (ServerResponse 'Normal View)
createViewHandler sc@ServerContext{..} (ServerNormalRequest _ CreateViewRequest{..}) = defaultExceptionHandle $ do
  plan <- HSC.streamCodegen $ TL.toStrict createViewRequestSql
  case plan of
    HSC.CreateViewPlan schema sources sink taskBuilder _repFactor _ -> do
      create sink
      (qid, timestamp) <- handleCreateAsSelect sc taskBuilder createViewRequestSql (P.ViewQuery (textToCBytes <$> sources) (textToCBytes sink) schema) False
      returnResp $ View { viewViewId = cBytesToLazyText qid
                        , viewStatus = fromIntegral $ fromEnum P.Running
                        , viewCreatedTime = timestamp
                        , viewSql = createViewRequestSql
                        , viewSchema = V.fromList $ TL.pack <$> schema
                        }
    _ -> returnErrResp StatusInternal (StatusDetails $ BSC.pack "inconsistent method called")
  where
    mkLogAttrs = HS.LogAttrs . HS.HsLogAttrs scDefaultStreamRepFactor
    create sName = HS.createStream scLDClient (HCH.transToViewStreamName sName) (mkLogAttrs Map.empty)

listViewsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListViewsRequest ListViewsResponse
  -> IO (ServerResponse 'Normal ListViewsResponse)
listViewsHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  queries <- P.withMaybeZHandle zkHandle P.getQueries
  let records = map hstreamQueryToView $ filter P.isViewQuery queries
  let resp = ListViewsResponse . V.fromList $ records
  returnResp resp

getViewHandler
  :: ServerContext
  -> ServerRequest 'Normal GetViewRequest View
  -> IO (ServerResponse 'Normal View)
getViewHandler ServerContext{..} (ServerNormalRequest _metadata GetViewRequest{..}) = do
  query <- do
    viewQueries <- filter P.isViewQuery <$> P.withMaybeZHandle zkHandle P.getQueries
    return $
      find (\P.PersistentQuery {..} -> cBytesToLazyText queryId == getViewRequestViewId) viewQueries
  case query of
    Just q -> returnResp $ hstreamQueryToView q
    _      -> returnErrResp StatusInternal "View does not exist"

deleteViewHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteViewRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteViewHandler sc (ServerNormalRequest _metadata DeleteViewRequest{..}) = defaultExceptionHandle $ do
    let name = TL.toStrict deleteViewRequestViewId
    dropHelper sc name False True
