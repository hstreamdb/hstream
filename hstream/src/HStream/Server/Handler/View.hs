{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.View where

import           Control.Exception                (SomeException, catch, try)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           ThirdParty.Google.Protobuf.Empty
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log

import qualified HStream.Connector.HStore         as HCH
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import           HStream.Server.Exception         (LowLevelStoreException (..))
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   handleCreateAsSelect, mark)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils.Converter          (cbytesToText, textToCBytes)

hstreamQueryToView :: HSP.Query -> View
hstreamQueryToView (HSP.Query queryId (HSP.Info sqlStatement createdTime) (HSP.ViewQuery _ _ schema) (HSP.Status status _)) =
  View (TL.pack $ ZDC.unpack queryId) (fromIntegral $ fromEnum status) createdTime (TL.pack $ ZT.unpack sqlStatement) (V.fromList $ TL.pack <$> schema)
hstreamQueryToView _ = emptyView

emptyView :: View
emptyView = View "" 0 0 "" []

hstreamViewIdIs :: T.Text -> HSP.Query -> Bool
hstreamViewIdIs name (HSP.Query queryId _ _ _) = (cbytesToText queryId) == name

createViewHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateViewRequest View
  -> IO (ServerResponse 'Normal View)
createViewHandler sc@ServerContext{..} (ServerNormalRequest _ CreateViewRequest{..}) = do
  plan' <- try $ HSC.streamCodegen $ (TL.toStrict createViewRequestSql)
  err <- case plan' of
    Left  (_ :: SomeSQLException) -> return $ Left "exception on parsing or codegen"
    Right (HSC.CreateViewPlan schema sources sink taskBuilder _repFactor _) -> mark LowLevelStoreException $ do
      create sink
      (qid, timestamp) <- handleCreateAsSelect sc taskBuilder createViewRequestSql (HSP.ViewQuery (textToCBytes <$> sources) (ZDC.pack . T.unpack $ sink) schema)
      return $ Right $ View (TL.pack $ ZDC.unpack qid) (fromIntegral $ fromEnum HSP.Running) timestamp createViewRequestSql (V.fromList $ TL.pack <$> schema)
    Right _ -> return $ Left "inconsistent method called"
  case err of
    Left err' -> do
      Log.fatal . string8 $ err'
      return (ServerNormalResponse Nothing [] StatusInternal "failed")
    Right view  -> return (ServerNormalResponse (Just view) [] StatusOk  "")
  where
    mkLogAttrs = HS.LogAttrs . HS.HsLogAttrs scDefaultStreamRepFactor
    create sName = HS.createStream scLDClient (HCH.transToStreamName sName) (mkLogAttrs Map.empty)

listViewsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListViewsRequest ListViewsResponse
  -> IO (ServerResponse 'Normal ListViewsResponse)
listViewsHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
  let records = map hstreamQueryToView queries
  let resp = ListViewsResponse . V.fromList $ records
  return (ServerNormalResponse (Just resp) [] StatusOk "")

getViewHandler
  :: ServerContext
  -> ServerRequest 'Normal GetViewRequest View
  -> IO (ServerResponse 'Normal View)
getViewHandler ServerContext{..} (ServerNormalRequest _metadata GetViewRequest{..}) = do
  query <- do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    return $ find (hstreamViewIdIs (T.pack $ TL.unpack getViewRequestViewId)) queries
  case query of
        Just q -> return (ServerNormalResponse (Just (hstreamQueryToView q)) [] StatusOk "")
        _      ->  return (ServerNormalResponse Nothing [] StatusInternal "Not exists")

deleteViewHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteViewRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteViewHandler ServerContext{..} (ServerNormalRequest _metadata DeleteViewRequest{..}) = do
  catch
    ((HSP.withMaybeZHandle zkHandle $ HSP.removeQuery' (ZDC.pack $ TL.unpack deleteViewRequestViewId) False) >> return (ServerNormalResponse (Just Empty) [] StatusOk ""))
    (\(_ :: SomeException) -> return (ServerNormalResponse Nothing [] StatusInternal "Failed"))
