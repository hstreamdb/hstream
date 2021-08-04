{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.View where

import           Control.Monad                    (void)
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (atomicModifyIORef')
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (HasDefault (def))

import qualified HStream.Connector.HStore         as HCH
import           HStream.Processing.Stream        (build)
import           HStream.SQL                      (RSQL (..), RSel (..),
                                                   RSelect (..),
                                                   genStreamBuilderWithStream,
                                                   genTaskName, parseAndRefine)
import           HStream.SQL.Exception            (SomeSQLException (ParseException),
                                                   throwSQLException)
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   groupbyStores,
                                                   runUnderlyingSelect)
import qualified HStream.Server.Persistence       as P
import qualified HStream.Store                    as HS
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (lazyTextToCBytes, returnResp,
                                                   textToCBytes)


createViewHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateViewRequest CreateViewResponse
  -> IO (ServerResponse 'Normal CreateViewResponse)
createViewHandler sc@ServerContext{..} (ServerNormalRequest _ CreateViewRequest{..}) = defaultExceptionHandle $ do
  let selectStatement =  TL.toStrict createViewRequestSql
      viewName = TL.toStrict createViewRequestViewName
  RQSelect select@(RSelect sel _ _ _ _) <- parseAndRefine selectStatement
  tName <- genTaskName
  (builder, source, sink, Just mat) <- genStreamBuilderWithStream tName (Just viewName) select
  let schema = case sel of
        RSelAsterisk    -> throwSQLException ParseException Nothing "Fields has to be specified"
        RSelList fields -> map snd fields
  createViewStream sink
  void $ runUnderlyingSelect sc (build builder) createViewRequestSql
    (P.ViewQuery (textToCBytes <$> source) (textToCBytes sink) schema) False
  atomicModifyIORef' groupbyStores (\hm -> (HM.insert sink mat hm, ()))
  returnResp def
    { createViewResponseViewName = TL.fromStrict sink
    , createViewResponseQueryId  = TL.fromStrict tName }
  where
    createViewStream vName = HS.createStream scLDClient (HCH.transToViewStreamName vName)
      (HS.LogAttrs $ HS.HsLogAttrs scDefaultStreamRepFactor Map.empty)

listViewsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListViewsRequest ListViewsResponse
  -> IO (ServerResponse 'Normal ListViewsResponse)
listViewsHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  views <- HS.findStreams scLDClient HS.StreamTypeView True
  let records = TL.pack . HS.showStreamName <$> views
  let resp = ListViewsResponse . V.fromList $ records
  returnResp resp

deleteViewHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteViewRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteViewHandler ServerContext{..} (ServerNormalRequest _metadata DeleteViewRequest{..}) =
  defaultExceptionHandle $ do
    P.withMaybeZHandle zkHandle $
      P.removeQuery' (lazyTextToCBytes deleteViewRequestViewName)
    returnResp Empty
