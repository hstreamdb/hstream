{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunViewSpec (spec) where

import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Test.Hspec
import           ThirdParty.Google.Protobuf.Empty

import           HStream.Common
import           HStream.Server.HStreamApi
import           HStream.Store
import           HStream.Store.Logger

viewIdIs :: TL.Text -> View -> Bool
viewIdIs targetId (View queryId _ _ _ _) = queryId == targetId

createView :: TL.Text -> IO (Maybe View)
createView sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let createViewRequest = CreateViewRequest { createViewRequestSql = sql }
  resp <- hstreamApiCreateView (ClientNormalRequest createViewRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@View{} _meta1 _meta2 StatusOk _details -> return $ Just x
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> do
      putStrLn $ "Internal Error: " <> show _details
      return Nothing
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

listViews :: IO (Maybe ListViewsResponse)
listViews = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let listViewsRequest = ListViewsRequest {}
  resp <- hstreamApiListViews (ClientNormalRequest listViewsRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@ListViewsResponse{} _meta1 _meta2 StatusOk _details -> return $ Just x
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> do
      putStrLn $ "Internal Error: " <> show _details
      return Nothing
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

getView :: TL.Text -> IO (Maybe View)
getView qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let getViewRequest = GetViewRequest { getViewRequestViewId = qid }
  resp <- hstreamApiGetView (ClientNormalRequest getViewRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@View{} _meta1 _meta2 StatusOk _details -> return $ Just x
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> do
      putStrLn $ "Internal Error: " <> show _details
      return Nothing
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

deleteView :: TL.Text -> IO Bool
deleteView qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let deleteViewRequest = DeleteViewRequest { deleteViewRequestViewId = qid }
  resp <- hstreamApiDeleteView (ClientNormalRequest deleteViewRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse Empty _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse Empty _meta1 _meta2 StatusInternal _details -> do
      putStrLn $ "Internal Error: " <> show _details
      return False
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False

spec :: Spec
spec = describe "HStream.RunViewSpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  viewname <- runIO $ TL.fromStrict <$> newRandomText 20

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source1 <> " ;"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just successResp)

  it "create streams" $
    ( do
        res1 <- executeCommandQuery $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just successResp)

  it "test query" $
    ( do
        -- test create view
        viewM <- createView ("CREATE VIEW " <> viewname <> " AS SELECT temperature, SUM(temperature) FROM " <> source1 <> " GROUP BY temperature EMIT CHANGES;")
        case viewM of
          Just view -> do
            -- test list views
            Just ListViewsResponse {listViewsResponseViews = views} <- listViews
            let record = V.find (viewIdIs (viewViewId view)) views
            case record of
              Just _ -> do
                -- test get view
                view' <- getView $ viewViewId view
                case view' of
                  Just _ -> do
                    -- test delete view
                    res <- deleteView $ viewViewId view
                    print res
                    case res of
                      True -> do
                        -- should be deleted
                        view'' <- getView $ viewViewId view
                        case view'' of
                          Just _ -> return False
                          _      -> return True
                      False -> return False
                  _ -> return False
              _ -> return False
          Nothing -> return False
    ) `shouldReturn` True

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source1 <> " ;"
        res2 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> viewname <> " ;"
        return [res1, res2]
    ) `shouldReturn` L.replicate 2 (Just successResp)
