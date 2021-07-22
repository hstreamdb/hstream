{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunQuerySpec (spec) where

import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Test.Hspec

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger

getQueryResponseIdIs :: TL.Text -> Query -> Bool
getQueryResponseIdIs targetId (Query queryId _ _ _) = queryId == targetId

createQuery :: TL.Text -> TL.Text -> IO (Maybe Query)
createQuery qid sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let createQueryRequest = CreateQueryRequest { createQueryRequestId = qid
                                              , createQueryRequestQueryText = sql
                                              }
  resp <- hstreamApiCreateQuery (ClientNormalRequest createQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@Query{} _meta1 _meta2 StatusOk _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Create Query Client Error: " <> show clientError
      return Nothing
    _ -> return Nothing


listQueries :: IO (Maybe ListQueriesResponse)
listQueries = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let listQueryRequesies = ListQueriesRequest {}
  resp <- hstreamApiListQueries (ClientNormalRequest listQueryRequesies 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@ListQueriesResponse{} _meta1 _meta2 StatusOk _details -> do
      return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "List Queries Client Error: " <> show clientError
      return Nothing
    _ -> return Nothing

getQuery :: TL.Text -> IO (Maybe Query)
getQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let getQueryRequest = GetQueryRequest { getQueryRequestId = qid }
  resp <- hstreamApiGetQuery (ClientNormalRequest getQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@Query{} _meta1 _meta2 StatusOk _details -> do
      return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Get Query Client Error: " <> show clientError
      return Nothing
    _ -> return Nothing

deleteQuery :: TL.Text -> IO Bool
deleteQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let deleteQueryRequest = DeleteQueryRequest { deleteQueryRequestId = qid }
  resp <- hstreamApiDeleteQuery (ClientNormalRequest deleteQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Delete Query Client Error: " <> show clientError
      return False
    _ -> return False

cancelQuery :: TL.Text -> IO Bool
cancelQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let cancelQueryRequest = CancelQueryRequest { cancelQueryRequestId = qid }
  resp <- hstreamApiCancelQuery (ClientNormalRequest cancelQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Cancel Query Client Error: " <> show clientError
      return False
    _ -> return False

restartQuery :: TL.Text -> IO Bool
restartQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let restartQueryRequest = RestartQueryRequest { restartQueryRequestId = qid }
  resp <- hstreamApiRestartQuery (ClientNormalRequest restartQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Restart Query Client Error: " <> show clientError
      return False
    _ -> return False

spec :: Spec
spec = describe "HStream.RunQuerySpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  let queryname1 = "testquery1"

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM " <> source1 <> " IF EXISTS;"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just commandQuerySuccessResp)

  it "create streams" $
    ( do
        res1 <- executeCommandQuery $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just commandQuerySuccessResp)

  it "create query" $
    ( do
        res <- createQuery queryname1 ("SELECT * FROM " <> source1 <> " EMIT CHANGES;")
        case res of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "list queries" $
    ( do
        Just ListQueriesResponse {listQueriesResponseQueries = queries} <- listQueries
        let record = V.find (getQueryResponseIdIs queryname1) queries
        case record of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "get query" $
    ( do
        query <- getQuery queryname1
        case query of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "cancel query" $
    ( do
        _ <- cancelQuery queryname1
        query <- getQuery queryname1
        case query of
          Just (Query _ 2 _ _ ) -> return True
          _                     -> return False
    ) `shouldReturn` True

  it "restart query" $
    ( do
        _ <- restartQuery queryname1
        query <- getQuery queryname1
        case query of
          Just (Query _ 1 _ _ ) -> return True
          _                     -> return False
    ) `shouldReturn` True

  it "delete query" $
    ( do
        _ <- cancelQuery queryname1
        _ <- deleteQuery queryname1
        query <- getQuery queryname1
        case query of
          Just Query{} -> return True
          _            -> return False
    ) `shouldReturn` False

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM " <> source1 <> " IF EXISTS;"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just commandQuerySuccessResp)
