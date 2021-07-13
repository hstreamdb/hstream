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
import           Proto3.Suite                     (Enumerated (..))
import           Test.Hspec

import           HStream.Common
import           HStream.Server.HStreamApi
import           HStream.Store
import           HStream.Store.Logger

getQueryResponseIdIs :: TL.Text -> GetQueryResponse -> Bool
getQueryResponseIdIs targetId (GetQueryResponse queryId _ _ _ _) = queryId == targetId

createQuery :: TL.Text -> TL.Text -> IO (Maybe CreateQueryResponse)
createQuery qid sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let createQueryRequest = CreateQueryRequest { createQueryRequestId = qid
                                              , createQueryRequestQueryText = sql
                                              }
  resp <- hstreamApiCreateQuery (ClientNormalRequest createQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@CreateQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

successCreateQueryResp :: CreateQueryResponse
successCreateQueryResp = CreateQueryResponse
  { createQueryResponseSuccess = True
  }

fetchQuery :: IO (Maybe FetchQueryResponse)
fetchQuery = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let fetchQueryRequest = FetchQueryRequest {}
  resp <- hstreamApiFetchQuery (ClientNormalRequest fetchQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@FetchQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

getQuery :: TL.Text -> IO (Maybe GetQueryResponse)
getQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let getQueryRequest = GetQueryRequest { getQueryRequestId = qid }
  resp <- hstreamApiGetQuery (ClientNormalRequest getQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@GetQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

deleteQuery :: TL.Text -> IO (Maybe DeleteQueryResponse)
deleteQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let deleteQueryRequest = DeleteQueryRequest { deleteQueryRequestId = qid }
  resp <- hstreamApiDeleteQuery (ClientNormalRequest deleteQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@DeleteQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

cancelQuery :: TL.Text -> IO (Maybe CancelQueryResponse)
cancelQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let cancelQueryRequest = CancelQueryRequest { cancelQueryRequestId = qid }
  resp <- hstreamApiCancelQuery (ClientNormalRequest cancelQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@CancelQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

restartQuery :: TL.Text -> IO (Maybe RestartQueryResponse)
restartQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let restartQueryRequest = RestartQueryRequest { restartQueryRequestId = qid }
  resp <- hstreamApiRestartQuery (ClientNormalRequest restartQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@RestartQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

spec :: Spec
spec = describe "HStream.RunQuerySpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  let queryname1 = "testquery1"

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM " <> source1 <> " IF EXISTS;"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just successResp)

  it "create streams" $
    ( do
        res1 <- executeCommandQuery $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just successResp)

  it "create query" $
    ( do
        createQuery queryname1 ("SELECT * FROM " <> source1 <> " EMIT CHANGES;")
    ) `shouldReturn` Just successCreateQueryResp

  it "fetch queries" $
    ( do
        Just FetchQueryResponse {fetchQueryResponseResponses = queries} <- fetchQuery
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
          Just (GetQueryResponse _ 2 _ _ _) -> return True
          _                                 -> return False
    ) `shouldReturn` True

  it "restart query" $
    ( do
        _ <- restartQuery queryname1
        query <- getQuery queryname1
        case query of
          Just (GetQueryResponse _ 1 _ _ _) -> return True
          _                                 -> return False
    ) `shouldReturn` True

  it "delete query" $
    ( do
        _ <- cancelQuery queryname1
        _ <- deleteQuery queryname1
        query <- getQuery queryname1
        case query of
          Just (GetQueryResponse _ _ _ _ Enumerated {enumerated = Right HStreamServerErrorNotExistError}) -> return True
          _ -> return False
    ) `shouldReturn` True
