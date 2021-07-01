{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunQuerySpec (spec) where

import           Control.Concurrent
import qualified Data.Aeson         as Aeson
import qualified Data.List          as L
import qualified Data.Text.Lazy     as TL
import           Test.Hspec
import           Network.GRPC.HighLevel.Generated
import qualified Data.Vector                      as V

import           HStream.Common
import           HStream.Store
import           HStream.Server.HStreamApi

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
  let getQueryRequest = GetQueryRequest { getQueryRequestId = qid}
  resp <- hstreamApiGetQuery (ClientNormalRequest getQueryRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@GetQueryResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

spec :: Spec
spec = describe "HStream.RunQuerySpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  let query1 = "testquery1"

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

  it "create query" $
    ( do
        createQuery query1 ("SELECT * FROM " <> source1 <> " EMIT CHANGES;")
    ) `shouldReturn` Just successCreateQueryResp

  it "fetch queries" $
    ( do
        Just FetchQueryResponse {fetchQueryResponseResponses = queries} <- fetchQuery 
        let record = V.find (getQueryResponseIdIs query1) queries
        case record of 
          Just _ -> return True
          _ -> return False
    ) `shouldReturn` True

  it "get query" $
    ( do
        query <- getQuery query1 
        case query of 
          Just _ -> return True
          _ -> return False
    ) `shouldReturn` True
