{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunQuerySpec (spec) where

import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Test.Hspec

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger             (pattern C_DBG_ERROR,
                                                   setLogDeviceDbgLevel)
import           HStream.Utils                    (TaskStatus (..),
                                                   setupSigsegvHandler)

getQueryResponseIdIs :: T.Text -> Query -> Bool
getQueryResponseIdIs targetId (Query queryId _ _ _) = queryId == targetId

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

getQuery :: T.Text -> IO (Maybe Query)
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

deleteQuery :: T.Text -> IO Bool
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

terminateQuery :: T.Text -> IO Bool
terminateQuery qid = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let terminateQueryRequest = TerminateQueriesRequest { terminateQueriesRequestQueryId = V.singleton qid,
                                                        terminateQueriesRequestAll = False }
  resp <- hstreamApiTerminateQueries (ClientNormalRequest terminateQueryRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Cancel Query Client Error: " <> show clientError
      return False
    _ -> return False

restartQuery :: T.Text -> IO Bool
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
spec = aroundAll provideHstreamApi $
  describe "HStream.RunQuerySpec" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  source1 <- runIO $ do xs <- newRandomText 20; pure ("`" <> xs <> "_-xx.024`")
  source2 <- runIO $ do xs <- newRandomText 20; pure ("`" <> xs <> "_xx-.210`")

  let sql = "CREATE STREAM " <> source2 <> " AS SELECT * FROM " <> source1 <> " EMIT CHANGES;"

  it "clean streams" $ \api -> do
    runDropSql api $ "DROP STREAM " <> source1 <> " IF EXISTS;"

  it "create streams" $ \api ->
    runCreateStreamSql api $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"

  it "run a query" $ \api ->
    runCreateWithSelectSql api sql

  it "list queries" $ \_ ->
    ( do
        Just ListQueriesResponse {listQueriesResponseQueries = queries} <- listQueries
        return $ V.null queries
    ) `shouldReturn` False

  it "get query" $ \_ ->
    ( do
        Just ListQueriesResponse {listQueriesResponseQueries = queries} <- listQueries
        let (Just thisQuery) = V.find (\x -> queryQueryText x == sql) queries
        query <- getQuery (queryId thisQuery)
        case query of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "terminate query" $ \_ ->
    ( do
        Just ListQueriesResponse {listQueriesResponseQueries = queries} <- listQueries
        let (Just thisQuery) = V.find (\x -> queryQueryText x == sql) queries
        _ <- terminateQuery (queryId thisQuery)
        query <- getQuery (queryId thisQuery)
        let terminated = getPBStatus Terminated
        case query of
          Just (Query _ status _ _ ) -> return (status == terminated)
          _                          -> return False
    ) `shouldReturn` True

  it "delete query" $ \_ ->
    ( do
        Just ListQueriesResponse {listQueriesResponseQueries = queries} <- listQueries
        let (Just thisQuery) = V.find (\x -> queryQueryText x == sql) queries
        _ <- terminateQuery (queryId thisQuery)
        _ <- deleteQuery (queryId thisQuery)
        query <- getQuery (queryId thisQuery)
        case query of
          Just Query{} -> return True
          _            -> return False
    ) `shouldReturn` False

  it "clean streams" $ \api -> do
    runDropSql api $ "DROP STREAM " <> source1 <> " IF EXISTS;"
