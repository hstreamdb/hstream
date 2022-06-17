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

  source1 <- runIO $ newRandomText 20
  let queryname1 = "testquery1"

  it "clean streams" $ \api -> do
    runDropSql api $ "DROP STREAM " <> source1 <> " IF EXISTS;"

  it "create streams" $ \api ->
    runCreateStreamSql api $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"

  it "list queries" $ \_ ->
    ( do
        Just ListQueriesResponse {listQueriesResponseQueries = queries} <- listQueries
        let record = V.find (getQueryResponseIdIs queryname1) queries
        case record of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "get query" $ \_ ->
    ( do
        query <- getQuery queryname1
        case query of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "terminate query" $ \_ ->
    ( do
        _ <- terminateQuery queryname1
        query <- getQuery queryname1
        let terminated = getPBStatus Terminated
        case query of
          Just (Query _ status _ _ ) -> return (status == terminated)
          _                          -> return False
    ) `shouldReturn` True

  -- it "restart query" $ \_ ->
  --   ( do
  --       _ <- restartQuery queryname1
  --       query <- getQuery queryname1
  --       case query of
  --         Just (Query _ P.Running _ _ ) -> return True
  --         _                             -> return False
  --   ) `shouldReturn` True

  it "delete query" $ \_ ->
    ( do
        _ <- terminateQuery queryname1
        _ <- deleteQuery queryname1
        query <- getQuery queryname1
        case query of
          Just Query{} -> return True
          _            -> return False
    ) `shouldReturn` False

  it "clean streams" $ \api -> do
    runDropSql api $ "DROP STREAM " <> source1 <> " IF EXISTS;"
