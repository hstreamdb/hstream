{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunConnectorSpec (spec) where

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

getConnectorResponseIdIs :: TL.Text -> GetConnectorResponse -> Bool
getConnectorResponseIdIs targetId (GetConnectorResponse connectorId _ _ _ _) = connectorId == targetId

createSinkConnector :: TL.Text -> IO (Maybe CreateSinkConnectorResponse)
createSinkConnector sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let createSinkConnectorRequest = CreateSinkConnectorRequest { createSinkConnectorRequestSql = sql }
  resp <- hstreamApiCreateSinkConnector (ClientNormalRequest createSinkConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@CreateSinkConnectorResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

successCreateConnectorResp :: CreateSinkConnectorResponse
successCreateConnectorResp = CreateSinkConnectorResponse
  { createSinkConnectorResponseSuccess = True
  }

listConnector :: IO (Maybe ListConnectorResponse)
listConnector = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let listConnectorRequest = ListConnectorRequest {}
  resp <- hstreamApiListConnector (ClientNormalRequest listConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@ListConnectorResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

getConnector :: TL.Text -> IO (Maybe GetConnectorResponse)
getConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let getConnectorRequest = GetConnectorRequest { getConnectorRequestId = connectorId }
  resp <- hstreamApiGetConnector (ClientNormalRequest getConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@GetConnectorResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

deleteConnector :: TL.Text -> IO (Maybe DeleteConnectorResponse)
deleteConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let deleteConnectorRequest = DeleteConnectorRequest { deleteConnectorRequestId = connectorId }
  resp <- hstreamApiDeleteConnector (ClientNormalRequest deleteConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@DeleteConnectorResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

cancelConnector :: TL.Text -> IO (Maybe CancelConnectorResponse)
cancelConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let cancelConnectorRequest = CancelConnectorRequest { cancelConnectorRequestId = connectorId }
  resp <- hstreamApiCancelConnector (ClientNormalRequest cancelConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@CancelConnectorResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

restartConnector :: TL.Text -> IO (Maybe RestartConnectorResponse)
restartConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let restartConnectorRequest = RestartConnectorRequest { restartConnectorRequestId = connectorId }
  resp <- hstreamApiRestartConnector (ClientNormalRequest restartConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@RestartConnectorResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

spec :: Spec
spec = describe "HStream.RunConnectorSpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  let mysqlConnector = "mysql"

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM " <> source1 <> " IF EXISTS ;"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just successResp)

  it "create streams" $
    ( do
        res1 <- executeCommandQuery $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just successResp)

  it "create mysql sink connector" $
    ( do
        createSinkConnector ("CREATE SINK CONNECTOR " <> mysqlConnector <> " WITH (type = mysql, host = \"127.0.0.1\", stream = " <> source1 <> ");")
    ) `shouldReturn` Just successCreateConnectorResp

  it "list connectors" $
    ( do
        Just ListConnectorResponse {listConnectorResponseResponses = connectors} <- listConnector
        let record = V.find (getConnectorResponseIdIs mysqlConnector) connectors
        case record of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "get connector" $
    ( do
        connector <- getConnector mysqlConnector
        case connector of
          Just _ -> return True
          _      -> return False
    ) `shouldReturn` True

  it "cancel connector" $
    ( do
        _ <- cancelConnector mysqlConnector
        connector <- getConnector mysqlConnector
        case connector of
          Just (GetConnectorResponse _ 2 _ _ _) -> return True
          _                                     -> return False
    ) `shouldReturn` True

  it "restart connector" $
    ( do
        _ <- restartConnector mysqlConnector
        connector <- getConnector mysqlConnector
        case connector of
          Just (GetConnectorResponse _ 1 _ _ _) -> return True
          _                                     -> return False
    ) `shouldReturn` True

  it "delete connector" $
    ( do
        _ <- cancelConnector mysqlConnector
        _ <- deleteConnector mysqlConnector
        connector <- getConnector mysqlConnector
        case connector of
          Just (GetConnectorResponse _ _ _ _ Enumerated {enumerated = Right HStreamServerErrorNotExistError}) -> return True
          _ -> return False
    ) `shouldReturn` True
