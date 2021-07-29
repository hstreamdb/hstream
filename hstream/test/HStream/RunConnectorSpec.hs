{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunConnectorSpec (spec) where

import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (isJust)
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Test.Hspec

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger

getConnectorResponseIdIs :: TL.Text -> Connector -> Bool
getConnectorResponseIdIs targetId (Connector connectorId _ _ _ ) = connectorId == targetId

-- TODO: mv to hstream-client library

createSinkConnector :: TL.Text -> IO (Maybe Connector)
createSinkConnector sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let createSinkConnectorRequest = CreateSinkConnectorRequest { createSinkConnectorRequestSql = sql }
  resp <- hstreamApiCreateSinkConnector (ClientNormalRequest createSinkConnectorRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Create Connector Client Error: " <> show clientError
      return Nothing
    _ -> return Nothing

listConnectors :: IO (Maybe ListConnectorsResponse)
listConnectors = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let listConnectorsRequest = ListConnectorsRequest {}
  resp <- hstreamApiListConnectors (ClientNormalRequest listConnectorsRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@ListConnectorsResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "List Connectors Client Error: " <> show clientError
      return Nothing

getConnector :: TL.Text -> IO (Maybe Connector)
getConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let getConnectorRequest = GetConnectorRequest { getConnectorRequestId = connectorId }
  resp <- hstreamApiGetConnector (ClientNormalRequest getConnectorRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@Connector{} _meta1 _meta2 _status _details -> do
      return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Get Connector Client Error: " <> show clientError
      return Nothing

deleteConnector :: TL.Text -> IO Bool
deleteConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let deleteConnectorRequest = DeleteConnectorRequest { deleteConnectorRequestId = connectorId }
  resp <- hstreamApiDeleteConnector (ClientNormalRequest deleteConnectorRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Delete Connector Client Error: " <> show clientError
      return False
    _ -> return False

cancelConnector :: TL.Text -> IO Bool
cancelConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let cancelConnectorRequest = CancelConnectorRequest { cancelConnectorRequestId = connectorId }
  resp <- hstreamApiCancelConnector (ClientNormalRequest cancelConnectorRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Cancel Connector Client Error: " <> show clientError
      return False
    _ -> return False

restartConnector :: TL.Text -> IO Bool
restartConnector connectorId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let restartConnectorRequest = RestartConnectorRequest { restartConnectorRequestId = connectorId }
  resp <- hstreamApiRestartConnector (ClientNormalRequest restartConnectorRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return False
    ClientErrorResponse clientError -> do
      putStrLn $ "Restart Connector Client Error: " <> show clientError
      return False
    _ -> return False

spec :: Spec
spec = describe "HStream.RunConnectorSpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR
  let mysqlConnector = "mysql"

  it "create mysql sink connector" $ do
    executeCommandQuery' ("DROP STREAM " <> source1 <> " IF EXISTS ;")
      `shouldReturn` querySuccessResp
    executeCommandQuery' ("CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);")
      `shouldReturn` querySuccessResp

    createSinkConnector (createMySqlConnectorSql mysqlConnector source1)
      >>= (`shouldSatisfy` isJust)

  it "list connectors" $ do
    Just ListConnectorsResponse {listConnectorsResponseConnectors = connectors} <- listConnectors
    let record = V.find (getConnectorResponseIdIs mysqlConnector) connectors
    record `shouldSatisfy` isJust

  it "get connector" $ do
    getConnector mysqlConnector >>= (`shouldSatisfy` isJust)

  it "cancel connector" $
    ( do
        _ <- cancelConnector mysqlConnector
        connector <- getConnector mysqlConnector
        case connector of
          Just (Connector _ 2 _ _) -> return True
          _                        -> return False
    ) `shouldReturn` True

  it "restart connector" $
    ( do
        _ <- restartConnector mysqlConnector
        connector <- getConnector mysqlConnector
        case connector of
          Just (Connector _ 1 _ _) -> return True
          _                        -> return False
    ) `shouldReturn` True

  it "delete connector" $
    ( do
        _ <- cancelConnector mysqlConnector
        _ <- deleteConnector mysqlConnector
        connector <- getConnector mysqlConnector
        case connector of
          Just Connector{} -> return True
          _                -> return False
    ) `shouldReturn` False

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM " <> source1 <> " IF EXISTS ;"
        return [res1]
    ) `shouldReturn` L.replicate 1 (Just querySuccessResp)
