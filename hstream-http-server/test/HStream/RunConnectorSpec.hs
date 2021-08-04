{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.RunConnectorSpec (spec) where

import           Data.Aeson                    (decode)
import           Data.Maybe                    (fromMaybe, isJust)
import qualified Data.Text                     as T
import qualified Database.MySQL.Base           as MySQL
import           Network.HTTP.Simple
import           System.Environment            (lookupEnv)
import           System.IO.Unsafe              (unsafePerformIO)
import           Test.Hspec

import           HStream.HTTP.Server.Connector (ConnectorBO (..))
import           HStream.SpecUtils             (buildRequest, createStream,
                                                deleteStream)

mysqlConnectInfo :: MySQL.ConnectInfo
mysqlConnectInfo = unsafePerformIO $ do
  port <- read . fromMaybe "3306" <$> lookupEnv "MYSQL_LOCAL_PORT"
  return $ MySQL.ConnectInfo { ciUser = "root"
                             , ciPassword = ""
                             , ciPort = port
                             , ciHost = "127.0.0.1"
                             , ciDatabase = "mysql"
                             , ciCharset = 33
                             }
{-# NOINLINE mysqlConnectInfo #-}

createMySqlConnectorSql :: String -> String -> String
createMySqlConnectorSql name stream
  = "CREATE SINK CONNECTOR " <> name <> " WITH (type=mysql, "
 <> "host=" <> (show $ MySQL.ciHost mysqlConnectInfo) <> ","
 <> "port=" <> (show $ MySQL.ciPort mysqlConnectInfo) <> ","
 <> "username=" <> (show $ MySQL.ciUser mysqlConnectInfo) <> ","
 <> "password=" <> (show $ MySQL.ciPassword mysqlConnectInfo) <> ","
 <> "database=" <> (show $ MySQL.ciDatabase mysqlConnectInfo) <> ","
 <> "stream=" <> stream
 <> ");"

-- TODO: config the request url
listConnectors :: IO [ConnectorBO]
listConnectors = do
  request <- buildRequest "GET" "connectors"
  response <- httpLBS request
  let connectors = decode (getResponseBody response) :: Maybe [ConnectorBO]
  case connectors of
      Nothing          -> return []
      Just connectors' -> return connectors'

getConnector :: String -> IO (Maybe ConnectorBO)
getConnector cName = do
  request <- buildRequest "GET" ("connectors/" <> cName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe ConnectorBO)

createConnector :: String -> IO (Maybe ConnectorBO)
createConnector sql = do
  request' <- buildRequest "POST" "connectors/"
  let request = setRequestBodyJSON (ConnectorBO Nothing Nothing Nothing (T.pack sql)) request'
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe ConnectorBO)

terminateConnector :: String -> IO (Maybe Bool)
terminateConnector cName = do
  request <- buildRequest "POST" ("connectors/terminate/" <> cName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe Bool)

deleteConnector :: String -> IO (Maybe Bool)
deleteConnector cName = do
  request <- buildRequest "DELETE" ("connectors/" <> cName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe Bool)

spec :: Spec
spec = describe "HStream.RunConnectorSpec" $ do
    let cName = "testconnector"
    let sName = "teststream"

    let sql = createMySqlConnectorSql cName sName

    it "create connector" $ do
      _ <- createStream sName 3
      connector <- createConnector sql
      connector `shouldSatisfy` (connectorWithCorrectSql sql)

    it "list connectors" $ do
      connectors <- listConnectors
      (length connectors >= 1) `shouldBe` True

    it "get connector" $ do
      connector <- getConnector cName
      connector `shouldSatisfy` (connectorWithCorrectSql sql)

    it "terminate connector" $ do
      res <- terminateConnector cName
      res `shouldBe` (Just True)

    it "delete connector" $ do
      res <- deleteConnector cName
      _ <- deleteStream sName
      res `shouldSatisfy` isJust
  where
    connectorWithCorrectSql :: String -> Maybe ConnectorBO -> Bool
    connectorWithCorrectSql sql connector = case connector of
      Just (ConnectorBO _ _ _ sql') -> sql == (T.unpack sql')
      _                             -> False
