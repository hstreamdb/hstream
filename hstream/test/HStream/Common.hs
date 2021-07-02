{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Common where

import           Control.Concurrent
import qualified Data.Aeson                        as Aeson
import qualified Data.ByteString.Lazy.Char8        as DBCL
import qualified Data.HashMap.Strict               as HM
import           Data.IORef
import qualified Data.List                         as L
import qualified Data.Map.Strict                   as Map
import           Data.Text                         (Text)
import qualified Data.Text                         as Text
import qualified Data.Text.Lazy                    as TL
import qualified Data.Vector                       as V
import qualified Database.ClickHouseDriver.Client  as ClickHouse
import qualified Database.ClickHouseDriver.Types   as ClickHouse
import           Database.MySQL.Base               (MySQLValue (MySQLInt32))
import qualified Database.MySQL.Base               as MySQL
import qualified System.IO.Streams                 as Streams
import           System.Random
import           Test.Hspec
import           ThirdParty.Google.Protobuf.Struct
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call        (clientCallCancel)

import           HStream.Store
import           HStream.Utils
import           HStream.Server.HStreamApi

clientConfig :: ClientConfig
clientConfig = ClientConfig { clientServerHost = Host "127.0.0.1"
                            , clientServerPort = Port 6570
                            , clientArgs = []
                            , clientSSLConfig = Nothing
                            , clientAuthority = Nothing
                            }

newRandomText :: Int -> IO Text
newRandomText n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen

successResp :: CommandQueryResponse
successResp = CommandQueryResponse
  { commandQueryResponseKind = Just (CommandQueryResponseKindSuccess CommandSuccess)
  }

mkStruct :: [(Text, Aeson.Value)] -> Struct
mkStruct = jsonObjectToStruct . HM.fromList

executeCommandQuery :: TL.Text
                    -> IO (Maybe CommandQueryResponse)
executeCommandQuery sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details ->
      return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

executeCommandPushQuery :: TL.Text -> IO [Struct]
executeCommandPushQuery sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ref <- newIORef []
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery $
      ClientReaderRequest commandPushQuery 15
                          (MetadataMap Map.empty) (action (15.0 :: Double) ref)
  readIORef ref
  where
    action timeout ref call _meta recv
      | timeout <= 0 = clientCallCancel call
      | otherwise = do
          msg <- recv
          case msg of
            Right Nothing     -> do
              threadDelay 500000
              action (timeout - 0.5) ref call _meta recv
              return ()
            Right (Just (Struct kvmap)) -> do
              threadDelay 500000
              -- Note: remove "SELECT" tag in returned result
              case snd $ head (Map.toList kvmap) of
                (Just (Value (Just (ValueKindStructValue resp)))) -> do
                  modifyIORef ref (\xs -> xs ++ [resp])
                  action (timeout - 0.5) ref call _meta recv
                  return ()
                _ -> error "unknown data encountered"
            _ -> return ()

terminateQuery :: TL.Text
                    -> IO (Maybe TerminateQueryResponse)
terminateQuery queryName = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let terminateQuery = TerminateQueryRequest{ terminateQueryRequestQueryName = queryName }
  resp <- hstreamApiTerminateQuery (ClientNormalRequest terminateQuery 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@TerminateQueryResponse{} _meta1 _meta2 _status _details ->
      return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

mysqlConnectInfo :: MySQL.ConnectInfo
mysqlConnectInfo = MySQL.ConnectInfo {
    ciUser = "root",
    ciPassword = "password",
    ciPort = 3306,
    ciHost = "127.0.0.1",
    ciDatabase = "mysql",
    ciCharset = 33
  }

createMysqlTable :: Text -> IO ()
createMysqlTable source = do
  conn <- MySQL.connect mysqlConnectInfo
  _ <- MySQL.execute_ conn $ MySQL.Query . DBCL.pack $ "CREATE TABLE IF NOT EXISTS "<> Text.unpack source <>"(temperature INT, humidity INT) CHARACTER SET utf8"
  MySQL.close conn

fetchMysql :: Text -> IO [[MySQL.MySQLValue]]
fetchMysql source = do
  conn <- MySQL.connect mysqlConnectInfo
  (_, items) <- MySQL.query_ conn $ MySQL.Query . DBCL.pack $ "SELECT * FROM " <> Text.unpack source
  datas <- Streams.fold (\xs x -> xs ++ [x]) [] items
  _ <- MySQL.execute_ conn $ MySQL.Query . DBCL.pack $ "DROP TABLE IF EXISTS " <> Text.unpack source
  MySQL.close conn
  return datas

clickHouseConnectInfo :: ClickHouse.ConnParams
clickHouseConnectInfo = ClickHouse.ConnParams {
  username'    = "default",
  host'        = "127.0.0.1",
  port'        = "9000",
  password'    = "",
  compression' = False,
  database'    = "default"
}

createClickHouseTable :: Text -> IO ()
createClickHouseTable source = do
  conn <- ClickHouse.createClient clickHouseConnectInfo
  ClickHouse.query conn ("CREATE TABLE IF NOT EXISTS " ++ Text.unpack source ++
        " (temperature Int64, humidity Int64) " ++ "ENGINE = Memory")
  ClickHouse.closeClient conn

fetchClickHouse :: Text -> IO (V.Vector (V.Vector ClickHouse.ClickhouseType))
fetchClickHouse source = do
  conn <- ClickHouse.createClient clickHouseConnectInfo
  q <- ClickHouse.query conn $ "SELECT * FROM " <> Text.unpack source <> " ORDER BY temperature"
  ClickHouse.closeClient conn
  case q of
    Right res -> return res
    _         -> return V.empty
