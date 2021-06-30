{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunSQLSpec (spec) where

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

import           HStream.Store
import           HStream.Utils

import           HStream.Server.HStreamApi
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call        (clientCallCancel)

clientConfig :: ClientConfig
clientConfig = ClientConfig { clientServerHost = Host "127.0.0.1"
                            , clientServerPort = Port 6570
                            , clientArgs = []
                            , clientSSLConfig = Nothing
                            , clientAuthority = Nothing
                            }

--------------------------------------------------------------------------------
spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  source1 <- runIO $ TL.fromStrict <$> newRandomText 20
  source2 <- runIO $ TL.fromStrict <$> newRandomText 20
  sink1   <- runIO $ TL.fromStrict <$> newRandomText 20
  sink2   <- runIO $ TL.fromStrict <$> newRandomText 20
  let source3 = "source3"
      source4 = "source4"

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source1 <> " ;"
        res2 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source2 <> " ;"
        res3 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source3 <> " ;"
        res4 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source4 <> " ;"
        res5 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> sink1 <> " ;"
        res6 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> sink2 <> " ;"
        return [res1, res2, res3, res4, res5, res6]
    ) `shouldReturn` L.replicate 6 (Just successResp)

  it "create streams" $
    (do
        res1 <- executeCommandQuery $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        res2 <- executeCommandQuery $ "CREATE STREAM " <> source2 <> ";"
        res3 <- executeCommandQuery $ "CREATE STREAM " <> source3 <> " WITH (REPLICATE = 3);"
        res4 <- executeCommandQuery $ "CREATE STREAM " <> source4 <> " WITH (REPLICATE = 3);"
        res5 <- executeCommandQuery $ "CREATE STREAM " <> sink1   <> " WITH (REPLICATE = 3);"
        res6 <- executeCommandQuery $ "CREATE STREAM " <> sink2   <> " ;"
        return [res1, res2, res3, res4, res5, res6]
    ) `shouldReturn` L.replicate 6 (Just successResp)

  it "insert data to source streams" $
    (do
      res1 <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (22, 80);"
      res2 <- executeCommandQuery $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (15, 10);"
      return [res1, res2]
    ) `shouldReturn` L.replicate 2 (Just successResp)

  it "a simple SQL query" $
    (do
       _ <- forkIO $ do
         threadDelay 5000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
         _ <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (31, 26);"
         _ <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (15, 10);"
         return ()
       executeCommandPushQuery $ "SELECT * FROM " <> source1 <> " EMIT CHANGES;"
    ) `shouldReturn` [ mkStruct [("temperature", Aeson.Number 31), ("humidity", Aeson.Number 26)]
                     , mkStruct [("temperature", Aeson.Number 15), ("humidity", Aeson.Number 10)]
                     ]

  it "mysql connector" $
    (do
       createMysqlTable $ TL.toStrict source3
       _ <- executeCommandQuery $ "CREATE SOURCE | SINK CONNECTOR mysql WITH (type = \"mysql\", host = \"127.0.0.1\", streamname = \""<> source3 <>"\");"
       _ <- executeCommandQuery $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (12, 84);"
       _ <- executeCommandQuery $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (22, 83);"
       _ <- executeCommandQuery $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (32, 82);"
       _ <- executeCommandQuery $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (42, 81);"
       threadDelay 5000000
       fetchMysql $ TL.toStrict source3
    ) `shouldReturn` [ [MySQLInt32 12, MySQLInt32 84]
                     , [MySQLInt32 22, MySQLInt32 83]
                     , [MySQLInt32 32, MySQLInt32 82]
                     , [MySQLInt32 42, MySQLInt32 81]
                     ]

  it "clickhouse connector" $
    (do
       createClickHouseTable $ TL.toStrict source4
       _ <- executeCommandQuery $ "CREATE SOURCE | SINK CONNECTOR clickhouse WITH (type = \"clickhouse\", host = \"127.0.0.1\", streamname = \""<> source4 <>"\");"
       _ <- executeCommandQuery $ "INSERT INTO " <> source4 <> " (temperature, humidity) VALUES (12, 84);"
       _ <- executeCommandQuery $ "INSERT INTO " <> source4 <> " (temperature, humidity) VALUES (22, 83);"
       _ <- executeCommandQuery $ "INSERT INTO " <> source4 <> " (temperature, humidity) VALUES (32, 82);"
       _ <- executeCommandQuery $ "INSERT INTO " <> source4 <> " (temperature, humidity) VALUES (42, 81);"
       threadDelay 5000000
       fetchClickHouse $ TL.toStrict source4
    ) `shouldReturn` V.fromList [ V.fromList [ClickHouse.CKInt64 12,ClickHouse.CKInt64 84]
                                , V.fromList [ClickHouse.CKInt64 22,ClickHouse.CKInt64 83]
                                , V.fromList [ClickHouse.CKInt64 32,ClickHouse.CKInt64 82]
                                , V.fromList [ClickHouse.CKInt64 42,ClickHouse.CKInt64 81]
                     ]
    -- Note: ClickHouse does not return data in deterministic order by default,
    --       see [this answer](https://stackoverflow.com/questions/54786494/clickhouse-query-row-order-behaviour).

  it "GROUP BY without timewindow" $
    (do
        _ <- forkIO $ do
          threadDelay 5000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
          _ <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (a, b) VALUES (1, 2);"
          _ <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (a, b) VALUES (2, 2);"
          _ <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (a, b) VALUES (3, 2);"
          _ <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (a, b) VALUES (4, 3);"
          return ()
        executeCommandPushQuery $ "SELECT SUM(a) AS result FROM " <> source1 <> " GROUP BY b EMIT CHANGES;"
        ) `shouldReturn` [ mkStruct [("result", Aeson.Number 1)]
                         , mkStruct [("result", Aeson.Number 3)]
                         , mkStruct [("result", Aeson.Number 6)]
                         , mkStruct [("result", Aeson.Number 4)]
                         ]

--------------------------------------------------------------------------------
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
  _ <- ClickHouse.query conn $ "DROP TABLE IF EXISTS " <> Text.unpack source
  ClickHouse.closeClient conn
  case q of
    Right res -> return res
    _         -> return V.empty
