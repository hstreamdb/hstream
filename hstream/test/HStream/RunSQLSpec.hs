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

  it "clean streams" $
    ( do
        setLogDeviceDbgLevel C_DBG_ERROR
        res1 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source1 <> " ;"
        res2 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source2 <> " ;"
        res3 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> source3 <> " ;"
        res4 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> sink1 <> " ;"
        res5 <- executeCommandQuery $ "DROP STREAM IF EXISTS " <> sink2 <> " ;"
        return [res1, res2, res3, res4, res5]
    ) `shouldReturn` L.replicate 5 (Just successResp)

  it "create streams" $
    (do
        res1 <- executeCommandQuery $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        res2 <- executeCommandQuery $ "CREATE STREAM " <> source2 <> ";"
        res3 <- executeCommandQuery $ "CREATE STREAM " <> source3 <> " WITH (REPLICATE = 3);"
        res4 <- executeCommandQuery $ "CREATE STREAM " <> sink1   <> " WITH (REPLICATE = 3);"
        res5 <- executeCommandQuery $ "CREATE STREAM " <> sink2   <> " ;"
        return [res1, res2, res3, res4, res5]
    ) `shouldReturn` L.replicate 5 (Just successResp)

  it "insert data to source streams" $
    (do
      res1 <- executeCommandQuery $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (22, 80);"
      res2 <- executeCommandQuery $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (15, 10);"
      return [res1, res2]
    ) `shouldReturn` L.replicate 2 (Just successResp)

  it "a simple SQL query" $
    (do
       _ <- forkIO $ do
         threadDelay 1000000
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

  it "GROUP BY without timewindow" $
    (do
        _ <- forkIO $ do
          threadDelay 1000000
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
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10
                                (MetadataMap Map.empty) (action (5.0 :: Double) ref))
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
