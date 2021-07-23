{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunSQLSpec (spec) where

import           Control.Concurrent
import           Control.Monad                   (void)
import qualified Data.Aeson                      as Aeson
import qualified Data.List                       as L
import qualified Data.Text.Lazy                  as TL
import qualified Data.Vector                     as V
import qualified Database.ClickHouseDriver.Types as ClickHouse
import           Database.MySQL.Base             (MySQLValue (MySQLInt32))
import           System.IO.Unsafe                (unsafePerformIO)
import           Test.Hspec

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import           HStream.ThirdParty.Protobuf     (Struct)
import           HStream.Utils.Converter         (structToStruct)
import           HStream.Utils.Format            (formatCommandQueryResponse)

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR
  baseSpec
  connectorSpec
  viewSpec

-------------------------------------------------------------------------------
-- BaseSpec

source1 :: TL.Text
source1 = unsafePerformIO $ ("RunSQLSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE source1 #-}

baseSpecSetup :: IO ()
baseSpecSetup = do
  void $ executeCommandQuery' $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"

baseSpecClean :: IO ()
baseSpecClean = do
  void $ executeCommandQuery' $ "DROP STREAM " <> source1 <> " IF EXISTS;"

baseSpec :: Spec
baseSpec = beforeAll_ baseSpecSetup $ afterAll_ baseSpecClean $ do

  it "insert data and select" $ do
    _ <- forkIO $ do
      -- FIXME: requires a notification mechanism to ensure that the task
      -- starts successfully before inserting data
      threadDelay 5000000
      executeCommandQuery' ("INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (22, 80);")
        `shouldReturn` successResp
      executeCommandQuery' ("INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (15, 10);")
        `shouldReturn` successResp

    executeCommandPushQuery ("SELECT * FROM " <> source1 <> " EMIT CHANGES;")
      `shouldReturn` [ mkStruct [("temperature", Aeson.Number 22), ("humidity", Aeson.Number 80)]
                     , mkStruct [("temperature", Aeson.Number 15), ("humidity", Aeson.Number 10)]
                     ]

  it "GROUP BY without timewindow" $ do
    _ <- forkIO $ do
      -- FIXME: requires a notification mechanism to ensure that the task
      -- starts successfully before inserting data
      threadDelay 5000000
      executeCommandQuery' ("INSERT INTO " <> source1 <> " (a, b) VALUES (1, 2);")
        `shouldReturn` successResp
      executeCommandQuery' ("INSERT INTO " <> source1 <> " (a, b) VALUES (2, 2);")
        `shouldReturn` successResp
      executeCommandQuery' ("INSERT INTO " <> source1 <> " (a, b) VALUES (3, 2);")
        `shouldReturn` successResp
      executeCommandQuery' ("INSERT INTO " <> source1 <> " (a, b) VALUES (4, 3);")
        `shouldReturn` successResp

    executeCommandPushQuery ("SELECT SUM(a) AS result FROM " <> source1 <> " GROUP BY b EMIT CHANGES;")
      `shouldReturn` [ mkStruct [("result", Aeson.Number 1)]
                     , mkStruct [("result", Aeson.Number 3)]
                     , mkStruct [("result", Aeson.Number 6)]
                     , mkStruct [("result", Aeson.Number 4)]
                     ]

-------------------------------------------------------------------------------
-- ConnectorSpec

sink1 :: TL.Text
sink1 = unsafePerformIO $ ("RunSQLSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE sink1 #-}

sink2 :: TL.Text
sink2 = unsafePerformIO $ ("RunSQLSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE sink2 #-}

connectorSpecSetup :: IO ()
connectorSpecSetup = do
  void $ executeCommandQuery' $ "CREATE STREAM " <> sink1   <> " WITH (REPLICATE = 3);"
  void $ executeCommandQuery' $ "CREATE STREAM " <> sink2   <> " ;"
  createMysqlTable $ TL.toStrict sink1
  createClickHouseTable $ TL.toStrict sink2

connectorSpecClean :: IO ()
connectorSpecClean = do
  void $ executeCommandQuery' $ "DROP STREAM " <> sink1 <> " IF EXISTS;"
  void $ executeCommandQuery' $ "DROP STREAM " <> sink2 <> " IF EXISTS;"
  dropMysqlTable $ TL.toStrict sink1
  dropClickHouseTable $ TL.toStrict sink2

connectorSpec :: Spec
connectorSpec = beforeAll_ connectorSpecSetup $ afterAll_ connectorSpecClean $
  describe "HStream.RunSQLSpec.Connector" $ do

  it "mysql connector" $ do
    executeCommandQuery' (createMySqlConnectorSql "mysql" sink1)
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink1 <> " (temperature, humidity) VALUES (12, 84);")
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink1 <> " (temperature, humidity) VALUES (22, 83);")
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink1 <> " (temperature, humidity) VALUES (32, 82);")
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink1 <> " (temperature, humidity) VALUES (42, 81);")
      `shouldReturn` successResp
    threadDelay 5000000
    fetchMysql (TL.toStrict sink1) `shouldReturn` [ [MySQLInt32 12, MySQLInt32 84]
                                                  , [MySQLInt32 22, MySQLInt32 83]
                                                  , [MySQLInt32 32, MySQLInt32 82]
                                                  , [MySQLInt32 42, MySQLInt32 81]
                                                  ]

  it "clickhouse connector" $ do
    executeCommandQuery' (createClickHouseConnectorSql "clickhouse" sink2)
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink2 <> " (temperature, humidity) VALUES (12, 84);")
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink2 <> " (temperature, humidity) VALUES (22, 83);")
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink2 <> " (temperature, humidity) VALUES (32, 82);")
      `shouldReturn` successResp
    executeCommandQuery' ("INSERT INTO " <> sink2 <> " (temperature, humidity) VALUES (42, 81);")
      `shouldReturn` successResp
    threadDelay 5000000
    -- Note: ClickHouse does not return data in deterministic order by default,
    --       see [this answer](https://stackoverflow.com/questions/54786494/clickhouse-query-row-order-behaviour).
    fetchClickHouse (TL.toStrict sink2)
      `shouldReturn` V.fromList [ V.fromList [ClickHouse.CKInt64 12,ClickHouse.CKInt64 84]
                                , V.fromList [ClickHouse.CKInt64 22,ClickHouse.CKInt64 83]
                                , V.fromList [ClickHouse.CKInt64 32,ClickHouse.CKInt64 82]
                                , V.fromList [ClickHouse.CKInt64 42,ClickHouse.CKInt64 81]
                     ]

-------------------------------------------------------------------------------
-- ViewSpec

viewSource1 :: TL.Text
viewSource1 = unsafePerformIO $ ("RunSQLSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE viewSource1 #-}

viewSource2 :: TL.Text
viewSource2 = unsafePerformIO $ ("RunSQLSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE viewSource2 #-}

view1 :: TL.Text
view1 = unsafePerformIO $ ("RunSQLSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE view1 #-}

viewSpecSetup :: IO ()
viewSpecSetup = do
  void $ executeCommandQuery' $ "CREATE STREAM " <> viewSource1 <> ";"
  void $ executeCommandQuery' $ "CREATE STREAM " <> viewSource2 <> " AS SELECT a, 1 AS b FROM " <> viewSource1 <> " EMIT CHANGES;"
  void $ executeCommandQuery' $ "CREATE VIEW " <> view1 <> " AS SELECT SUM(a) FROM " <> viewSource2 <> " GROUP BY b EMIT CHANGES;"

viewSpecClean :: IO ()
viewSpecClean = do
  void $ executeCommandQuery' $ "DROP VIEW " <> view1 <> " IF EXISTS;"
  void $ executeCommandQuery $ "DROP STREAM " <> viewSource1 <> " IF EXISTS;"
  void $ executeCommandQuery $ "DROP STREAM " <> viewSource2 <> " IF EXISTS;"

viewSpec :: Spec
viewSpec = beforeAll_ viewSpecSetup $ afterAll_ viewSpecClean $
  describe "HStream.RunSQLSpec.View" $ do

  it "show streams should not include views" $ do
    (Just res) <- executeCommandQuery "SHOW STREAMS;"
    L.sort (words (formatCommandQueryResponse 0 res))
      `shouldBe` map TL.unpack (L.sort [viewSource1, viewSource2])

  it "show views should not include streams" $ do
    (Just res2) <- executeCommandQuery "SHOW VIEWS;"
    formatCommandQueryResponse 0 res2 `shouldBe` TL.unpack (view1 <> "\n")

  it "select from view" $
    ( do
        threadDelay 2000000
        _ <- executeCommandQuery $ "INSERT INTO " <> viewSource1 <> " (a) VALUES (1);"
        _ <- executeCommandQuery $ "INSERT INTO " <> viewSource1 <> " (a) VALUES (2);"
        threadDelay 2000000
        (Just res1) <- executeCommandQuery $ "SELECT * FROM " <> view1 <> " WHERE b = 1;"
        _ <- executeCommandQuery $ "INSERT INTO " <> viewSource1 <> " (a) VALUES (3);"
        _ <- executeCommandQuery $ "INSERT INTO " <> viewSource1 <> " (a) VALUES (4);"
        threadDelay 2000000
        (Just res2) <- executeCommandQuery $ "SELECT * FROM " <> view1 <> " WHERE b = 1;"
        return [res1, res2]
    ) `shouldReturn` mkViewResponse . mkStruct <$>
    [ [("SUM(a)", Aeson.Number 3)],
      [("SUM(a)", Aeson.Number 10)] ]

mkViewResponse :: Struct -> CommandQueryResponse
mkViewResponse = CommandQueryResponse . Just . CommandQueryResponseKindResultSet .
  CommandQueryResultSet . V.singleton . structToStruct "SELECTVIEW"
