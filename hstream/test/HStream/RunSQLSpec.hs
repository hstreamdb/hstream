{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunSQLSpec (spec) where

import           Control.Concurrent
import qualified Data.Aeson                      as Aeson
import qualified Data.List                       as L
import qualified Data.Text.Lazy                  as TL
import qualified Data.Vector                     as V
import qualified Database.ClickHouseDriver.Types as ClickHouse
import           Database.MySQL.Base             (MySQLValue (MySQLInt32))
import           Test.Hspec

import           HStream.Logger                  as Log
import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger            (pattern C_DBG_ERROR,
                                                  setLogDeviceDbgLevel)
import           HStream.ThirdParty.Protobuf     (Struct)
import           HStream.Utils

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  baseSpec
  connectorSpec
  viewSpec

-------------------------------------------------------------------------------
-- BaseSpec

baseSpecAround :: ActionWith (HStreamClientApi, TL.Text) -> HStreamClientApi -> IO ()
baseSpecAround = provideRunTest setup clean
  where
    setup api = do
      source <- TL.fromStrict <$> newRandomText 20
      runCreateStreamSql api $ "CREATE STREAM " <> source <> " WITH (REPLICATE = 3);"
      return source
    clean api source =
      runDropSql api $ "DROP STREAM " <> source <> " IF EXISTS;"

baseSpec :: Spec
baseSpec = aroundAll provideHstreamApi $ aroundWith baseSpecAround $
  describe "BaseSpec" $ parallel $ do

  it "insert data and select" $ \(api, source) -> do
    _ <- forkIO $ do
      -- FIXME: requires a notification mechanism to ensure that the task
      -- starts successfully before inserting data
      threadDelay 5000000
      Log.d $ "Insert into " <> Log.buildLazyText source <> " ..."
      runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (22, 80);")
      runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (15, 10);")

    -- TODO
    executeCommandPushQuery ("SELECT * FROM " <> source <> " EMIT CHANGES;")
      `shouldReturn` [ mkStruct [("temperature", Aeson.Number 22), ("humidity", Aeson.Number 80)]
                     , mkStruct [("temperature", Aeson.Number 15), ("humidity", Aeson.Number 10)]
                     ]

  it "GROUP BY without timewindow" $ \(api, source) -> do
    _ <- forkIO $ do
      -- FIXME: requires a notification mechanism to ensure that the task
      -- starts successfully before inserting data
      threadDelay 5000000
      Log.d $ "Insert into " <> Log.buildLazyText source <> " ..."
      runInsertSql api ("INSERT INTO " <> source <> " (a, b) VALUES (1, 2);")
      runInsertSql api ("INSERT INTO " <> source <> " (a, b) VALUES (2, 2);")
      runInsertSql api ("INSERT INTO " <> source <> " (a, b) VALUES (3, 2);")
      runInsertSql api ("INSERT INTO " <> source <> " (a, b) VALUES (4, 3);")

    -- TODO
    executeCommandPushQuery ("SELECT SUM(a) AS result FROM " <> source <> " GROUP BY b EMIT CHANGES;")
      `shouldReturn` [ mkStruct [("result", Aeson.Number 1)]
                     , mkStruct [("result", Aeson.Number 3)]
                     , mkStruct [("result", Aeson.Number 6)]
                     , mkStruct [("result", Aeson.Number 4)]
                     ]

-------------------------------------------------------------------------------
-- ConnectorSpec

connectorSpecAround :: ActionWith (HStreamClientApi, TL.Text) -> HStreamClientApi -> IO ()
connectorSpecAround = provideRunTest setup clean
  where
    setup api = do
      source <- TL.fromStrict <$> newRandomText 20
      runCreateStreamSql api $ "CREATE STREAM " <> source <> ";"
      createMysqlTable $ TL.toStrict source
      createClickHouseTable $ TL.toStrict source
      return source
    clean api source = do
      runDropSql api $ "DROP STREAM " <> source <> " IF EXISTS;"
      dropMysqlTable $ TL.toStrict source
      dropClickHouseTable $ TL.toStrict source
      -- TODO: drop connector

connectorSpec :: Spec
connectorSpec = aroundAll provideHstreamApi $ aroundWith connectorSpecAround $
  describe "ConnectorSpec" $ parallel $ do

  it "mysql connector" $ \(api, source) -> do
    runQuerySimple_ api (createMySqlConnectorSql ("mysql_" <> source) source)
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (12, 84);")
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (22, 83);")
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (32, 82);")
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (42, 81);")
    threadDelay 5000000
    fetchMysql (TL.toStrict source) `shouldReturn` [ [MySQLInt32 12, MySQLInt32 84]
                                                   , [MySQLInt32 22, MySQLInt32 83]
                                                   , [MySQLInt32 32, MySQLInt32 82]
                                                   , [MySQLInt32 42, MySQLInt32 81]
                                                   ]

  it "clickhouse connector" $ \(api, source) -> do
    runQuerySimple_ api (createClickHouseConnectorSql ("clickhouse_" <> source) source)
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (12, 84);")
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (22, 83);")
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (32, 82);")
    runInsertSql api ("INSERT INTO " <> source <> " (temperature, humidity) VALUES (42, 81);")
    threadDelay 5000000
    -- Note: ClickHouse does not return data in deterministic order by default,
    --       see [this answer](https://stackoverflow.com/questions/54786494/clickhouse-query-row-order-behaviour).
    fetchClickHouse (TL.toStrict source)
      `shouldReturn` V.fromList [ V.fromList [ClickHouse.CKInt64 12, ClickHouse.CKInt64 84]
                                , V.fromList [ClickHouse.CKInt64 22, ClickHouse.CKInt64 83]
                                , V.fromList [ClickHouse.CKInt64 32, ClickHouse.CKInt64 82]
                                , V.fromList [ClickHouse.CKInt64 42, ClickHouse.CKInt64 81]
                     ]

-------------------------------------------------------------------------------
-- ViewSpec

viewSpecAround
  :: ActionWith (HStreamClientApi, (TL.Text, TL.Text, TL.Text))
  -> HStreamClientApi -> IO ()
viewSpecAround = provideRunTest setup clean
  where
    setup api = do
      source1 <- ("runsql_view_source1_" <>) . TL.fromStrict <$> newRandomText 20
      source2 <- ("runsql_view_source2_" <>) . TL.fromStrict <$> newRandomText 20
      viewName <- ("runsql_view_view_" <>) . TL.fromStrict <$> newRandomText 20
      runCreateStreamSql     api $ "CREATE STREAM " <> source1 <> ";"
      runCreateWithSelectSql api $ "CREATE STREAM " <> source2
                                <> " AS SELECT a, 1 AS b FROM " <> source1
                                <> " EMIT CHANGES;"
      runQuerySimple_ api $ "CREATE VIEW " <> viewName
                         <> " AS SELECT SUM(a) FROM " <> source2
                         <> " GROUP BY b EMIT CHANGES;"
      -- FIXME: wait the SELECT task to be initialized.
      threadDelay 2000000
      return (source1, source2, viewName)
    clean api (source1, source2, viewName) = do
      runDropSql api $ "DROP VIEW " <> viewName <> " IF EXISTS;"
      runDropSql api $ "DROP STREAM " <> source2 <> " IF EXISTS;"
      runDropSql api $ "DROP STREAM " <> source1 <> " IF EXISTS;"

viewSpec :: Spec
viewSpec =
  aroundAll provideHstreamApi $ aroundAllWith viewSpecAround $
  describe "ViewSpec" $ parallel $ do

  it "show streams should not include views" $ \(api, (_s1, _s2, view)) -> do
    res <- runShowStreamsSql api "SHOW STREAMS;"
    L.sort (words res)
      `shouldNotContain` map TL.unpack (L.sort [view])

  it "show views should not include streams" $ \(api, (s1, s2, _view)) -> do
    res <- runShowViewsSql api "SHOW VIEWS;"
    L.sort (words res)
      `shouldNotContain` map TL.unpack (L.sort [s1, s2])

  it "select from view" $ \(api, (source1, _source2, viewName)) -> do
    runInsertSql api $ "INSERT INTO " <> source1 <> " (a) VALUES (1);"
    runInsertSql api $ "INSERT INTO " <> source1 <> " (a) VALUES (2);"
    threadDelay 4000000
    runQuerySimple api ("SELECT * FROM " <> viewName <> " WHERE b = 1;")
      `grpcShouldReturn` mkViewResponse (mkStruct [("SUM(a)", Aeson.Number 3)])

    runInsertSql api $ "INSERT INTO " <> source1 <> " (a) VALUES (3);"
    runInsertSql api $ "INSERT INTO " <> source1 <> " (a) VALUES (4);"
    threadDelay 4000000
    runQuerySimple api ("SELECT * FROM " <> viewName <> " WHERE b = 1;")
      `grpcShouldReturn` mkViewResponse (mkStruct [("SUM(a)", Aeson.Number 10)])
