{-# LANGUAGE CPP                 #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RegressionSpec (spec) where

import           Control.Concurrent
import qualified Data.Aeson                    as Aeson
import qualified Data.List                     as L
import qualified Data.Text                     as T
import           Test.Hspec

import           HStream.Base                  (setupFatalSignalHandler)
import           HStream.SpecUtils
import           HStream.Store.Logger          (pattern C_DBG_ERROR,
                                                setLogDeviceDbgLevel)

import           Network.GRPC.HighLevel.Client
import           Network.GRPC.LowLevel

spec :: Spec
spec = aroundAll provideHstreamApi $
  describe "HStream.RegressionSpec" $ do
  runIO setupFatalSignalHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  it "#391_JOIN" $ \api -> do
    runDropSql api "DROP STREAM s1 IF EXISTS;"
    runDropSql api "DROP STREAM s2 IF EXISTS;"
    runCreateStreamSql api "CREATE STREAM s1;"
    runCreateStreamSql api "CREATE STREAM s2;"
    _ <- forkIO $ do
      threadDelay 10000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
      runInsertSql api "INSERT INTO s1 (a, b) VALUES (1, 3);"
      runInsertSql api "INSERT INTO s2 (a, b) VALUES (2, 3);"
#ifdef HStreamUseV2Engine
    runFetchSql "SELECT b, SUM(s1.a), SUM(s2.a) FROM s1 INNER JOIN s2 ON s1.b = s2.b GROUP BY s1.b EMIT CHANGES;"
#else
    runFetchSql "SELECT b, SUM(s1.a), SUM(s2.a) FROM s1 INNER JOIN s2 ON s1.b = s2.b WITHIN (INTERVAL '1' HOUR) GROUP BY s1.b EMIT CHANGES;"
#endif
      `shouldReturn` [ mkStruct
        [ ("SUM(s1.a)", mkIntNumber 1)
        , ("SUM(s2.a)", mkIntNumber 2)
        , ("b"        , mkIntNumber 3)
        ]]
    threadDelay 500000
    runDropSql api "DROP STREAM s1 IF EXISTS;"
    runDropSql api "DROP STREAM s2 IF EXISTS;"

  it "#403_RAW" $ \api -> do
    runDropSql api "DROP STREAM s4 IF EXISTS;"
    runDropSql api "DROP STREAM s5 IF EXISTS;"
    runCreateStreamSql api "CREATE STREAM s4;"
    qName <- runCreateWithSelectSql' api "CREATE STREAM s5 AS SELECT SUM(a), COUNT(*) AS result, b FROM s4 GROUP BY b;"
    _ <- forkIO $ do
      threadDelay 10000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
      runInsertSql api "INSERT INTO s4 (a, b) VALUES (1, 4);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s4 (a, b) VALUES (1, 4);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s4 (a, b) VALUES (1, 4);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s4 (a, b) VALUES (1, 4);"
    runFetchSql "SELECT `SUM(a)`, `result` AS cnt, b FROM s5 EMIT CHANGES;"
      >>= (`shouldSatisfy`
           (\l -> not (L.null l) &&
                  L.isSubsequenceOf l
                  [ mkStruct [("cnt", mkIntNumber 1), ("b", mkIntNumber 4), ("SUM(a)", mkIntNumber 1)]
                  , mkStruct [("cnt", mkIntNumber 2), ("b", mkIntNumber 4), ("SUM(a)", mkIntNumber 2)]
                  , mkStruct [("cnt", mkIntNumber 3), ("b", mkIntNumber 4), ("SUM(a)", mkIntNumber 3)]
                  , mkStruct [("cnt", mkIntNumber 4), ("b", mkIntNumber 4), ("SUM(a)", mkIntNumber 4)]])
          )
    threadDelay 500000
    runTerminateSql api $ "TERMINATE QUERY " <> qName <> " ;"
    threadDelay 500000
    runDropSql api "DROP STREAM s5 IF EXISTS;"
    runDropSql api "DROP STREAM s4 IF EXISTS;"

  -- FIXME
  xit "HS352_INT" $ \api -> do
    runDropSql api "DROP STREAM s6 IF EXISTS;"
    runDropSql api "DROP VIEW v6 IF EXISTS;"
    runCreateStreamSql api "CREATE STREAM s6;"
    qName <- runCreateWithSelectSql' api "CREATE VIEW v6 as SELECT key2, key3, SUM(key1) FROM s6 GROUP BY key2, key3;"
    _ <- forkIO $ do
      threadDelay 10000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
      runInsertSql api "INSERT INTO s6 (key1, key2, key3) VALUES (0, \"hello_00000000000000000000\", true);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s6 (key1, key2, key3) VALUES (1, \"hello_00000000000000000001\", false);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s6 (key1, key2, key3) VALUES (2, \"hello_00000000000000000000\", true);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s6 (key1, key2, key3) VALUES (3, \"hello_00000000000000000001\", false);"
      threadDelay 500000
      runInsertSql api "INSERT INTO s6 (key1, key2, key3) VALUES (4, \"hello_00000000000000000000\", true);"
    threadDelay 20000000
    runViewQuerySql api "SELECT * FROM v6 WHERE key3 = FALSE;"
      `shouldReturn` mkViewResponse (mkStruct [ ("SUM(key1)", mkIntNumber 4)
                                                  , ("key2", Aeson.String "hello_00000000000000000001")
                                                  , ("key3", Aeson.Bool False)]
                                        )
    runTerminateSql api $ "TERMINATE QUERY " <> qName <> ";"
    threadDelay 500000
    runDropSql api "DROP STREAM s6 IF EXISTS;"
    runDropSql api "DROP VIEW v6 IF EXISTS;"

  it "#1200_BINARY" $ \api -> do
    runDropSql api "DROP STREAM stream_binary IF EXISTS;"
    runCreateStreamSql api "CREATE STREAM stream_binary;"
    _ <- forkIO $ do
      threadDelay 10000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
      runInsertSql api "INSERT INTO stream_binary VALUES \"aaaaaaaaa\";"
      threadDelay 500000
      runInsertSql api "INSERT INTO stream_binary VALUES \"xxxxxxxxx\";"
      threadDelay 500000
      runInsertSql api "INSERT INTO stream_binary VALUES \"{ \\\"a\\\": 1}\";"
      threadDelay 500000
      runInsertSql api "INSERT INTO stream_binary (b, c) VALUES (1, 2);"
    runFetchSql "SELECT * FROM stream_binary EMIT CHANGES;"
      `shouldReturn` [ mkStruct [ ("a", mkIntNumber 1) ]
                     , mkStruct
                       [ ("b", mkIntNumber 1)
                       , ("c", mkIntNumber 2)
                       ]
                     ]
    threadDelay 500000
    runDropSql api "DROP STREAM stream_binary IF EXISTS;"
