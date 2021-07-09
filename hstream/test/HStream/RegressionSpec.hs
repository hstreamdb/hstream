{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RegressionSpec (spec) where

import           Control.Concurrent
import qualified Data.Aeson         as Aeson
import           Test.Hspec

import           HStream.Common

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do

  it "#391_JOIN" $
    (do
       _ <- executeCommandQuery "CREATE STREAM s1;"
       _ <- executeCommandQuery "CREATE STREAM s2;"
       _ <- forkIO $ do
         threadDelay 5000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
         _ <- executeCommandQuery "INSERT INTO s1 (a, b) VALUES (1, 3);"
         _ <- executeCommandQuery "INSERT INTO s2 (a, b) VALUES (2, 3);"
         return ()
       executeCommandPushQuery "SELECT s1.a, s2.a, s1.b, s2.b, SUM(s1.a), SUM(s2.a) FROM s1 INNER JOIN s2 WITHIN (INTERVAL 1 MINUTE) ON (s1.b = s2.b) GROUP BY s1.b EMIT CHANGES;"
    ) `shouldReturn` [ mkStruct [ ("SUM(s1.a)", Aeson.Number 1)
                                , ("SUM(s2.a)", Aeson.Number 2)
                                , ("s1.a"     , Aeson.Number 1)
                                , ("s1.b"     , Aeson.Number 3)
                                , ("s2.a"     , Aeson.Number 2)
                                , ("s2.b"     , Aeson.Number 3)
                                ]
                     ]

  it "#394_SESSION" $
    (do
       _ <- executeCommandQuery "CREATE STREAM s3;"
       _ <- forkIO $ do
         threadDelay 5000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
         _ <- executeCommandQuery "INSERT INTO s3 (a, b) VALUES (1, 4);"
         _ <- executeCommandQuery "INSERT INTO s3 (a, b) VALUES (1, 4);"
         _ <- executeCommandQuery "INSERT INTO s3 (a, b) VALUES (1, 4);"
         _ <- executeCommandQuery "INSERT INTO s3 (a, b) VALUES (1, 4);"
         return ()
       executeCommandPushQuery "SELECT a, b, SUM(a) FROM s3 GROUP BY b, SESSION(INTERVAL 10 MINUTE) EMIT CHANGES;"
    ) `shouldReturn` [ mkStruct [("SUM(a)", Aeson.Number 1), ("a", Aeson.Number 1), ("b", Aeson.Number 4)]
                     , mkStruct [("SUM(a)", Aeson.Number 2), ("a", Aeson.Number 1), ("b", Aeson.Number 4)]
                     , mkStruct [("SUM(a)", Aeson.Number 3), ("a", Aeson.Number 1), ("b", Aeson.Number 4)]
                     , mkStruct [("SUM(a)", Aeson.Number 4), ("a", Aeson.Number 1), ("b", Aeson.Number 4)]
                     ]

  it "#403_RAW" $
    (do
       _ <- executeCommandQuery "CREATE STREAM s4;"
       _ <- executeCommandQuery "CREATE STREAM s5 AS SELECT SUM(a), a + 1, COUNT(*) AS result, b FROM s4 GROUP BY b EMIT CHANGES;"
       _ <- forkIO $ do
         threadDelay 5000000 -- FIXME: requires a notification mechanism to ensure that the task starts successfully before inserting data
         _ <- executeCommandQuery "INSERT INTO s4 (a, b) VALUES (1, 4);"
         _ <- executeCommandQuery "INSERT INTO s4 (a, b) VALUES (1, 4);"
         _ <- executeCommandQuery "INSERT INTO s4 (a, b) VALUES (1, 4);"
         _ <- executeCommandQuery "INSERT INTO s4 (a, b) VALUES (1, 4);"
         return ()
       executeCommandPushQuery "SELECT `SUM(a)`, `result` AS cnt, b, `a+1` FROM s5 EMIT CHANGES;"
    ) `shouldReturn` [ mkStruct [("cnt", Aeson.Number 1), ("a+1", Aeson.Number 2), ("b", Aeson.Number 4), ("SUM(a)", Aeson.Number 1)]
                     , mkStruct [("cnt", Aeson.Number 2), ("a+1", Aeson.Number 2), ("b", Aeson.Number 4), ("SUM(a)", Aeson.Number 2)]
                     , mkStruct [("cnt", Aeson.Number 3), ("a+1", Aeson.Number 2), ("b", Aeson.Number 4), ("SUM(a)", Aeson.Number 3)]
                     , mkStruct [("cnt", Aeson.Number 4), ("a+1", Aeson.Number 2), ("b", Aeson.Number 4), ("SUM(a)", Aeson.Number 4)]
                     ]
