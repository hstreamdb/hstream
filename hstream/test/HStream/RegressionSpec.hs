{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RegressionSpec (spec) where

import           Control.Concurrent
import qualified Data.Aeson                      as Aeson
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
