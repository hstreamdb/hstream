{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.ParseRefineSpec where

import           HStream.SQL.AST
import           HStream.SQL.Parse
import           Test.Hspec

spec :: Spec
spec = describe "Create" $ do

  it "create stream without option, alias or SELECT clause" $ do
    parseAndRefine "CREATE STREAM foo;"
      `shouldReturn` RQCreate (RCreate "foo" (RStreamOptions { rRepFactor = 3 }))
