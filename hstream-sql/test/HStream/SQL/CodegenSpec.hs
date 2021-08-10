{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.CodegenSpec where

import qualified Data.Aeson                   as A
import           HStream.SQL.AST
import           HStream.SQL.Internal.Codegen
import           Test.Hspec

spec :: Spec
spec = describe "Scalar Functions" $ do

  it "binary add" $ do
    binOpOnValue OpAdd (A.Number 1) (A.Number 2) `shouldBe` (A.Number 3)
