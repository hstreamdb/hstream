{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.SQL.Codegen.MathSpec where

import qualified Data.Aeson                   as A
import           Data.Function
import           Data.Scientific
import           HStream.SQL.AST
import           HStream.SQL.Internal.Codegen
import           Test.Hspec
import           Test.HUnit

spec :: Spec
spec = describe "Math Scalar Functions" do

-- Arith Expr

  it "binary add" do
    binOpOnValue OpAdd (A.Number 1) (A.Number 2) `shouldBe` (A.Number 3)

  it "floor" do
    unaryOpOnValue OpFloor (A.Number 1.5)
      `shouldBe` A.Number 1

  it "ceil" do
    unaryOpOnValue OpCeil (A.Number 1.5) `shouldBe` A.Number 2
