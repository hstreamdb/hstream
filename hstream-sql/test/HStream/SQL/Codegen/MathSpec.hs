{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.MathSpec where

import qualified Data.Aeson                   as A
import           Data.Function
import           HStream.SQL.AST
import           HStream.SQL.Internal.Codegen
import           Test.Hspec

spec :: Spec
spec = describe "Math Scalar Functions" $ do

  it "binary add" $ do
    binOpOnValue OpAdd (A.Number 1) (A.Number 2) `shouldBe` (A.Number 3)
