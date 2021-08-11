{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.SQL.Codegen.ArraySpec where

import qualified Data.Aeson                   as A
import           Data.Function
import           HStream.SQL.AST
import           HStream.SQL.Internal.Codegen
import           Test.Hspec

spec :: Spec
spec = describe "Array Scalar Functions" do

  it "ARRAY_DISTINCT__0" do
    unaryOpOnValue OpDistinct (A.Number <$> [1, 1, 2, 3, 1, 2] & A.Array)
      `shouldBe` (A.Number <$> [1, 2, 3] & A.Array)

  it "ARRAY_DISTINCT__1" do
    unaryOpOnValue OpDistinct ([A.String "apple", A.String "apple", A.Null, A.String "cherry"] & A.Array)
      `shouldBe` ([A.String "apple", A.Null, A.String "cherry"] & A.Array)

  it "ARRAY_EXCEPT__0" do
    binOpOnValue OpExcept (A.Number <$> [1, 2, 3, 1, 2] & A.Array) (A.Number <$> [2, 3] & A.Array)
      `shouldBe` A.Array [A.Number 1]

  it "ARRAY_EXCEPT__1" do
    binOpOnValue OpExcept (A.Array [A.String "apple", A.String "apple", A.Null, A.String "cherry"])
      (A.Array [A.String "cherry"]) `shouldBe` A.Array [A.String "apple", A.Null]

  it "ARRAY_INTERSECT__0" do
    (binOpOnValue OpIntersect `on` (\x -> A.Number <$> x & A.Array)) [1, 2, 3, 1, 2] [2, 1]
      `shouldBe` (A.Number <$> [1, 2] & A.Array)

  it "ARRAY_INTERSECT__1" do
    binOpOnValue OpIntersect (A.Array [A.String "apple", A.String "apple", A.Null, A.String "cherry"])
      (A.Array [A.String "apple"]) `shouldBe` A.Array [A.String "apple"]

--- derived decEq
  it "ARRAY_MAX__0" do
    unaryOpOnValue OpArrMax (A.Array [A.Number (-1), A.Number 2, A.Null, A.Number 0])
      `shouldBe` A.Null

  it "ARRAY_MAX__1" do
    unaryOpOnValue OpArrMax (A.Array [A.Bool True, A.Bool False])
      `shouldBe` A.Bool True

  it "ARRAY_MAX__2" do
    unaryOpOnValue OpArrMax (A.Array [A.Number 23, A.Number 24, A.String "r"])
      `shouldBe` A.Number 24

  it "ARRAY_MAX__3" do
    unaryOpOnValue OpArrMax (A.Array [A.String "Foo", A.String "Bar", A.String "baz"])
      `shouldBe` A.String "baz"

  it "ARRAY_MIN__0" do
    unaryOpOnValue OpArrMin (A.Array [A.Number (-1), A.Number 2, A.Null, A.Number 0])
      `shouldBe` A.Number (-1)

  it "ARRAY_MIN__1" do
    unaryOpOnValue OpArrMin (A.Array [A.Bool True, A.Bool False])
      `shouldBe` A.Bool False

  it "ARRAY_MIN__2" do
    unaryOpOnValue OpArrMin (A.Array [A.Number 23, A.Number 24, A.String "r"])
      `shouldBe` A.String "r"

  it "ARRAY_MIN__3" do
    unaryOpOnValue OpArrMin (A.Array [A.String "Foo", A.String "Bar", A.String "baz"])
      `shouldBe` A.String "Bar"

  it "ARRAY_REMOVE__0" do
    binOpOnValue OpRemove (A.Number <$> [1, 2, 3, 2, 1] & A.Array) (A.Number 2)
      `shouldBe` (A.Number <$> [1, 3, 1] & A.Array)

  it "ARRAY_REMOVE__1" do
    binOpOnValue OpRemove (A.Array [A.Bool False, A.Null, A.Bool True, A.Bool True]) (A.Bool False)
      `shouldBe` A.Array [A.Null, A.Bool True, A.Bool True]

  it "ARRAY_REMOVE__2" do
    binOpOnValue OpRemove (A.Array [A.String "Foo", A.String "Bar", A.Null, A.String "baz"]) A.Null
      `shouldBe` A.Array [A.String "Foo", A.String "Bar", A.String "baz"]

  it "ARRAY_SORT__0" do
    unaryOpOnValue OpSort (A.Array [A.Number (-1), A.Number 2, A.Null, A.Number 0])
      `shouldBe` A.Array [A.Number (-1), A.Number 0, A.Number 2, A.Null]

  it "ARRAY_SORT__1" do
    unaryOpOnValue OpSort (A.Array [A.Bool False, A.Null, A.Bool True])
      `shouldBe` A.Array [A.Bool False, A.Bool True, A.Null]

  it "ARRAY_SORT__2" do
    unaryOpOnValue OpSort (A.Array [A.String "Foo", A.String "Bar", A.Null, A.String "baz"])
      `shouldBe` A.Array [A.String "Bar", A.String "Foo", A.String "baz", A.Null]

  it "ARRAY_UNION__0" do
    (binOpOnValue OpUnion `on` (\xs -> A.Number <$> xs & A.Array))
      [1, 2, 3, 1, 2] [4, 1] `shouldBe` (\xs -> A.Number <$> xs & A.Array) [1, 2, 3, 4]

  it "ARRAY_UNION__1" do
    binOpOnValue OpUnion (A.Array [A.String "apple", A.String "apple", A.Null, A.String "cherry"])
      (A.Array [A.String "cherry"]) `shouldBe` A.Array [A.String "apple", A.Null, A.String "cherry"]
