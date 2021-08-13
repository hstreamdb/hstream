{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.ValidateSpec where

import           Data.Either                   (isLeft, isRight)
import           HStream.SQL.Abs
import           HStream.SQL.Internal.Validate
import           Test.Hspec

spec :: Spec
spec = describe "Validate Basic Data Types" $ do
  let mkNothing :: BNFC'Position
      mkNothing = Nothing :: BNFC'Position

  it "PNInteger" $ do
    validate (PInteger  mkNothing 807) `shouldSatisfy` isRight
    validate (NInteger  mkNothing  36) `shouldSatisfy` isRight
    validate (IPInteger mkNothing  16) `shouldSatisfy` isRight

  it "PNDouble" $ do
    validate (PDouble  mkNothing 0.807) `shouldSatisfy` isRight
    validate (IPDouble mkNothing 20.05) `shouldSatisfy` isRight
    validate (NDouble  mkNothing 15.00) `shouldSatisfy` isRight

  it "SString" $ do
    validate (SString "netural term") `shouldSatisfy` isRight

  it "RawColumn" $ do
    validate (RawColumn "Kaze no Yukue") `shouldSatisfy` isRight

  it "Boolean" $ do
    validate (BoolTrue  mkNothing) `shouldSatisfy` isRight
    validate (BoolFalse mkNothing) `shouldSatisfy` isRight

  it "date" $ do
    validate (DDate mkNothing
              (IPInteger Nothing 2021) (IPInteger Nothing 02) (IPInteger Nothing 29))
      `shouldSatisfy` isLeft
    validate (DDate mkNothing
              (IPInteger Nothing 2020) (IPInteger Nothing 02) (IPInteger Nothing 29))
      `shouldSatisfy` isRight
    validate (DDate mkNothing
              (IPInteger Nothing 2005) (IPInteger Nothing 13) (IPInteger Nothing 29))
      `shouldSatisfy` isLeft

  it "time" $ do
    validate (DTime mkNothing
      (IPInteger Nothing 14) (IPInteger Nothing 61) (IPInteger Nothing 59))
        `shouldSatisfy` isLeft
    validate (DTime mkNothing
      (IPInteger Nothing 14) (IPInteger Nothing 16) (IPInteger Nothing 59))
        `shouldSatisfy` isRight

  it "Interval" $ do
    validate (DInterval mkNothing (IPInteger mkNothing 13)  (TimeUnitYear mkNothing))
      `shouldSatisfy` isRight
    validate (DInterval mkNothing (NInteger mkNothing (-1)) (TimeUnitYear mkNothing))
      `shouldSatisfy` isRight

  it "ColName" $ do
    validate (ColNameSimple mkNothing (Ident "col")) `shouldSatisfy` isRight
    validate (ColNameStream mkNothing (Ident "stream") (Ident "col"))
      `shouldSatisfy` isRight
