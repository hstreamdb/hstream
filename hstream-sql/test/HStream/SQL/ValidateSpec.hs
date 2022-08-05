{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.ValidateSpec where

import qualified Data.Aeson                    as A
import           Data.Either                   (isLeft, isRight)
import           Data.Function
import           Data.Functor
import qualified Data.Text                     as T
import qualified Data.Vector                   as V
import           HStream.SQL.Abs
import           HStream.SQL.Internal.Validate
import           Test.Hspec

mapi = h 0 where
  h i f = \case
    []    -> []
    a : l -> let r = f i a in r : h (i + 1) f l

spec :: Spec
spec = describe "Validate Basic Data Types" $ do

  let mkNothing :: BNFC'Position
      mkNothing = Nothing :: BNFC'Position
  let setH = ExprSetFunc mkNothing (SetFuncCountAll mkNothing)
  let xsVal = [ExprInt mkNothing $ PInteger mkNothing 42, ExprBool mkNothing $ BoolTrue mkNothing, ExprArr mkNothing [ExprInt mkNothing $ PInteger mkNothing 42, ExprBool mkNothing $ BoolTrue mkNothing]]

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

  it "RawIdent" $ do
    validate (RawIdent "Kaze no Yukue") `shouldSatisfy` isRight

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

  it "Aggregate function Ok" $ do
    validate (SetFuncCountAll mkNothing) `shouldSatisfy` isRight
    validate (SetFuncCount mkNothing (ExprBool mkNothing $ BoolTrue mkNothing))
      `shouldSatisfy` isRight
    validate (SetFuncAvg mkNothing (ExprInt mkNothing $ PInteger mkNothing 42))
      `shouldSatisfy` isRight
    validate (SetFuncSum mkNothing (ExprInt mkNothing $ PInteger mkNothing 42))
      `shouldSatisfy` isRight
    validate (SetFuncMax mkNothing (ExprString mkNothing "g free"))
      `shouldSatisfy` isRight
    validate (SetFuncMin mkNothing (ExprInt mkNothing $ NInteger mkNothing 40))
      `shouldSatisfy` isRight

  it "Aggregate function Err" $ do
    validate (SetFuncCount mkNothing setH) `shouldSatisfy` isLeft
    validate (SetFuncAvg   mkNothing setH) `shouldSatisfy` isLeft
    validate (SetFuncMax   mkNothing setH) `shouldSatisfy` isLeft
    validate (SetFuncMin   mkNothing setH) `shouldSatisfy` isLeft

  it "array const" $ do
      validate (ExprArr mkNothing []) `shouldSatisfy` isRight
      validate (ExprArr mkNothing xsVal)
          `shouldSatisfy` isRight

  it "map const" $ do
    validate (ExprMap mkNothing []) `shouldSatisfy` isRight
    validate
      (ExprMap mkNothing $ DLabelledValueExpr mkNothing "foo" <$> xsVal)
        `shouldSatisfy` isLeft
    validate
      (ExprMap mkNothing $ mapi (\i -> DLabelledValueExpr mkNothing (Ident $ "foo" <> T.pack (show i))) xsVal)
        `shouldSatisfy` isRight

  it "sel" $ do
    validate (SelListSublist mkNothing (DerivedColSimpl mkNothing <$> xsVal))
      `shouldSatisfy` isRight
    validate (SelListSublist mkNothing ((\x -> DerivedColAs mkNothing x (Ident "comm")) <$> xsVal))
      `shouldSatisfy` isLeft
    validate (SelListSublist mkNothing (mapi (\i x -> DerivedColAs mkNothing x (Ident $ "comm" <> T.pack (show i))) $ xsVal))
      `shouldSatisfy` isRight

