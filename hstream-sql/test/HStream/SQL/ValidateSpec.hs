{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.ValidateSpec where

import           Data.Either                   (isLeft)
import           HStream.SQL.Abs
import           HStream.SQL.Internal.Validate
import           Test.Hspec

spec :: Spec
spec = describe "Validate Basic Data Types" $ do

  it "date" $ do
    validate (DDate (Nothing :: BNFC'Position)
              (IPInteger Nothing 2021) (IPInteger Nothing 02) (IPInteger Nothing 29))
      `shouldSatisfy` isLeft

  it "time" $ do
    validate (DTime (Nothing :: BNFC'Position)
      (IPInteger Nothing 14) (IPInteger Nothing 61) (IPInteger Nothing 59))
        `shouldSatisfy` isLeft
