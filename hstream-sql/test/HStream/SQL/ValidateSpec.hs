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
