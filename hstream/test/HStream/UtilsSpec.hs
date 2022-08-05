{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.UtilsSpec (spec) where


import           Test.Hspec

import           HStream.Server.Types.Validate

spec :: Spec
spec = describe "HStream.UtilsSpec" do
  it "isValidateResourceName" do
    isValidateResourceName "x"        `shouldBe` True
    isValidateResourceName "x.1"      `shouldBe` True
    isValidateResourceName "x_-X...." `shouldBe` True
    isValidateResourceName "_x"       `shouldBe` False
