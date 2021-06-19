{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.AppenderSpec where

import           Control.Applicative     (liftA2)
import           Data.Maybe              (fromMaybe)
import           System.IO.Unsafe        (unsafePerformIO)
import           Test.Hspec
import           Z.Data.Vector           (Bytes, packASCII)

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "Stream Writer" $ do
  let logid = 1

  it "append and read" $ do
    _ <- S.append client logid "hello" Nothing
    readPayload logid Nothing `shouldReturn` "hello"

  it "appendBatch" $ do
    _ <- S.appendBatch client logid ["hello", "world"] S.CompressionLZ4 Nothing
    readPayload' logid Nothing `shouldReturn` ["hello", "world"]
