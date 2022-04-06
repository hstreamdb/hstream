{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.WriterSpec where

import qualified Data.ByteString         as BS
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "Stream Writer & Reader" $ do
  let logid = 1

  it "append and read" $ do
    _ <- S.append client logid "hello" Nothing
    readPayload logid Nothing `shouldReturn` "hello"

  it "appendBS and read" $ do
    _ <- S.appendBS client logid "hello" Nothing
    readPayload logid Nothing `shouldReturn` "hello"

  it "appendBatch" $ do
    _ <- S.appendBatch client logid ["hello", "world"] S.CompressionLZ4 Nothing
    readPayload' logid Nothing `shouldReturn` ["hello", "world"]

  prop "appendCompressedBS" $ do
    let maxPayload = 1024 * 1024
    let gen = scale (*1024) arbitrary `suchThat` (\bs -> BS.length bs < maxPayload)
    forAllShow gen show $ \payload -> do
      S.AppendCompletion{..} <- S.appendCompressedBS client logid payload S.CompressionLZ4 Nothing
      readLSN logid (Just appendCompLSN) `shouldReturn` [payload]
