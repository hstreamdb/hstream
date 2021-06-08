{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.StoreSpec where

import           Data.Int                (Int64)
import           Test.Hspec
import qualified Z.Data.Builder          as B
import qualified Z.Data.CBytes           as CBytes
import           Z.Data.Vector           (packASCII)
import           Z.IO.Time               (SystemTime (..), getSystemTime')

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "HStoreSpec" $ do
  let logid = 1

  it "get default payload size for this client" $ do
    S.getMaxPayloadSize client `shouldReturn` (1024 * 1024)    -- 1MB

  it "modify default payload size for this client" $ do
    S.setClientSetting client "max-payload-size" "1024" -- minimum value: 16
    S.getMaxPayloadSize client `shouldReturn` 1024
    _ <- S.append client logid (packASCII $ replicate 1024 'a') Nothing
    S.append client logid (packASCII $ replicate 1025 'a') Nothing `shouldThrow` anyException
    S.d "Reset default payload size"
    S.setClientSetting client "max-payload-size" $ CBytes.buildCBytes $ B.int @Int (1024 * 1024)

  it "get tail sequence number" $ do
    seqNum0 <- S.appendCbLSN <$> S.append client logid "hello" Nothing
    seqNum1 <- S.getTailLSN client logid
    seqNum0 `shouldBe` seqNum1
    let logid' = 101 -- an unknown logid
    S.getTailLSN client logid' `shouldThrow` anyException

  it "trim record" $ do
    sn0 <- S.appendCbLSN <$> S.append client logid "hello" Nothing
    sn1 <- S.appendCbLSN <$> S.append client logid "world" Nothing
    readPayload logid (Just sn0) `shouldReturn` "hello"
    S.trim client logid sn0
    readPayload' logid (Just sn0) `shouldReturn` []
    readPayload logid (Just sn1) `shouldReturn` "world"

  -- TODO: findKey by timestamp is not so accurate.
  --
  -- it "find time" $ do
  --   let toMillisecond (MkSystemTime s ns) = s * 1000 + fromIntegral (ns `div` 1000000)
  --   ms0 <- toMillisecond <$> getSystemTime'
  --   sn0 <- S.appendCbLSN <$> S.append client logid "hello" Nothing
  --   ms1 <- toMillisecond <$> getSystemTime'
  --   sn1 <- S.appendCbLSN <$> S.append client logid "world" Nothing

  --   S.findTime client logid ms0 S.FindKeyStrict `shouldReturn` sn0
  --   S.findTime client logid ms1 S.FindKeyStrict `shouldReturn` sn1
