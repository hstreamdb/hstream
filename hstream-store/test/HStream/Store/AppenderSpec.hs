{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.AppenderSpec (spec) where

import           Control.Exception
import           System.IO.Unsafe  (unsafePerformIO)
import           Test.Hspec
import           Z.Data.Vector     (Bytes, packASCII)

import qualified HStream.Store     as S

client :: S.LDClient
client = unsafePerformIO $ do
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  S.newLDClient "/data/store/logdevice.conf"
{-# NOINLINE client #-}

spec :: Spec
spec = describe "Stream Writer" $ do
  let logid = 1

  it "append and read" $ do
    _ <- S.append client logid "hello" Nothing
    readLastPayload client logid `shouldReturn` "hello"

  it "get default payload size for this client" $
    S.getMaxPayloadSize client `shouldReturn` (1024 * 1024)    -- 1MB

  it "modify default payload size for this client" $ do
    S.setClientSettings client "max-payload-size" "1024" -- minimum value: 16
    S.getMaxPayloadSize client `shouldReturn` 1024
    _ <- S.append client logid (packASCII $ replicate 1024 'a') Nothing
    S.append client logid (packASCII $ replicate 1025 'a') Nothing `shouldThrow` anyException

  it "get tail sequence number" $ do
    seqNum0 <- S.appendCbLSN <$> S.append client logid "hello" Nothing
    seqNum1 <- S.getTailLSN client logid
    seqNum0 `shouldBe` seqNum1

readLastPayload :: S.LDClient -> S.C_LogID -> IO Bytes
readLastPayload client logid = do
  sn <- S.getTailLSN client logid
  reader <- S.newLDReader client 1 Nothing
  S.readerStartReading reader logid sn sn
  xs <- S.readerRead reader 10
  return $ S.recordPayload $ head xs
