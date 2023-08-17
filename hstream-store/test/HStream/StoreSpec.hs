module HStream.StoreSpec where

import           Test.Hspec

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils
import           Z.Data.CBytes           (CBytes)

spec :: Spec
spec = describe "StoreSpec" $ do
  base
  except

loggroupAround' :: SpecWith (CBytes, S.C_LogID) -> Spec
loggroupAround' =
  let logid = 201
      logname = "StoreSpec"
   in loggroupAround logid logname S.def{S.logReplicationFactor = S.defAttr1 1}

base :: Spec
base = describe "Base" $ do
  let logid = 1

  it "get tail sequence number" $ do
    seqNum0 <- S.appendCompLSN <$> S.append client logid "hello" Nothing
    seqNum1 <- S.getTailLSN client logid
    seqNum0 `shouldBe` seqNum1

  it "trim record" $ do
    sn0 <- S.appendCompLSN <$> S.append client logid "hello" Nothing
    sn1 <- S.appendCompLSN <$> S.append client logid "world" Nothing
    readPayload logid (Just sn0) `shouldReturn` "hello"
    S.trim client logid sn0
    readPayload' logid (Just sn0) `shouldReturn` []
    readPayload logid (Just sn1) `shouldReturn` "world"
    -- trim lsn beyond tailLSN should fail
    S.trim client logid (sn1 + 1) `shouldThrow` anyException

  it "trim un-existed record" $ do
    sn0 <- S.appendCompLSN <$> S.append client logid "hello" Nothing
    S.trim client logid (sn0 - 1) `shouldReturn` ()
    readPayload logid (Just sn0) `shouldReturn` "hello"

  loggroupAround' $ do
    it "trim last" $ \(_lgname, ranlogid) -> do
      -- trim an empty log
      S.trimLastBefore 1 client ranlogid `shouldReturn` ()
      sn0 <- S.appendCompLSN <$> S.append client ranlogid "hello" Nothing
      sn1 <- S.appendCompLSN <$> S.append client ranlogid "world" Nothing
      readPayload ranlogid (Just sn0) `shouldReturn` "hello"
      S.trimLastBefore 1 client ranlogid `shouldReturn` ()
      -- nothing happened if we trimLast multi times
      S.trimLastBefore 1 client ranlogid `shouldReturn` ()
      readPayload' ranlogid (Just sn0) `shouldReturn` []
      readPayload ranlogid (Just sn1) `shouldReturn` "world"

  loggroupAround' $ do
    it "logIdHasGroup" $ \(_lgname, randlogid) -> do
      S.logIdHasGroup client randlogid `shouldReturn` True
      123456 `shouldNotBe` randlogid
      S.logIdHasGroup client 123456 `shouldReturn` False

  it "find time with a timestamp of 0" $ do
    headSn <- S.findTime client logid 0 S.FindKeyStrict
    S.trim client logid headSn
    sn <- S.findTime client logid 0 S.FindKeyStrict
    sn `shouldBe` headSn + 1

  -- FIXME: need to find correct way to test this
  --
  --it "find time with maximal timestamp" $ do
  --  sn0 <- S.appendCompLSN <$> S.append client logid "test" Nothing
  --  sn1 <- S.findTime client logid maxBound S.FindKeyStrict
  --  sn1 `shouldBe` sn0 + 1
  --  -- findTime(max) respects the trim point but there was an off-by-one in the
  --  -- code when the entire log was trimmed.
  --  S.trim client logid sn0
  --  sn2 <- S.findTime client logid maxBound S.FindKeyStrict
  --  sn2 `shouldBe` sn0 + 1

except :: Spec
except = describe "Except" $ do
  -- FIXME: this does not fail immediately, and very slow
  xit "get tailLSN of an unknown logid should throw NOTFOUND" $ do
    let logid' = 10000 -- an unknown logid
    S.getTailLSN client logid' `shouldThrow` S.isNOTFOUND
