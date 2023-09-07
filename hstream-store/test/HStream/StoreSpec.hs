module HStream.StoreSpec where

import           Test.Hspec

import           Control.Monad                    (void)
import           Data.Time.Clock.POSIX            (getPOSIXTime)
import           Z.Data.CBytes                    (CBytes)

import           HStream.Base                     (genUnique)
import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as S
import           HStream.Store.SpecUtils

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

  time <- runIO getPOSIXTime
  loggroupAround (round time) "testTrim" S.def{S.logReplicationFactor = S.defAttr1 1} $ do
    it "trim un-existed record" $ \(_lgname, ranlogid) -> do
      S.isLogEmpty client ranlogid `shouldReturn` True
      sn0 <- S.appendCompLSN <$> S.append client ranlogid "hello" Nothing
      S.trim client ranlogid (sn0 - 10) `shouldReturn` ()

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

  -- Here we use a unique logid
  findKeyLogid <- runIO genUnique
  let findKeyLogname = "test_findkey"
      findKeyAttrs = S.def{S.logReplicationFactor = S.defAttr1 1}
  loggroupAround findKeyLogid findKeyLogname findKeyAttrs $ do
    it "findKey" $ \(_lgname, randlogid) -> do
      void $ S.appendCompressedBS client randlogid "p" S.CompressionNone Nothing
      void $ S.appendCompressedBS client randlogid "p" S.CompressionNone (Just [])
      sn0 <- S.appendCompLSN <$>
        S.appendCompressedBS client randlogid "p0" S.CompressionNone
                             (Just [(S.KeyTypeFindKey, "0")])
      sn1 <- S.appendCompLSN <$>
        S.appendCompressedBS client randlogid "p1" S.CompressionNone
                             (Just [(S.KeyTypeFindKey, "1")])
      sn2 <- S.appendCompLSN <$>
        S.appendCompressedBS client randlogid "p2" S.CompressionNone
                             (Just [(S.KeyTypeFindKey, "2")])
      void $ S.appendCompressedBS client randlogid "p" S.CompressionNone Nothing

      (lo0, hi0) <- S.findKey client randlogid "0" S.FindKeyStrict
      lo0 `shouldBe` S.LSN_INVALID
      hi0 `shouldBe` sn0
      (lo1, hi1) <- S.findKey client randlogid "1" S.FindKeyStrict
      lo1 `shouldBe` sn0
      hi1 `shouldBe` sn1
      (lo2, hi2) <- S.findKey client randlogid "2" S.FindKeyStrict
      lo2 `shouldBe` sn1
      hi2 `shouldBe` sn2

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
