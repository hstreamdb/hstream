module HStream.BaseSpec (spec) where

import           Control.Concurrent
import           Control.Monad
import qualified Data.ByteString.Short                as B.Short
import           Data.Either
import qualified Data.Set                             as Set
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck
import           Test.QuickCheck.Instances.ByteString ()
import qualified Z.Data.CBytes                        as CBytes

import           HStream.Base
import           HStream.Base.Bytes

spec :: Spec
spec = parallel $ do
  baseSpec

baseSpec :: Spec
baseSpec = describe "HStream.Base" $ do
  it "genUniqueSpec" $ do
    let maxInflights = 5
        eachTimes = 5
    results <- forM [0..maxInflights-1] $ const newEmptyMVar
    forM_ [0..maxInflights-1] $ \idx -> forkIO $ do
      r <- replicateM eachTimes genUnique
      putMVar (results !! idx) r
    rs <- concat <$> forM [0..maxInflights-1] (takeMVar . (results !!))
    length rs `shouldBe` Set.size (Set.fromList rs)

  it "ShortByteString to/from CByte" $ do
    sbs2cbytes "xx" `shouldBe` "xx"
    sbs2cbytes (B.Short.singleton 0) `shouldBe` CBytes.empty
    sbs2cbytes (B.Short.singleton 0 <> B.Short.singleton 32) `shouldBe` CBytes.empty
    sbs2cbytesUnsafe (B.Short.singleton 0) `shouldBe` CBytes.empty

  prop "CByte to/from ShortByteString" $ \x ->
    (sbs2cbytes $ cbytes2sbs x) === x

  -- TODO
  it "setupFatalSignalHandler" $ setupFatalSignalHandler `shouldReturn` ()
