module HStream.BaseSpec (spec) where

import           Control.Concurrent
import           Control.Monad
import           Data.Either
import qualified Data.Set           as Set
import           Test.Hspec

import           HStream.Base

spec :: Spec
spec = parallel $ do
  baseSpec

baseSpec :: Spec
baseSpec = describe "HStream.Utils" $ do
  it "genUniqueSpec" $ do
    let maxInflights = 5
        eachTimes = 5
    results <- forM [0..maxInflights-1] $ const newEmptyMVar
    forM_ [0..maxInflights-1] $ \idx -> forkIO $ do
      r <- replicateM eachTimes genUnique
      putMVar (results !! idx) r
    rs <- concat <$> forM [0..maxInflights-1] (takeMVar . (results !!))
    length rs `shouldBe` Set.size (Set.fromList rs)
