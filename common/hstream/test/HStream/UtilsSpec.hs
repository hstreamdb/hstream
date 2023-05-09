module HStream.UtilsSpec (spec) where

import           Control.Concurrent
import           Control.Monad
import           Data.Either
import qualified Data.Set           as Set
import           HStream.Utils
import           Test.Hspec

spec :: Spec
spec = parallel $ do
  timeIntervalSpec

timeIntervalSpec :: Spec
timeIntervalSpec = describe "TimeInterval" $ do
  it "Parse 1s should be OK" $ do
    parserInterval "1s" `shouldBe` Right (Seconds 1)
  it "Parse 1ss should be Err" $ do
    parserInterval "1ss" `shouldSatisfy` isLeft
