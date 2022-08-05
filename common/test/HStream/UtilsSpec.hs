module HStream.UtilsSpec (spec) where

import           Control.Concurrent
import           Control.Monad
import           Data.Either
import qualified Data.Set                  as Set
import qualified HStream.Server.HStreamApi as API
import           HStream.Utils
import           HStream.Utils.Validation
import           Test.Hspec

spec :: Spec
spec = parallel $ do
  utilsSpec
  timeIntervalSpec

timeIntervalSpec :: Spec
timeIntervalSpec = describe "TimeInterval" $ do
  it "Parse 1s should be OK" $ do
    parserInterval "1s" `shouldBe` Right (Seconds 1)
  it "Parse 1ss should be Err" $ do
    parserInterval "1ss" `shouldSatisfy` isLeft

utilsSpec :: Spec
utilsSpec = describe "HStream.Utils" $ do
  it "genUniqueSpec" $ do
    let maxInflights = 5
        eachTimes = 5
    results <- forM [0..maxInflights-1] $ const newEmptyMVar
    forM_ [0..maxInflights-1] $ \idx -> forkIO $ do
      r <- replicateM eachTimes genUnique
      putMVar (results !! idx) r
    rs <- concat <$> forM [0..maxInflights-1] (takeMVar . (results !!))
    length rs `shouldBe` Set.size (Set.fromList rs)

  -- TODO
  it "setupSigsegvHandler" $ setupSigsegvHandler `shouldReturn` ()

  it "isValidateResourceName" $ do
    checkResourceName "x"         `shouldBe` Right ()
    checkResourceName "x.1"       `shouldBe` Right ()
    checkResourceName "x_-X...."  `shouldBe` Right ()
    checkResourceName "_x"        `shouldSatisfy` isLeft
    checkResourceName "zookeeper" `shouldSatisfy` isLeft
    checkResourceName "sdsdsds''" `shouldSatisfy` isLeft

  it "validate connector info" $ do
    let connectorInfo = API.Connector. Just $ pairListToStruct [("name", textToMaybeValue "test_name.....---------42")]
    checkPB connectorInfo `shouldBe` Right ()
