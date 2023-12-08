module HStream.LoggerSpec (spec) where

import           System.FilePath ((</>))
import           System.IO.Temp  (withSystemTempDirectory)
import           Test.Hspec

import qualified HStream.Logger  as Log

spec :: Spec
spec = describe "HStream.Logger" $ do
  it "LogFileRotate" $ do
    withSystemTempDirectory "HStreamLoggerTest" $ \tmpDir -> do
      let logfile = tmpDir </> "test.log"
      let logfile0 = tmpDir </> "test.log.0"

      Log.withDefaultLogger $ do
        let logType = Log.LogFileRotate (Log.FileLogSpec logfile 100 100)
        Log.setDefaultLogger Log.INFO False logType True
        Log.info "3333333333"
        Log.debug "should not be printed"
        Log.info "2222222222"
        Log.info "1111111111"

      -- >>> extract "[INFO][2023-12-06T10:19:53+0000][test/HStream/LoggerSpec.hs:16:7][thread#71]3333333333"
      -- 3333333333
      let extract line = takeWhile (/= ']') (reverse line)

      (map extract . lines) <$> readFile logfile0 `shouldReturn`
        ["3333333333", "2222222222"]
      (map extract . lines) <$> readFile logfile `shouldReturn` ["1111111111"]
