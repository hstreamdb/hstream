module Main (main) where

import           Control.Exception
import qualified Data.Text         as T
import           HStream.SQL       (parseAndRefine)
import           Lib               (HasRunTest (..), mkMain)

main :: IO ()
main = mkMain SyntaxTestRunner

data SyntaxTestRunner = SyntaxTestRunner

instance HasRunTest SyntaxTestRunner where
  runTest _ x = do
    res <- try @SomeException $ parseAndRefine x
    pure $ case res of
      Right ok -> Right . T.pack $ show ok
      Left err -> Left . T.pack . head . lines . show $ err
  getTestName _ = "syntax"
