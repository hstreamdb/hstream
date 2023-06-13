module Main (main) where

import qualified Data.Text   as T
import           HStream.SQL (parseAndRefine)
import           Lib         (HasRunTest (..), mkMain)

main :: IO ()
main = mkMain SyntaxTestRunner

data SyntaxTestRunner = SyntaxTestRunner

instance HasRunTest SyntaxTestRunner where
  runTest _ = (T.pack . show <$>) . parseAndRefine
  getTestName _ = "syntax"
