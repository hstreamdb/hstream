module Main (main) where

import qualified Data.Text           as T
import           HStream.SQL         (parseAndRefine)
import           HStream.SQL.AST     (RSQL (..))
import           HStream.SQL.Planner (decouple)
import           Lib                 (HasRunTest (..), mkMain)

main :: IO ()
main = mkMain PlannerTestRunner

data PlannerTestRunner = PlannerTestRunner

instance HasRunTest PlannerTestRunner where
  runTest _ sql = do
    stmt <- parseAndRefine sql
    case stmt of
      RQSelect rSelect -> pure . pure . T.pack . show $ decouple rSelect
      _                -> undefined
  getTestName _ = "planner"
