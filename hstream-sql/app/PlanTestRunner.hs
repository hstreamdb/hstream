{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

#ifdef HStreamEnableSchema
import qualified Data.IntMap            as IntMap
import qualified Data.Text              as T
import           HStream.SQL            (BoundDataType (..), ColumnCatalog (..),
                                         Schema (..), parseAndBind)
import           HStream.SQL.Binder     (BoundSQL (..))
import           HStream.SQL.PlannerNew (planIO)
import           Lib                    (HasRunTest (..), mkMain)
#else
import qualified Data.Text              as T
import           HStream.SQL            (parseAndRefine)
import           HStream.SQL.AST        (RSQL (..))
import           HStream.SQL.Planner    (decouple)
import           Lib                    (HasRunTest (..), mkMain)
#endif

#ifdef HStreamEnableSchema
main :: IO ()
main = mkMain PlannerTestRunner

data PlannerTestRunner = PlannerTestRunner

instance HasRunTest PlannerTestRunner where
  runTest _ sql = do
    stmt <- parseAndBind sql mockGetSchema
    case stmt of
      BoundQSelect rSelect -> do
        plan <- planIO rSelect mockGetSchema
        pure . pure . T.pack . show $ plan
      _                -> undefined
  getTestName _ = "planner"

mockGetSchema :: T.Text -> IO (Maybe Schema)
mockGetSchema s
  | s == "s"  = pure . Just $ Schema
                { schemaOwner = "s"
                , schemaColumns = IntMap.fromList
                  [ (0, ColumnCatalog { columnId         = 0
                                      , columnName       = "x"
                                      , columnType       = BTypeInteger
                                      , columnStream     = "s"
                                      , columnStreamId   = 0
                                      , columnIsNullable = True
                                      , columnIsHidden   = False
                                      })
                  ]
                }
  | otherwise = pure Nothing

#else

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
#endif
