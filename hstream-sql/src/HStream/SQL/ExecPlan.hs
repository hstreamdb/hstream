{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.SQL.ExecPlan
  ( ExecutionPlan(..)
  , SinkType(..)
  , ExecutionTopology(..)
  , genExecutionPlan
  ) where

import qualified Data.List                             as L
import qualified Data.Text                             as T
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Internal
import           HStream.SQL.Codegen
import           HStream.SQL.Exception
import           HStream.SQL.ExecPlan.Utils
import           Text.Printf                           (printf)

--------------------------------------------------------------------------------

data ExecutionPlan = ExecutionPlan
  { execPlanSql      :: T.Text
  , execPlanSources  :: [T.Text]
  , execPlanSink     :: SinkType
  , execPlanTopology :: ExecutionTopology
  } deriving Eq

data SinkType
  = NormalSink T.Text
  | TempSink   T.Text
  | ViewSink   T.Text
  deriving Eq

instance Show ExecutionPlan where
  show ExecutionPlan{..} =
    "===== SQL =====\n" <> T.unpack execPlanSql <> "\n\n" <>
    "=== Sources ===\n" <> L.concatMap ((<> "\n"). T.unpack) execPlanSources <> "\n" <>
    "==== Sink =====\n" <> sinkInfo <> "\n\n" <>
    "=== Topology ==\n" <> show execPlanTopology
    where
      sinkInfo = case execPlanSink of
        NormalSink sink -> T.unpack sink
        ViewSink   sink -> T.unpack sink <> " (view)"
        TempSink   sink -> T.unpack sink <> " (temp)"

newtype ExecutionTopology
  = ExecutionTopology (([T.Text], [T.Text]), [T.Text]) deriving Eq

instance Show ExecutionTopology where
  show (ExecutionTopology ((l1, l2), common)) = go l1 l2 common (1 :: Int)
    where
      go [] [] common n     =
        L.concatMap (\(i,x) -> show i <> "   " <> T.unpack x <> "\n") ([n..] `zip` common)
      go [] (y:ys) common n =
        printf "%d.2 %s\n" n y <>
        go [] ys common (n+1)
      go (x:xs) [] common n =
        printf "%d.1 %s\n" n x <>
        go xs [] common (n+1)
      go (x:xs) (y:ys) common n =
        printf "%d.1 %s\n" n x <>
        printf "%d.2 %s\n" n y <>
        go xs ys common (n+1)

genExecTopology :: Task -> ExecutionTopology
genExecTopology task =
  case allTraverses task of
    [l]     -> ExecutionTopology (([], []), l)
    [l1,l2] -> ExecutionTopology $ l1 `fuseOnEq` l2
    _       -> error "impossible happened..."

genExecutionPlan :: T.Text -> IO ExecutionPlan
genExecutionPlan sql = do
  plan <- streamCodegen sql
  case plan of
    SelectPlan sources sink builder           ->
      return $ ExecutionPlan
      { execPlanSql      = sql
      , execPlanSources  = sources
      , execPlanSink     = TempSink sink
      , execPlanTopology = genExecTopology (build builder)
      }
    CreateBySelectPlan sources sink builder _ ->
      return $ ExecutionPlan
      { execPlanSql      = sql
      , execPlanSources  = sources
      , execPlanSink     = NormalSink sink
      , execPlanTopology = genExecTopology (build builder)
      }
    CreateViewPlan _ sources sink builder _ _ ->
      return $ ExecutionPlan
      { execPlanSql      = sql
      , execPlanSources  = sources
      , execPlanSink     = ViewSink sink
      , execPlanTopology = genExecTopology (build builder)
      }
    _ -> throwSQLException GenExecPlanException Nothing
           "inconsistent method called, no execution plan generated"
