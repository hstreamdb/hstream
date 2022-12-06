module HStream.SQL.Planner where

import Data.Text (Text)

import HStream.SQL.AST

data RelationExpr
  = StreamScan Text
  | LoopJoin RelationExpr RelationExpr ScalarExpr RJoinType
  | Filter RelationExpr ScalarExpr
  | Project RelationExpr [Int]
  | Affiliate RelationExpr [ScalarExpr]
  | Reduce RelationExpr [Int] [AggregateExpr]
  | Distinct RelationExpr
  | TimeWindow RelationExpr WindowType
  | Union RelationExpr RelationExpr

data ScalarExpr
  = ColumnRef Int Int
  | Literal Constant
  | CallUnary UnaryOp ScalarExpr
  | CallBinary BinaryOp ScalarExpr ScalarExpr
  | CallCast ScalarExpr RDataType

data AggregateExpr = AggregateExpr
  { unAggregateExpr :: Aggregate ScalarExpr
  }
