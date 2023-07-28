{-# LANGUAGE RecordWildCards #-}

module HStream.SQL.PlannerNew.Extra where

import           Control.Applicative          ((<|>))
import           Data.Int                     (Int64)
import qualified Data.IntMap                  as IntMap
import           Data.Text                    (Text)
import qualified Data.Time                    as Time

import           HStream.SQL.Binder
import           HStream.SQL.PlannerNew.Types

-- | Scan a 'BoundExpr' and and look up all aggregate exprs in it. It assumes
--   that aggregate can not nest.
exprAggregates :: BoundExpr -> [BoundExpr]
exprAggregates expr = case expr of
  BoundExprCast _ e _           -> exprAggregates e
  BoundExprArray _ es           -> concat (exprAggregates <$> es)
  BoundExprAccessArray _ e _    -> exprAggregates e
  BoundExprCol{}                -> []
  BoundExprConst{}              -> []
  BoundExprAggregate _ agg      -> [expr]
  BoundExprAccessJson _ _ e1 e2 -> exprAggregates e1 <> exprAggregates e2
  BoundExprBinOp _ _ e1 e2      -> exprAggregates e1 <> exprAggregates e2
  BoundExprUnaryOp _ _ e        -> exprAggregates e
  BoundExprTerOp _ _ e1 e2 e3   -> concat (exprAggregates <$> [e1,e2,e3])

-- | Give a predicate, an optional transform function and a 'RelationExpr',
-- find the first 'RelationExpr' that satisfies the predicate and apply the
-- transform function to it.
-- Return the found 'RelationExpr' and the transformed 'RelationExpr'. If
-- not found, return 'Nothing' and the original 'RelationExpr'.
scanRelationExpr :: (RelationExpr -> Bool)
                 -> Maybe (RelationExpr -> RelationExpr)
                 -> RelationExpr
                 -> (Maybe RelationExpr, RelationExpr)
scanRelationExpr p trans_m r = if p r then
  case trans_m of
    Nothing    -> (Just r, r)
    Just trans -> (Just r, trans r)   else
  case r of
    StreamScan _                -> (Nothing, r)
    LoopJoinOn s r1 r2 x1 x2 x3 -> let tup1 = scanRelationExpr p trans_m r1
                                       tup2 = scanRelationExpr p trans_m r2
                                    in (fst tup1 <|> fst tup2, LoopJoinOn s (snd tup1) (snd tup2) x1 x2 x3)
    Filter s r' x               -> let tup = scanRelationExpr p trans_m r'
                                    in (fst tup, Filter s (snd tup) x)
    Project s r' x              -> let tup = scanRelationExpr p trans_m r'
                                    in (fst tup, Project s (snd tup) x)
    Reduce s r' x1 x2 x3        -> let tup = scanRelationExpr p trans_m r'
                                    in (fst tup, Reduce s (snd tup) x1 x2 x3)
    Distinct s r'               -> let tup = scanRelationExpr p trans_m r'
                                    in (fst tup, Distinct s (snd tup))
    Union s r1 r2               -> let tup1 = scanRelationExpr p trans_m r1
                                       tup2 = scanRelationExpr p trans_m r2
                                    in (fst tup1 <|> fst tup2, Union s (snd tup1) (snd tup2))
