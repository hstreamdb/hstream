{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}

module HStream.SQL.Codegen.Common where

import qualified Data.HashMap.Strict               as HM
import qualified Data.List                         as L
import           Data.Maybe
import qualified Data.Text                         as T

#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST
import           HStream.SQL.Codegen.AggOp
import           HStream.SQL.Codegen.BinOp
import           HStream.SQL.Codegen.Cast
import           HStream.SQL.Codegen.ColumnCatalog
import           HStream.SQL.Codegen.JsonOp
import           HStream.SQL.Codegen.TerOp
import           HStream.SQL.Codegen.UnaryOp
import           HStream.SQL.Planner
import           HStream.SQL.Planner.Pretty        ()

#ifdef HStreamUseV2Engine
#define ERROR_TYPE DiffFlowError
#define ERR RunShardError
#else
#define ERROR_TYPE HStreamProcessingError
#define ERR OperationError
#endif

scalarExprToFun :: ScalarExpr -> FlowObject -> Either ERROR_TYPE FlowValue
scalarExprToFun scalar o = case scalar of
  ColumnRef field stream_m ->
    case getField (ColumnCatalog field stream_m) o of
      Nothing    -> Left . ERR $ "Can not get column: " <> T.pack (show $ ColumnCatalog field stream_m)
      Just (_,v) -> Right v
  Literal constant -> Right $ constantToFlowValue constant
  CallUnary op scalar -> do
    v1 <- scalarExprToFun scalar o
    unaryOpOnValue op v1
  CallBinary op scalar1 scalar2 -> do
    v1 <- scalarExprToFun scalar1 o
    v2 <- scalarExprToFun scalar2 o
    binOpOnValue op v1 v2
  CallTernary op scalar1 scalar2 scalar3 -> do
    v1 <- scalarExprToFun scalar1 o
    v2 <- scalarExprToFun scalar2 o
    v3 <- scalarExprToFun scalar3 o
    terOpOnValue op v1 v2 v3
  CallCast scalar typ -> do
    v1 <- scalarExprToFun scalar o
    castOnValue typ v1
  CallJson op scalar1 scalar2 -> do
    v1 <- scalarExprToFun scalar1 o
    v2 <- scalarExprToFun scalar2 o
    jsonOpOnValue op v1 v2
  ValueArray scalars -> do
    values <- mapM (flip scalarExprToFun o) scalars
    return $ FlowArray values
  AccessArray scalar rhs -> do
    v1 <- scalarExprToFun scalar o
    case v1 of
      FlowArray arr -> case rhs of
        RArrayAccessRhsIndex n ->
          if n >= 0 && n < L.length arr then
            Right $ arr L.!! n else
            Left . ERR $ "Access array operator: out of bound"
        RArrayAccessRhsRange start_m end_m ->
          let start = fromMaybe 0 start_m
              end   = fromMaybe (maxBound :: Int) end_m
           in if start >= 0 && end < L.length arr then
                Right $ FlowArray (L.drop start (L.take (end+1) arr)) else
                Left . ERR $ "Access array operator: out of bound"
      _ -> Left . ERR $ "Can not perform AccessArray operator on value " <> T.pack (show v1)

--------------------------------------------------------------------------------
-- Aggregate
data AggregateComponent = AggregateComponent
  { aggregateInit :: FlowObject
  , aggregateF    :: FlowObject -> FlowObject -> Either (ERROR_TYPE,FlowObject) FlowObject
  , aggregateMergeF :: FlowObject -> FlowObject -> FlowObject -> Either (ERROR_TYPE,FlowObject) FlowObject
  }

composeAggs :: [AggregateComponent] -> AggregateComponent
composeAggs []   =
  AggregateComponent
  { aggregateInit = HM.empty
  , aggregateF    = \_ _ -> Right HM.empty
  , aggregateMergeF = \_ _ _ -> Right HM.empty
  }
composeAggs aggs =
  AggregateComponent
  { aggregateInit = HM.unions (L.map aggregateInit aggs)
  , aggregateF = \acc row ->
      let accs_m = map (\agg -> aggregateF agg acc row) aggs
       in foldl1 folder accs_m
  , aggregateMergeF = \k o1 o2 ->
      let accs_m = map (\agg -> aggregateMergeF agg k o1 o2) aggs
       in foldl1 folder accs_m
  }
  where folder cur x = case cur of
          Left (e,v) -> case x of
            Left (_, v') -> Left (e, HM.union v v') -- FIXME: e <> e'
            Right v'     -> Left (e, HM.union v v')
          Right v    -> case x of
            Left (e',v') -> Left (e', HM.union v v')
            Right v'     -> Right $ HM.union v v'

genAggregateComponent :: Aggregate ScalarExpr
                      -> ColumnCatalog
                      -> AggregateComponent
genAggregateComponent agg cata = case agg of
  Nullary nAgg ->
    AggregateComponent
    { aggregateInit = HM.singleton cata (nullaryAggInitValue nAgg)
    , aggregateF    = \acc _ ->
        case getField cata acc of
          Just (k, acc_v) ->
            case nullaryAggOpOnValue nAgg acc_v of
              Left e  -> Left (e, HM.fromList [(cata, FlowNull)])
              Right v -> Right $ HM.fromList [(k,v)]
          _               -> let e = ERR $ T.pack (show nAgg) <> ": internal error. Please report this as a bug"
                              in Left (e, HM.fromList [(cata, FlowNull)])
    , aggregateMergeF = \k o1 o2 ->
        case getField cata o1 of
          Just (_,v1) ->
            case getField cata o2 of
              Just (_,v2) ->
                case aggMergeOnValue agg k v1 v2 of
                  Left e  -> Left $ (e, HM.fromList [(cata,FlowNull)])
                  Right v -> Right $ HM.fromList [(cata,v)]
              _           -> let e = ERR $ T.pack (show nAgg) <> ": internal error. Please report this as a bug"
                              in Left (e, HM.fromList [(cata, FlowNull)])
          _           -> let e = ERR $ T.pack (show nAgg) <> ": internal error. Please report this as a bug"
                          in Left (e, HM.fromList [(cata, FlowNull)])
    }
  Unary uAgg expr ->
    AggregateComponent
    { aggregateInit = HM.singleton cata (unaryAggInitValue uAgg)
    , aggregateF = \acc row ->
        case getField cata acc of
          Just (k, acc_v) ->
            case scalarExprToFun expr row of
              Left e      -> Left (e, HM.fromList [(cata, FlowNull)])
              Right row_v ->
                case unaryAggOpOnValue uAgg acc_v row_v of
                  Left e  -> Left (e, HM.fromList [(cata, FlowNull)])
                  Right v -> Right $ HM.fromList [(k,v)]
          _               -> let e = ERR $ T.pack (show uAgg) <> ": internal error. Please report this as a bug"
                              in Left (e, HM.fromList [(cata, FlowNull)])
    , aggregateMergeF = \k o1 o2 ->
        case getField cata o1 of
          Just (_,v1) ->
            case getField cata o2 of
              Just (_,v2) ->
                case aggMergeOnValue agg k v1 v2 of
                  Left e  -> Left $ (e, HM.fromList [(cata,FlowNull)])
                  Right v -> Right $ HM.fromList [(cata,v)]
              _           -> let e = ERR $ T.pack (show uAgg) <> ": internal error. Please report this as a bug"
                              in Left (e, HM.fromList [(cata, FlowNull)])
          _           -> let e = ERR $ T.pack (show uAgg) <> ": internal error. Please report this as a bug"
                          in Left (e, HM.fromList [(cata, FlowNull)])
    }
  Binary bAgg exprV exprK ->
    AggregateComponent
    { aggregateInit = HM.singleton cata (binaryAggInitValue bAgg)
    , aggregateF = \acc row ->
        case getField cata acc of
          Just (_, acc_v) ->
            case scalarExprToFun exprK row of
              Left e    -> Left (e, HM.fromList [(cata, FlowNull)])
              Right k_v ->
                case scalarExprToFun exprV row of
                  Left e      ->
                    Left (e, HM.fromList [(cata, FlowNull)])
                  Right row_v ->
                    case binaryAggOpOnValue bAgg acc_v k_v row_v of
                      Left e  -> Left (e, HM.fromList [(cata, FlowNull)])
                      Right v -> Right $ HM.fromList [(cata, v)]
          _                 -> let e = ERR $ T.pack (show bAgg) <> ": internal error. Please report this as a bug"
                                in Left (e, HM.fromList [(cata, FlowNull)])
    , aggregateMergeF = \k o1 o2 ->
        case getField cata o1 of
          Just (_,v1) ->
            case getField cata o2 of
              Just (_,v2) ->
                case aggMergeOnValue agg k v1 v2 of
                  Left e  -> Left $ (e, HM.fromList [(cata,FlowNull)])
                  Right v -> Right $ HM.fromList [(cata,v)]
              _           -> let e = ERR $ T.pack (show bAgg) <> ": internal error. Please report this as a bug"
                              in Left (e, HM.fromList [(cata, FlowNull)])
          _           -> let e = ERR $ T.pack (show bAgg) <> ": internal error. Please report this as a bug"
                          in Left (e, HM.fromList [(cata, FlowNull)])
    }
