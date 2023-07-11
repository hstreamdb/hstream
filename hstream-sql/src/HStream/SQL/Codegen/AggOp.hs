{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.AggOp
  ( nullaryAggInitValue
  , unaryAggInitValue
  , binaryAggInitValue

  , nullaryAggOpOnValue
  , unaryAggOpOnValue
  , binaryAggOpOnValue

  , aggMergeOnValue
  ) where

import qualified Data.List                as L
#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST

#ifdef HStreamUseV2Engine
#define ERROR_TYPE DiffFlowError
#define ERR RunShardError
#else
#define ERROR_TYPE HStreamProcessingError
#define ERR OperationError
#endif

--------------------------------------------------------------------------------
nullaryAggInitValue :: NullaryAggregate -> FlowValue
nullaryAggInitValue AggCountAll = FlowInt 0

unaryAggInitValue :: UnaryAggregate -> FlowValue
unaryAggInitValue AggCount = FlowInt 0
unaryAggInitValue AggSum   = FlowInt 0
unaryAggInitValue AggMax   = FlowInt 0
unaryAggInitValue AggMin   = FlowInt 0
unaryAggInitValue AggAvg   = FlowInt 0

binaryAggInitValue :: BinaryAggregate -> FlowValue
binaryAggInitValue AggTopK         = FlowArray []
binaryAggInitValue AggTopKDistinct = FlowArray []

--------------------------------------------------------------------------------
nullaryAggOpOnValue :: NullaryAggregate -> FlowValue -> Either ERROR_TYPE FlowValue
nullaryAggOpOnValue AggCountAll acc = op_countall acc

op_countall :: FlowValue -> Either ERROR_TYPE FlowValue
op_countall (FlowInt acc_x) = Right $ FlowInt (acc_x + 1)
op_countall _ = Left $ ERR "CountAll: internal error. Please report this as a bug"

----
unaryAggOpOnValue :: UnaryAggregate -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
unaryAggOpOnValue AggCount acc row = op_count acc row
unaryAggOpOnValue AggSum   acc row = op_sum   acc row
unaryAggOpOnValue AggMax   acc row = op_max   acc row
unaryAggOpOnValue AggMin   acc row = op_min   acc row
unaryAggOpOnValue AggAvg   _   _   = Left $ ERR "Avg: not supported now"

op_count :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_count (FlowInt acc_x) _ = Right $ FlowInt (acc_x + 1)
op_count FlowNull _ = Right FlowNull
op_count _ _ = Left $ ERR "Count: internal error. Please report this as a bug"

op_sum :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_sum (FlowInt acc_x) (FlowInt   row_x) = Right $ FlowInt (acc_x + row_x)
op_sum (FlowInt acc_x) (FlowFloat row_x) = Right $ FlowFloat (fromIntegral acc_x + row_x)
op_sum (FlowInt _) _ = Left $ ERR "Sum: type mismatch (expect a numeral value)"
op_sum (FlowFloat acc_x) (FlowInt   row_x) = Right $ FlowFloat (acc_x + fromIntegral row_x)
op_sum (FlowFloat acc_x) (FlowFloat row_x) = Right $ FlowFloat (acc_x + row_x)
op_sum (FlowFloat _) _ = Left $ ERR "Sum: type mismatch (expect a numeral value)"
op_sum FlowNull _ = Right FlowNull
op_sum _ _ = Left $ ERR "Sum: internal error. Please report this as a bug"

op_max :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_max (FlowInt acc_x) (FlowInt row_x) = Right $ FlowInt (max acc_x row_x)
op_max (FlowInt acc_x) (FlowFloat row_x) = Right $ FlowFloat (max (fromIntegral acc_x) row_x)
op_max (FlowInt _) _ = Left $ ERR "Max: type mismatch (expect a numeral value)"
op_max (FlowFloat acc_x) (FlowInt row_x) = Right $ FlowFloat (max acc_x (fromIntegral row_x))
op_max (FlowFloat acc_x) (FlowFloat row_x) = Right $ FlowFloat (max acc_x row_x)
op_max (FlowFloat _) _ = Left $ ERR "Max: type mismatch (expect a numeral value)"
op_max FlowNull _ = Right FlowNull
op_max _ _ = Left $ ERR "Max: internal error. Please report this as a bug"

op_min :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_min (FlowInt acc_x) (FlowInt row_x) = Right $ FlowInt (min acc_x row_x)
op_min (FlowInt acc_x) (FlowFloat row_x) = Right $ FlowFloat (min (fromIntegral acc_x) row_x)
op_min (FlowInt _) _ = Left $ ERR "Min: type mismatch (expect a numeral value)"
op_min (FlowFloat acc_x) (FlowInt row_x) = Right $ FlowFloat (min acc_x (fromIntegral row_x))
op_min (FlowFloat acc_x) (FlowFloat row_x) = Right $ FlowFloat (min acc_x row_x)
op_min (FlowFloat _) _ = Left $ ERR "Min: type mismatch (expect a numeral value)"
op_min FlowNull _ = Right FlowNull
op_min _ _ = Left $ ERR "Min: internal error. Please report this as a bug"

----
binaryAggOpOnValue :: BinaryAggregate -> FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
binaryAggOpOnValue _agg _acc _row1 _row2 = undefined

-- op_topk :: FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
-- op_topk (FlowArray acc_x) (FlowInt k) (FlowInt row_x) =
--   let arr = (L.take k) . (L.sortBy (flip compare)) $ (FlowFloat (fromIntegral row_x)) : acc_x
--    in Right $ FlowArray arr
-- op_topk (FlowArray acc_x) (FlowInt k) (FlowFloat row_x) =
--   let arr = (L.take k) . (L.sortBy (flip compare)) $ (FlowFloat row_x) : acc_x
--   in Right $ FlowArray arr
-- op_topk (FlowArray _) (FlowInt _) _ = Left $ ERR "TopK: type mismatch (expect a numeral value)"
-- op_topk (FlowArray _) _ _ = Left $ ERR "TopK: type mismatch (expect an integer value)"
-- op_topk FlowNull _ _ = Right FlowNull
-- op_topk _ _ _ = Left $ ERR "TopK: internal error. Please report this as a bug"

-- op_topkdistinct :: FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
-- op_topkdistinct (FlowArray acc_x) (FlowInt k) (FlowInt row_x) =
--   let arr = (L.take k) . (L.sortBy (flip compare)) . L.nub $ (FlowFloat (fromIntegral row_x)) : acc_x
--    in Right $ FlowArray arr
-- op_topkdistinct (FlowArray acc_x) (FlowInt k) (FlowFloat row_x) =
--   let arr = (L.take k) . (L.sortBy (flip compare)) . L.nub $ (FlowFloat row_x) : acc_x
--   in Right $ FlowArray arr
-- op_topkdistinct (FlowArray _) (FlowInt _) _ = Left $ ERR "TopKDistinct: type mismatch (expect a numeral value)"
-- op_topkdistinct (FlowArray _) _ _ = Left $ ERR "TopKDistinct: type mismatch (expect an integer value)"
-- op_topkdistinct FlowNull _ _ = Right FlowNull
-- op_topkdistinct _ _ _ = Left $ ERR "TopKDistinct: internal error. Please report this as a bug"

--------------------------------------------------------------------------------
aggMergeOnValue :: Aggregate expr -> FlowObject -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
aggMergeOnValue (Nullary AggCountAll) _ (FlowInt n1) (FlowInt n2) = Right $ FlowInt (n1 + n2)
aggMergeOnValue (Nullary AggCountAll) _ _ _ = Left $ ERR "CountAll: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggCount _) _ (FlowInt n1) (FlowInt n2) = Right $ FlowInt (n1 + n2)
aggMergeOnValue (Unary AggCount _) _ _ _ = Left $ ERR "Count: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggSum _) _ (FlowInt   n1) (FlowInt   n2) = Right $ FlowInt   (n1 + n2)
aggMergeOnValue (Unary AggSum _) _ (FlowInt   n1) (FlowFloat n2) = Right $ FlowFloat (fromIntegral n1 + n2)
aggMergeOnValue (Unary AggSum _) _ (FlowFloat n1) (FlowInt   n2) = Right $ FlowFloat (n1 + fromIntegral n2)
aggMergeOnValue (Unary AggSum _) _ (FlowFloat n1) (FlowFloat n2) = Right $ FlowFloat (n1 + n2)
aggMergeOnValue (Unary AggSum _) _ _ _ = Left $ ERR "Sum: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggMax _) _ (FlowInt   n1) (FlowInt   n2) = Right $ FlowInt   (max n1 n2)
aggMergeOnValue (Unary AggMax _) _ (FlowInt   n1) (FlowFloat n2) = Right $ FlowFloat (max (fromIntegral n1) n2)
aggMergeOnValue (Unary AggMax _) _ (FlowFloat n1) (FlowInt   n2) = Right $ FlowFloat (max n1 (fromIntegral n2))
aggMergeOnValue (Unary AggMax _) _ (FlowFloat n1) (FlowFloat n2) = Right $ FlowFloat (max n1 n2)
aggMergeOnValue (Unary AggMax _) _ _ _ = Left $ ERR "Max: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggMin _) _ (FlowInt   n1) (FlowInt   n2) = Right $ FlowInt   (min n1 n2)
aggMergeOnValue (Unary AggMin _) _ (FlowInt   n1) (FlowFloat n2) = Right $ FlowFloat (min (fromIntegral n1) n2)
aggMergeOnValue (Unary AggMin _) _ (FlowFloat n1) (FlowInt   n2) = Right $ FlowFloat (min n1 (fromIntegral n2))
aggMergeOnValue (Unary AggMin _) _ (FlowFloat n1) (FlowFloat n2) = Right $ FlowFloat (min n1 n2)
aggMergeOnValue (Unary AggMin _) _ _ _ = Left $ ERR "Min: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggAvg _) _ _ _ = Left $ ERR "Avg: not supported now"
aggMergeOnValue (Binary AggTopK _ _) _ (FlowArray x1) (FlowArray x2) =
  let len = L.length x1
      arr = (L.take len) . (L.sortBy (flip compare)) $ x1 <> x2
   in Right $ FlowArray arr
aggMergeOnValue (Binary AggTopK _ _) _ _ _ = Left $ ERR "TopK: internal error. Please report this as a bug"
aggMergeOnValue (Binary AggTopKDistinct _ _) _ (FlowArray x1) (FlowArray x2) =
  let len = L.length x1
      arr = (L.take len) . (L.sortBy (flip compare)) . (L.nub) $ x1 <> x2
   in Right $ FlowArray arr
aggMergeOnValue (Binary AggTopKDistinct _ _) _ _ _ = Left $ ERR "TopKDistinct: internal error. Please report this as a bug"
