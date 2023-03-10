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

import qualified Data.List                 as L
import           Data.Scientific
import qualified Data.Text                 as T
#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST
import           HStream.SQL.Codegen.Utils

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
unaryAggInitValue AggSum   = FlowNumeral 0
unaryAggInitValue AggMax   = FlowNumeral 0
unaryAggInitValue AggMin   = FlowNumeral 0
unaryAggInitValue AggAvg   = FlowNumeral 0

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
unaryAggOpOnValue AggAvg   acc row = Left $ ERR "Avg: not supported now"

op_count :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_count (FlowInt acc_x) _ = Right $ FlowInt (acc_x + 1)
op_count _ _ = Left $ ERR "Count: internal error. Please report this as a bug"

op_sum :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_sum (FlowNumeral acc_x) (FlowInt row_x) = Right $ FlowNumeral (acc_x + fromIntegral row_x)
op_sum (FlowNumeral acc_x) (FlowFloat row_x) = Right $ FlowNumeral (acc_x + fromFloatDigits row_x)
op_sum (FlowNumeral acc_x) (FlowNumeral row_x) = Right $ FlowNumeral (acc_x + row_x)
op_sum (FlowNumeral _) _ = Left $ ERR "Sum: type mismatch (expect a numeral value)"
op_sum _ _ = Left $ ERR "Sum: internal error. Please report this as a bug"

op_max :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_max (FlowNumeral acc_x) (FlowInt row_x) = Right $ FlowNumeral (max acc_x (fromIntegral row_x))
op_max (FlowNumeral acc_x) (FlowFloat row_x) = Right $ FlowNumeral (max acc_x (fromFloatDigits row_x))
op_max (FlowNumeral acc_x) (FlowNumeral row_x) = Right $ FlowNumeral (max acc_x row_x)
op_max (FlowNumeral _) _ = Left $ ERR "Max: type mismatch (expect a numeral value)"
op_max _ _ = Left $ ERR "Max: internal error. Please report this as a bug"

op_min :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_min (FlowNumeral acc_x) (FlowInt row_x) = Right $ FlowNumeral (min acc_x (fromIntegral row_x))
op_min (FlowNumeral acc_x) (FlowFloat row_x) = Right $ FlowNumeral (min acc_x (fromFloatDigits row_x))
op_min (FlowNumeral acc_x) (FlowNumeral row_x) = Right $ FlowNumeral (min acc_x row_x)
op_min (FlowNumeral _) _ = Left $ ERR "Min: type mismatch (expect a numeral value)"
op_min _ _ = Left $ ERR "Min: internal error. Please report this as a bug"

----
binaryAggOpOnValue :: BinaryAggregate -> FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
binaryAggOpOnValue agg acc row1 row2 = undefined

op_topk :: FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_topk (FlowArray acc_x) (FlowInt k) (FlowInt row_x) =
  let arr = (L.take k) . (L.sortBy (flip compare)) $ (FlowNumeral (fromIntegral row_x)) : acc_x
   in Right $ FlowArray arr
op_topk (FlowArray acc_x) (FlowInt k) (FlowFloat row_x) =
  let arr = (L.take k) . (L.sortBy (flip compare)) $ (FlowNumeral (fromFloatDigits row_x)) : acc_x
  in Right $ FlowArray arr
op_topk (FlowArray acc_x) (FlowInt k) (FlowNumeral row_x) =
  let arr = (L.take k) . (L.sortBy (flip compare)) $ (FlowNumeral row_x) : acc_x
   in Right $ FlowArray arr
op_topk (FlowArray acc_x) (FlowInt k) _ = Left $ ERR "TopK: type mismatch (expect a numeral value)"
op_topk (FlowArray acc_x) _ _ = Left $ ERR "TopK: type mismatch (expect an integer value)"
op_topk _ _ _ = Left $ ERR "TopK: internal error. Please report this as a bug"

op_topkdistinct :: FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_topkdistinct (FlowArray acc_x) (FlowInt k) (FlowInt row_x) =
  let arr = (L.take k) . (L.sortBy (flip compare)) . L.nub $ (FlowNumeral (fromIntegral row_x)) : acc_x
   in Right $ FlowArray arr
op_topkdistinct (FlowArray acc_x) (FlowInt k) (FlowFloat row_x) =
  let arr = (L.take k) . (L.sortBy (flip compare)) . L.nub $ (FlowNumeral (fromFloatDigits row_x)) : acc_x
  in Right $ FlowArray arr
op_topkdistinct (FlowArray acc_x) (FlowInt k) (FlowNumeral row_x) =
  let arr = (L.take k) . (L.sortBy (flip compare)) . L.nub $ (FlowNumeral row_x) : acc_x
   in Right $ FlowArray arr
op_topkdistinct (FlowArray acc_x) (FlowInt k) _ = Left $ ERR "TopKDistinct: type mismatch (expect a numeral value)"
op_topkdistinct (FlowArray acc_x) _ _ = Left $ ERR "TopKDistinct: type mismatch (expect an integer value)"
op_topkdistinct _ _ _ = Left $ ERR "TopKDistinct: internal error. Please report this as a bug"

--------------------------------------------------------------------------------
aggMergeOnValue :: Aggregate expr -> FlowObject -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
aggMergeOnValue (Nullary AggCountAll) k (FlowInt n1) (FlowInt n2) = Right $ FlowInt (n1 + n2)
aggMergeOnValue (Nullary AggCountAll) k _ _ = Left $ ERR "CountAll: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggCount _) k (FlowInt n1) (FlowInt n2) = Right $ FlowInt (n1 + n2)
aggMergeOnValue (Unary AggCount _) k _ _ = Left $ ERR "Count: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggSum _) k (FlowNumeral n1) (FlowNumeral n2) = Right $ FlowNumeral (n1 + n2)
aggMergeOnValue (Unary AggSum _) k _ _ = Left $ ERR "Sum: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggMax _) k (FlowNumeral n1) (FlowNumeral n2) = Right $ FlowNumeral (max n1 n2)
aggMergeOnValue (Unary AggMax _) k _ _ = Left $ ERR "Max: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggMin _) k (FlowNumeral n1) (FlowNumeral n2) = Right $ FlowNumeral (min n1 n2)
aggMergeOnValue (Unary AggMin _) k _ _ = Left $ ERR "Min: internal error. Please report this as a bug"
aggMergeOnValue (Unary AggAvg _) k _ _ = Left $ ERR "Avg: not supported now"
aggMergeOnValue (Binary AggTopK _ _) k (FlowArray x1) (FlowArray x2) =
  let len = L.length x1
      arr = (L.take len) . (L.sortBy (flip compare)) $ x1 <> x2
   in Right $ FlowArray arr
aggMergeOnValue (Binary AggTopK _ _) k _ _ = Left $ ERR "TopK: internal error. Please report this as a bug"
aggMergeOnValue (Binary AggTopKDistinct _ _) k (FlowArray x1) (FlowArray x2) =
  let len = L.length x1
      arr = (L.take len) . (L.sortBy (flip compare)) . (L.nub) $ x1 <> x2
   in Right $ FlowArray arr
aggMergeOnValue (Binary AggTopKDistinct _ _) k _ _ = Left $ ERR "TopKDistinct: internal error. Please report this as a bug"
