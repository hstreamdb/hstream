{-# LANGUAGE CPP               #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.TerOp
  (
    terOpOnValue
    )
 where

import           GHC.Stack                (HasCallStack)
#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST
import           HStream.SQL.Rts
-- basic operators. defined in AST in schemaless version
#ifdef HStreamEnableSchema
import           HStream.SQL.Binder
#endif

#ifdef HStreamUseV2Engine
#define ERROR_TYPE DiffFlowError
#define ERR RunShardError
#else
#define ERROR_TYPE HStreamProcessingError
#define ERR OperationError
#endif


-- TODO: unify with 'op_eq', 'op_geq' and 'op_leq'
terOpOnValue :: TerOp -> FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
terOpOnValue op = case op of
  OpBetweenAnd       -> op_between
  OpNotBetweenAnd    -> ((cmpNot .) .) . op_between
  OpBetweenSymAnd    -> op_between_sym
  OpNotBetweenSymAnd -> ((cmpNot .) .) . op_between_sym

op_between :: FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_between v0 v1 v2 = case (v0, v1, v2) of
  (FlowNull, _, _) -> pure FlowNull
  (_, FlowNull, _) -> pure FlowNull
  (_, _, FlowNull) -> pure FlowNull
  (FlowInt       v0', FlowInt       v1', FlowInt       v2') -> ret v0' v1' v2'
  (FlowFloat     v0', FlowFloat     v1', FlowFloat     v2') -> ret v0' v1' v2'
  (FlowBoolean   v0', FlowBoolean   v1', FlowBoolean   v2') -> ret v0' v1' v2'
  (FlowByte      v0', FlowByte      v1', FlowByte      v2') -> ret v0' v1' v2'
  (FlowText      v0', FlowText      v1', FlowText      v2') -> ret v0' v1' v2'
  (FlowDate      v0', FlowDate      v1', FlowDate      v2') -> ret v0' v1' v2'
  (FlowTime      v0', FlowTime      v1', FlowTime      v2') -> ret v0' v1' v2'
  (FlowTimestamp v0', FlowTimestamp v1', FlowTimestamp v2') -> ret v0' v1' v2'
  (FlowInterval  v0', FlowInterval  v1', FlowInterval  v2') -> ret v0' v1' v2'
  (FlowArray     v0', FlowArray     v1', FlowArray     v2') -> ret v0' v1' v2'
  -- X: PostgreSQL does not support comparing JSON
  _ -> Left . ERR $
    "Unsupported comparison between type <" <> showTypeOfFlowValue v0 <> ">, <" <> showTypeOfFlowValue v1 <> "> and <" <> showTypeOfFlowValue v2 <> ">. "
      <> "You might need to add explicit type casts."
  where
    ret :: Ord a => a -> a -> a -> Either ERROR_TYPE FlowValue
    ret v0' v1' v2' = pure . FlowBoolean $ v1' <= v0' && v0' <= v2'

cmp2 :: FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
cmp2 v0 v1 = case (v0, v1) of
  (FlowNull, _)                          -> pure FlowNull
  (_, FlowNull)                          -> pure FlowNull
  (FlowInt       v0', FlowInt       v1') -> ret v0' v1'
  (FlowFloat     v0', FlowFloat     v1') -> ret v0' v1'
  (FlowBoolean   v0', FlowBoolean   v1') -> ret v0' v1'
  (FlowByte      v0', FlowByte      v1') -> ret v0' v1'
  (FlowText      v0', FlowText      v1') -> ret v0' v1'
  (FlowDate      v0', FlowDate      v1') -> ret v0' v1'
  (FlowTime      v0', FlowTime      v1') -> ret v0' v1'
  (FlowTimestamp v0', FlowTimestamp v1') -> ret v0' v1'
  (FlowInterval  v0', FlowInterval  v1') -> ret v0' v1'
  (FlowArray     v0', FlowArray     v1') -> ret v0' v1'
  _ -> Left . ERR $
    "Unsupported comparison between type <" <> showTypeOfFlowValue v0 <> "> and <" <> showTypeOfFlowValue v1 <> ">. "
      <> "You might need to add explicit type casts."
  where
    ret v0' v1' = pure . FlowBoolean $ v0' <= v1'

cmpNot :: Either ERROR_TYPE FlowValue -> Either ERROR_TYPE FlowValue
cmpNot xs = do
  x <- xs
  pure $ case x of
    FlowNull       -> FlowNull
    FlowBoolean x' -> FlowBoolean $ not x'
    _              -> cmpCaseThrow "op_between"

op_between_sym :: FlowValue -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
op_between_sym v0 v1 v2 = do
  x <- cmp2 v1 v2
  case x of
    FlowNull -> pure FlowNull
    FlowBoolean b -> if b
      then op_between v0 v1 v2
      else op_between v0 v2 v1
    _ -> cmpCaseThrow "cmp2"

cmpCaseThrow :: HasCallStack => String -> a
cmpCaseThrow f = error $ "INTERNAL ERROR: `" <> f <> "` should return either `FlowNull` or `FlowBoolean`"
