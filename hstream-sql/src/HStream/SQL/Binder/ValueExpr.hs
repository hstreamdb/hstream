{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.Binder.ValueExpr where

import           Control.Monad.State
import qualified Data.Aeson                as Aeson
import           Data.Text                 (Text)
import qualified Data.Text                 as T
import           GHC.Generics              (Generic)

import           HStream.SQL.Abs
import           HStream.SQL.Binder.Basic
import           HStream.SQL.Binder.Common
import           HStream.SQL.Exception
import           HStream.SQL.Extra

type ExprName   = String
data BoundExpr = BoundExprCast        ExprName BoundExpr BoundDataType
               | BoundExprArray       ExprName [BoundExpr]
               | BoundExprAccessArray ExprName BoundExpr BoundArrayAccessRhs
               | BoundExprCol         ExprName Text Text Int -- stream name, column name, column id
               | BoundExprConst       ExprName Constant
               | BoundExprAggregate   ExprName (Aggregate BoundExpr)
               | BoundExprAccessJson  ExprName JsonOp   BoundExpr BoundExpr
               | BoundExprBinOp       ExprName BinaryOp BoundExpr BoundExpr
               | BoundExprUnaryOp     ExprName UnaryOp  BoundExpr
               | BoundExprTerOp       ExprName TerOp    BoundExpr BoundExpr BoundExpr
               deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)

-- FIXME: a proper 'Ord' implementation
instance Ord BoundExpr where
  e1 `compare` e2 = show e1 `compare` show e2

instance HasName BoundExpr where
  getName expr = case expr of
    BoundExprCast        name _ _     -> name
    BoundExprArray       name _       -> name
    BoundExprAccessArray name _ _     -> name
    BoundExprCol         name _ _ _   -> name
    BoundExprConst       name _       -> name
    BoundExprAggregate   name _       -> name
    BoundExprAccessJson  name _ _ _   -> name
    BoundExprBinOp       name _ _ _   -> name
    BoundExprUnaryOp     name _ _     -> name
    BoundExprTerOp       name _ _ _ _ -> name

inferExprType :: BoundExpr -> BoundDataType
inferExprType = \case
  BoundExprCast _ expr typ -> typ
  BoundExprArray _ exprs -> let typs = map inferExprType exprs
                            in undefined

--------------------------------------------------------------------------------
type instance BoundType ValueExpr = BoundExpr
instance Bind ValueExpr where
  bind expr = case expr of
    -- 1. Operations
    DExprCast _ exprCast -> case exprCast of
      ExprCast1 _ e typ -> do
        e'   <- bind e
        typ' <- bind typ
        return $ BoundExprCast (trimSpacesPrint expr) e' typ'
      ExprCast2 _ e typ -> do
        e'   <- bind e
        typ' <- bind typ
        return $ BoundExprCast (trimSpacesPrint expr) e' typ'
    ExprNot _ e         -> do
      e' <- bind e
      return $ BoundExprUnaryOp (trimSpacesPrint expr) OpNot e'
    ExprAnd _ e1 e2   -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpAnd e1' e2'
    ExprOr  _ e1 e2   -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpOr e1' e2'
    ExprEQ _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpEQ e1' e2'
    ExprNEQ _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpNEQ e1' e2'
    ExprLT _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpLT e1' e2'
    ExprGT _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpGT e1' e2'
    ExprLEQ _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpLEQ e1' e2'
    ExprGEQ _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpGEQ e1' e2'
    ExprAccessArray _ e rhs -> do
      e'   <- bind e
      rhs' <- bind rhs
      return $ BoundExprAccessArray (trimSpacesPrint expr) e' rhs'
    ExprAdd _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpAdd e1' e2'
    ExprSub _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpSub e1' e2'
    ExprMul _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprBinOp (trimSpacesPrint expr) OpMul e1' e2'
    -- 2. Constants
    ExprNull _                      -> return $ BoundExprConst (trimSpacesPrint expr) ConstantNull
    ExprInt _ n                     -> bind n        >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantInt . fromInteger $ x))
    ExprNum _ n                     -> bind n        >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantFloat x))
    ExprString _ s@(SingleQuoted _) -> bind s        >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantText x))
    ExprBool _ b                    -> bind b        >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantBoolean x))
    ExprInterval _ interval         -> bind interval >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantInterval x))
    ExprDate _ date                 -> bind date     >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantDate x))
    ExprTime _ time                 -> bind time     >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantTime x))
    ExprTimestamp _ ts              -> bind ts       >>= \x -> return (BoundExprConst (trimSpacesPrint expr) (ConstantTimestamp x))

    -- 3. Arrays
    ExprArr _ es -> mapM bind es >>= \xs -> return (BoundExprArray (trimSpacesPrint expr) xs)
    -- 4. Json access & 5. Scalar functions
    ExprScalarFunc _ func -> bind func
    -- 6. Set functions
    ExprSetFunc    _ func -> bind func
    -- 7. Column access
    ExprColName _ col -> case col of
      ColNameSimple pos colIdent        -> do
        ctx <- get
        col <- bind colIdent
        case lookupColumn ctx col of
          Nothing                 ->
            throwSQLException BindException pos $ "Column " <> T.unpack col <> " not found in ctx " <> show ctx
          Just (stream, colId, _) ->
            return $ BoundExprCol (trimSpacesPrint expr) stream col colId
      ColNameStream pos hIdent colIdent -> do
        ctx <- get
        stream <- bind hIdent
        col <- bind colIdent
        case lookupColumnWithStream ctx col stream of
          Nothing         ->
            throwSQLException BindException pos $ "Column " <> T.unpack stream <> "." <> T.unpack col <> " not found in ctx " <> show ctx
          Just (originalStream, colId, _) ->
            return $ BoundExprCol (trimSpacesPrint expr) originalStream col colId
    -- 8. Expand
    ExprBetween _ between -> case between of
      BetweenAnd       _ e1 e2 e3 -> helper e1 e2 e3 OpBetweenAnd
      NotBetweenAnd    _ e1 e2 e3 -> helper e1 e2 e3 OpNotBetweenAnd
      BetweenSymAnd    _ e1 e2 e3 -> helper e1 e2 e3 OpBetweenSymAnd
      NotBetweenSymAnd _ e1 e2 e3 -> helper e1 e2 e3 OpNotBetweenSymAnd
      where
        helper e1 e2 e3 op = do
          e1' <- bind e1
          e2' <- bind e2
          e3' <- bind e3
          return $ BoundExprTerOp (trimSpacesPrint expr) op e1' e2' e3'

----------------------------------------
--             aggregate
----------------------------------------
data Aggregate expr = Nullary NullaryAggregate
                    | Unary   UnaryAggregate  expr
                    | Binary  BinaryAggregate expr expr
                    deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance (Show expr) => Show (Aggregate expr) where
  show agg = case agg of
    Nullary nullary     -> show nullary
    Unary  unary expr   -> show unary  <> "(" <> show expr <> ")"
    Binary binary e1 e2 -> show binary <> "(" <> show e1 <> ", " <> show e2 <> ")"

data NullaryAggregate = AggCountAll deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show NullaryAggregate where
  show AggCountAll = "COUNT(*)"

data UnaryAggregate = AggCount
                    | AggAvg
                    | AggSum
                    | AggMax
                    | AggMin
                    deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show UnaryAggregate where
  show agg = case agg of
    AggCount -> "COUNT"
    AggAvg   -> "AVG"
    AggSum   -> "SUM"
    AggMax   -> "MAX"
    AggMin   -> "MIN"

data BinaryAggregate = AggTopK | AggTopKDistinct deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show BinaryAggregate where
  show agg = case agg of
    AggTopK         -> "TOPK"
    AggTopKDistinct -> "TOPK_DISTINCT"

type instance BoundType SetFunc = BoundExpr
instance Bind SetFunc where
  bind func = case func of
    SetFuncCountAll _ -> return $ BoundExprAggregate (trimSpacesPrint func) (Nullary AggCountAll)
    SetFuncCount _ e  -> bind e >>= \x -> return (BoundExprAggregate (trimSpacesPrint func) (Unary AggCount x))
    SetFuncAvg _ e    -> bind e >>= \x -> return (BoundExprAggregate (trimSpacesPrint func) (Unary AggAvg   x))
    SetFuncSum _ e    -> bind e >>= \x -> return (BoundExprAggregate (trimSpacesPrint func) (Unary AggSum   x))
    SetFuncMax _ e    -> bind e >>= \x -> return (BoundExprAggregate (trimSpacesPrint func) (Unary AggMax   x))
    SetFuncMin _ e    -> bind e >>= \x -> return (BoundExprAggregate (trimSpacesPrint func) (Unary AggMin   x))
    SetFuncTopK _ e1 e2         -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprAggregate (trimSpacesPrint func) (Binary AggTopK e1' e2')
    SetFuncTopKDistinct _ e1 e2 -> do
      e1' <- bind e1
      e2' <- bind e2
      return $ BoundExprAggregate (trimSpacesPrint func) (Binary AggTopKDistinct e1' e2')

----------------------------------------
--         scalar functions
----------------------------------------
type instance BoundType ScalarFunc = BoundExpr
instance Bind ScalarFunc where
  bind func = case func of
    -- json access functions
    (ScalarFuncFieldToJson   _ e1 e2) ->
      bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprAccessJson (trimSpacesPrint func) JOpArrow         x1 x2
    (ScalarFuncFieldToText   _ e1 e2) ->
      bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprAccessJson (trimSpacesPrint func) JOpLongArrow     x1 x2
    (ScalarFuncFieldsToJson  _ e1 e2) ->
      bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprAccessJson (trimSpacesPrint func) JOpHashArrow     x1 x2
    (ScalarFuncFieldsToTexts _ e1 e2) ->
      bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprAccessJson (trimSpacesPrint func) JOpHashLongArrow x1 x2
    -- normal scalar funcBoundions
    ScalarFuncIfNull   _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpIfNull    x1 x2
    ScalarFuncNullIf   _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpNullIf    x1 x2
    ArrayFuncContain   _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpContain   x1 x2
    ArrayFuncExcept    _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpExcept    x1 x2
    ArrayFuncIntersect _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpIntersect x1 x2
    ArrayFuncRemove    _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpRemove    x1 x2
    ArrayFuncUnion     _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpUnion     x1 x2
    ArrayFuncJoinWith  _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpArrJoin'  x1 x2
    ScalarFuncDateStr  _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpDateStr   x1 x2
    ScalarFuncStrDate  _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpStrDate   x1 x2
    ScalarFuncSplit    _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpSplit     x1 x2
    ScalarFuncChunksOf _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpChunksOf  x1 x2
    ScalarFuncTake     _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpTake      x1 x2
    ScalarFuncTakeEnd  _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpTakeEnd   x1 x2
    ScalarFuncDrop     _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpDrop      x1 x2
    ScalarFuncDropEnd  _ e1 e2 -> bind e1 >>= \x1 -> bind e2 >>= \x2 -> return $ BoundExprBinOp (trimSpacesPrint func) OpDropEnd   x1 x2
    ScalarFuncSin     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpSin)
    ScalarFuncSinh    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpSinh)
    ScalarFuncAsin    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAsin)
    ScalarFuncAsinh   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAsinh)
    ScalarFuncCos     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpCos)
    ScalarFuncCosh    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpCosh)
    ScalarFuncAcos    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAcos)
    ScalarFuncAcosh   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAcosh)
    ScalarFuncTan     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpTan)
    ScalarFuncTanh    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpTanh)
    ScalarFuncAtan    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAtan)
    ScalarFuncAtanh   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAtanh)
    ScalarFuncAbs     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpAbs)
    ScalarFuncCeil    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpCeil)
    ScalarFuncFloor   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpFloor)
    ScalarFuncRound   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpRound)
    ScalarFuncSign    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpSign)
    ScalarFuncSqrt    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpSqrt)
    ScalarFuncLog     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpLog)
    ScalarFuncLog2    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpLog2)
    ScalarFuncLog10   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpLog10)
    ScalarFuncExp     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpExp)
    ScalarFuncIsInt   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsInt)
    ScalarFuncIsFloat _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsFloat)
    ScalarFuncIsBool  _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsBool)
    ScalarFuncIsStr   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsStr)
    ScalarFuncIsArr   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsArr)
    ScalarFuncIsDate  _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsDate)
    ScalarFuncIsTime  _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpIsTime)
    ScalarFuncToStr   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpToStr)
    ScalarFuncToLower _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpToLower)
    ScalarFuncToUpper _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpToUpper)
    ScalarFuncTrim    _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpTrim)
    ScalarFuncLTrim   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpLTrim)
    ScalarFuncRTrim   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpRTrim)
    ScalarFuncRev     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpReverse)
    ScalarFuncStrlen  _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpStrLen)
    ArrayFuncDistinct _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpDistinct)
    ArrayFuncLength   _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpLength)
    ArrayFuncJoin     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpArrJoin)
    ArrayFuncMax      _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpArrMax)
    ArrayFuncMin      _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpArrMin)
    ArrayFuncSort     _ e -> bind e >>= return . (BoundExprUnaryOp (trimSpacesPrint func) OpSort)
