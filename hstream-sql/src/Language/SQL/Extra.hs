{-# LANGUAGE OverloadedStrings #-}

module Language.SQL.Extra
  ( anyJoin
  , extractRefNames
  , extractSelRefNames
  , extractCondRefNames
  , extractRefNameFromExpr
  ) where

import qualified Data.List        as L
import           Data.Text        (Text)
import           Language.SQL.Abs

--------------------------------------------------------------------------------
anyJoin :: [TableRef a] -> Bool
anyJoin []                               = False
anyJoin ((TableRefSimple _ _) : xs)      = anyJoin xs
anyJoin ((TableRefAs _ ref _) : xs)      = anyJoin (ref : xs)
anyJoin ((TableRefJoin _ _ _ _ _ _) : _) = True

extractRefNames :: [TableRef a] -> [Text]
extractRefNames [] = []
extractRefNames ((TableRefSimple _ (Ident name)) : xs)  = name : extractRefNames xs
extractRefNames ((TableRefAs _ ref (Ident name)) : xs)  = name : extractRefNames (ref : xs)
extractRefNames ((TableRefJoin _ ref1 _ ref2 _ _) : xs) = extractRefNames (ref1 : ref2 : xs)

-- SELECT match FROM
-- | Extract stream names mentioned in DerivedCols (part of SELECT clause).
-- Return value: (anySimpleCol, [streamName])
-- For example, "SELECT s1.col1, col2" -> (True, ["s1"])
extractSelRefNames :: [DerivedCol a] -> (Bool, [Text])
extractSelRefNames [] = (False, [])
extractSelRefNames ((DerivedColSimpl _ expr) : xs) = (b1 || b2, L.nub (refs1 ++ refs2))
  where (b1, refs1) = extractRefNameFromExpr expr
        (b2, refs2) = extractSelRefNames xs
extractSelRefNames ((DerivedColAs _ expr _) : xs) = (b1 || b2, L.nub (refs1 ++ refs2))
  where (b1, refs1) = extractRefNameFromExpr expr
        (b2, refs2) = extractSelRefNames xs

-- WHERE match FROM
-- | Extract stream names mentioned in a SearchCond.
-- Return value: (anySimpleCol, [streamName])
-- For example, "s1.col1 > col2" -> (True, ["s1"])
extractCondRefNames :: SearchCond a -> (Bool, [Text])
extractCondRefNames (CondOp pos e1 _ e2)      = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractCondRefNames (CondBetween pos e1 e e2) = extractRefNameFromExpr (ExprArr pos [e1, e, e2])
extractCondRefNames (CondOr _ c1 c2)        = (b1 || b2, L.nub (refs1 ++ refs2))
  where (b1, refs1) = extractCondRefNames c1
        (b2, refs2) = extractCondRefNames c2
extractCondRefNames (CondAnd pos c1 c2)       = extractCondRefNames (CondOr pos c1 c2)
extractCondRefNames (CondNot _ c)           = extractCondRefNames c

-- | Extract stream names mentioned in an expression.
-- Return value: (anySimpleCol, [streamName])
-- For example, "s1.col1 + col2" -> (True, ["s1"])
extractRefNameFromExpr :: ValueExpr a -> (Bool, [Text])
extractRefNameFromExpr (ExprColName _ (ColNameSimple _ _)) = (True, [])
extractRefNameFromExpr (ExprColName _ (ColNameStream _ (Ident s) _)) = (False, [s])
extractRefNameFromExpr (ExprColName _ (ColNameInner pos col _)) = extractRefNameFromExpr (ExprColName pos col)
extractRefNameFromExpr (ExprColName _ (ColNameIndex pos col _)) = extractRefNameFromExpr (ExprColName pos col)
extractRefNameFromExpr (ExprSetFunc _ (SetFuncCountAll _)) = (False, [])
extractRefNameFromExpr (ExprSetFunc _ (SetFuncCount _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncAvg _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncSum _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncMax _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncMin _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprArr _ es) = (L.or (fst <$> tups), L.nub . L.concat $ snd <$> tups)
  where tups = extractRefNameFromExpr <$> es
extractRefNameFromExpr (ExprMap pos es) = extractRefNameFromExpr (ExprArr pos $ extractExpr <$> es)
  where extractExpr (DLabelledValueExpr _ _ e) = e
extractRefNameFromExpr (ExprAdd pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr (ExprSub pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr (ExprMul pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr _ = (False, [])
