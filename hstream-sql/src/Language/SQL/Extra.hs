{-# LANGUAGE OverloadedStrings #-}

module Language.SQL.Extra where

import Language.SQL.Abs
import Data.Text (Text, pack)
import qualified Data.List as L

--------------------------------------------------------------------------------
anyJoin :: [TableRef] -> Bool
anyJoin [] = False
anyJoin ((TableRefSimple _) : xs) = anyJoin xs
anyJoin ((TableRefAs ref _) : xs) = anyJoin (ref : xs)
anyJoin ((TableRefJoin _ _ _ _ _) : _) = True

extractRefNames :: [TableRef] -> [Text]
extractRefNames [] = []
extractRefNames ((TableRefSimple (Ident name)) : xs)  = (pack name) : extractRefNames xs
extractRefNames ((TableRefAs ref (Ident name)) : xs)  = (pack name) : extractRefNames (ref : xs)
extractRefNames ((TableRefJoin ref1 _ ref2 _ _) : xs) = extractRefNames (ref1 : ref2 : xs)

-- SELECT match FROM
-- | Extract stream names mentioned in DerivedCols (part of SELECT clause).
-- Return value: (anySimpleCol, [streamName])
-- For example, "SELECT s1.col1, col2" -> (True, ["s1"])
extractSelRefNames :: [DerivedCol] -> (Bool, [Text])
extractSelRefNames [] = (False, [])
extractSelRefNames ((DerivedColSimpl expr) : xs) = (b1 || b2, L.nub (refs1 ++ refs2))
  where (b1, refs1) = extractRefNameFromExpr expr
        (b2, refs2) = extractSelRefNames xs
extractSelRefNames ((DerivedColAs expr _) : xs) = (b1 || b2, L.nub (refs1 ++ refs2))
  where (b1, refs1) = extractRefNameFromExpr expr
        (b2, refs2) = extractSelRefNames xs

-- WHERE match FROM
-- | Extract stream names mentioned in a SearchCond.
-- Return value: (anySimpleCol, [streamName])
-- For example, "s1.col1 > col2" -> (True, ["s1"])
extractCondRefNames :: SearchCond -> (Bool, [Text])
extractCondRefNames (CondOp e1 _ e2)     = extractRefNameFromExpr (ExprArr [e1, e2])
extractCondRefNames (CondBetween e1 e e2) = extractRefNameFromExpr (ExprArr [e1, e, e2])
extractCondRefNames (CondOr c1 c2)        = (b1 || b2, L.nub (refs1 ++ refs2))
  where (b1, refs1) = extractCondRefNames c1
        (b2, refs2) = extractCondRefNames c2
extractCondRefNames (CondAnd c1 c2)       = extractCondRefNames (CondOr c1 c2)
extractCondRefNames (CondNot c)           = extractCondRefNames c

-- | Extract stream names mentioned in an expression.
-- Return value: (anySimpleCol, [streamName])
-- For example, "s1.col1 + col2" -> (True, ["s1"])
extractRefNameFromExpr :: ValueExpr -> (Bool, [Text])
extractRefNameFromExpr (ExprColName (ColNameSimple _)) = (True, [])
extractRefNameFromExpr (ExprColName (ColNameStream (Ident s) _)) = (False, [pack s])
extractRefNameFromExpr (ExprColName (ColNameInner col _)) = extractRefNameFromExpr (ExprColName col)
extractRefNameFromExpr (ExprColName (ColNameIndex col _)) = extractRefNameFromExpr (ExprColName col)
extractRefNameFromExpr (ExprSetFunc SetFuncCountAll) = (False, [])
extractRefNameFromExpr (ExprSetFunc (SetFuncCount e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc (SetFuncAvg e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc (SetFuncSum e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc (SetFuncMax e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc (SetFuncMin e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprArr es) = (any (== True) (fst <$> tups), L.nub . L.concat $ snd <$> tups)
  where tups = extractRefNameFromExpr <$> es
extractRefNameFromExpr (ExprMap es) = extractRefNameFromExpr (ExprArr $ extractExpr <$> es)
  where extractExpr (DLabelledValueExpr _ e) = e
extractRefNameFromExpr (ExprAdd e1 e2) = extractRefNameFromExpr (ExprArr [e1, e2])
extractRefNameFromExpr (ExprSub e1 e2) = extractRefNameFromExpr (ExprArr [e1, e2])
extractRefNameFromExpr (ExprMul e1 e2) = extractRefNameFromExpr (ExprArr [e1, e2])
extractRefNameFromExpr (ExprDiv e1 e2) = extractRefNameFromExpr (ExprArr [e1, e2])
extractRefNameFromExpr _ = (False, [])
