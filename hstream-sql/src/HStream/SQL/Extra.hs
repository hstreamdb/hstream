module HStream.SQL.Extra
  ( extractPNInteger
  , extractPNDouble
  , extractRefNames
  , extractSelRefNames
  , extractCondRefNames
  , extractRefNameFromExpr
  , trimSpacesPrint
  ) where

import           Data.Char         (isSpace)
import qualified Data.List         as L
import           Data.Text         (Text)
import           HStream.SQL.Abs
import           HStream.SQL.Print (Print, printTree)

--------------------------------------------------------------------------------
extractPNInteger :: PNInteger -> Integer
extractPNInteger (PInteger  _ n) = n
extractPNInteger (IPInteger _ n) = n
extractPNInteger (NInteger  _ n) = (-n)

extractPNDouble :: PNDouble -> Double
extractPNDouble (PDouble  _ n) = n
extractPNDouble (IPDouble _ n) = n
extractPNDouble (NDouble  _ n) = (-n)

trimSpacesPrint :: Print a => a -> String
trimSpacesPrint = removeSpace . printTree
  where removeSpace = L.filter (not . isSpace)

--------------------------------------------------------------------------------
extractRefNames :: [TableRef] -> [Text]
extractRefNames [] = []
extractRefNames ((TableRefSimple _ (Ident name)) : xs)  = name : extractRefNames xs
extractRefNames ((TableRefSubquery _ _) : xs) = extractRefNames xs
extractRefNames ((TableRefUnion _ ref1 ref2) : xs) = extractRefNames (ref1:ref2:xs)
extractRefNames ((TableRefAs _ ref (Ident name)) : xs)  = name : extractRefNames (ref : xs)

-- SELECT match FROM
-- | Extract stream names mentioned in DerivedCols (part of SELECT clause).
-- Return value: (anySimpleCol, [streamName])
-- For example, "SELECT s1.col1, col2" -> (True, ["s1"])
extractSelRefNames :: [DerivedCol] -> (Bool, [Text])
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
extractCondRefNames :: SearchCond -> (Bool, [Text])
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
extractRefNameFromExpr :: ValueExpr -> (Bool, [Text])
extractRefNameFromExpr (ExprColName _ (ColNameSimple _ _)) = (True, [])
extractRefNameFromExpr (ExprColName _ (ColNameStream _ (Ident s) _)) = (False, [s])
extractRefNameFromExpr (ExprColName _ (ColNameInner pos col _)) = extractRefNameFromExpr (ExprColName pos col)
extractRefNameFromExpr (ExprColName _ (ColNameIndex pos col _)) = extractRefNameFromExpr (ExprColName pos col)
extractRefNameFromExpr (ExprSetFunc _ (SetFuncCountAll _)) = (False, [])
extractRefNameFromExpr (ExprSetFunc _ (SetFuncCount          _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncAvg            _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncSum            _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncMax            _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncMin            _ e)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncTopK         _ e _)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprSetFunc _ (SetFuncTopKDistinct _ e _)) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprArr _ es) = (L.or (fst <$> tups), L.nub . L.concat $ snd <$> tups)
  where tups = extractRefNameFromExpr <$> es
extractRefNameFromExpr (ExprMap pos es) = extractRefNameFromExpr (ExprArr pos $ extractExpr <$> es)
  where extractExpr (DLabelledValueExpr _ _ e) = e
extractRefNameFromExpr (ExprAdd pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr (ExprSub pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr (ExprMul pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr _ = (False, [])
