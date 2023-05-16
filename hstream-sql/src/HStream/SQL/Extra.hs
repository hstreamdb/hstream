module HStream.SQL.Extra
  ( extractPNInteger
  , extractPNDouble
  , extractColumnIdent
  , extractHIdent
  , extractRefNameFromExpr
  , trimSpacesPrint
  ) where

import           Data.Char         (isSpace)
import qualified Data.List         as L
import           Data.Text         (Text)
import qualified Data.Text         as Text
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

extractColumnIdent :: ColumnIdent -> Text
extractColumnIdent (ColumnIdentNormal _ (Ident text)) = text
extractColumnIdent (ColumnIdentRaw _ (QuotedRaw text')) =
  Text.tail . Text.init $ text'

extractHIdent :: HIdent -> Text
extractHIdent (HIdentNormal _ (Ident text)) = text
extractHIdent (HIdentRaw _ (QuotedRaw text')) =
  Text.tail . Text.init $ text'

trimSpacesPrint :: Print a => a -> String
trimSpacesPrint = removeSpace . printTree
  where removeSpace = L.filter (not . isSpace)

--------------------------------------------------------------------------------


-- WHERE match FROM
-- | Extract stream names mentioned in an expression.
-- Return value: (anySimpleCol, [streamName])
-- For example, "s1.col1 + col2" -> (True, ["s1"])
extractRefNameFromExpr :: ValueExpr -> (Bool, [Text])
extractRefNameFromExpr (ExprCast1 _ e _) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprCast2 _ e _) = extractRefNameFromExpr e
extractRefNameFromExpr (ExprEQ _ e1 e2) =
  let (b1,l1) = extractRefNameFromExpr e1
      (b2,l2) = extractRefNameFromExpr e2
   in (b1 || b2, L.nub (l1 ++ l2))
extractRefNameFromExpr (ExprNEQ pos e1 e2) = extractRefNameFromExpr (ExprEQ pos e1 e2)
extractRefNameFromExpr (ExprLT pos e1 e2) = extractRefNameFromExpr (ExprEQ pos e1 e2)
extractRefNameFromExpr (ExprGT pos e1 e2) = extractRefNameFromExpr (ExprEQ pos e1 e2)
extractRefNameFromExpr (ExprLEQ pos e1 e2) = extractRefNameFromExpr (ExprEQ pos e1 e2)
extractRefNameFromExpr (ExprGEQ pos e1 e2) = extractRefNameFromExpr (ExprEQ pos e1 e2)
extractRefNameFromExpr (ExprAccessArray _ e _) = extractRefNameFromExpr e
-- extractRefNameFromExpr (ExprSubquery _ _) = (False, []) -- FIXME

extractRefNameFromExpr (ExprColName _ (ColNameSimple _ _)) = (True, [])
extractRefNameFromExpr (ExprColName _ (ColNameStream _ hIdent _)) = (False, [extractHIdent hIdent])
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
extractRefNameFromExpr (ExprAdd pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr (ExprSub pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr (ExprMul pos e1 e2) = extractRefNameFromExpr (ExprArr pos [e1, e2])
extractRefNameFromExpr _ = (False, [])
