{-# LANGUAGE LambdaCase #-}

module HStream.SQL.Extra
  ( extractPNInteger
  , extractPNDouble
  , extractColumnIdent
  , extractHIdent
  , extractSingleQuoted
  , extractRefNameFromExpr
  , trimSpacesPrint
  , unifyValueExprCast
  , exprBetweenVals
  , parseFlowDateValue
  , parseFlowTimeValue
  , parseFlowTimestampValue
  , parseFlowIntervalValue
  ) where

import           Data.Char                (isSpace)
import qualified Data.List                as L
import           Data.Text                (Text)
import qualified Data.Text                as Text
import qualified Data.Time                as Time
import           Data.Time.Format.ISO8601 (iso8601ParseM)
import           HStream.SQL.Abs
import           HStream.SQL.Print        (Print, printTree)

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
extractColumnIdent (ColumnIdentDoubleQuoted _ (DoubleQuoted text)) =
  Text.tail . Text.init $ text

extractHIdent :: HIdent -> Text
extractHIdent (HIdentNormal _ (Ident text)) = text
extractHIdent (HIdentDoubleQuoted _ (DoubleQuoted text)) =
  Text.tail . Text.init $ text

extractSingleQuoted :: SingleQuoted -> Text
extractSingleQuoted (SingleQuoted x) = Text.tail . Text.init $ x

trimSpacesPrint :: Print a => a -> String
trimSpacesPrint = removeSpace . printTree
  where removeSpace = L.filter (not . isSpace)

--------------------------------------------------------------------------------


-- WHERE match FROM
-- | Extract stream names mentioned in an expression.
-- Return value: (anySimpleCol, [streamName])
-- For example, "s1.col1 + col2" -> (True, ["s1"])
extractRefNameFromExpr :: ValueExpr -> (Bool, [Text])
extractRefNameFromExpr (DExprCast _ e) = case e of
  ExprCast1 _ e _ -> extractRefNameFromExpr e
  ExprCast2 _ e _ -> extractRefNameFromExpr e
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

unifyValueExprCast :: ExprCast -> (ValueExpr, DataType, BNFC'Position)
unifyValueExprCast = \case
  ExprCast1 pos x y -> (x, y, pos)
  ExprCast2 pos x y -> (x, y, pos)

exprBetweenVals :: Between -> (ValueExpr, ValueExpr, ValueExpr)
exprBetweenVals expr = case expr of
  BetweenAnd       _ x y z -> h x y z
  NotBetweenAnd    _ x y z -> h x y z
  BetweenSymAnd    _ x y z -> h x y z
  NotBetweenSymAnd _ x y z -> h x y z
  where
    h x y z = (x, y, z)

parseFlowDateValue :: Text.Text -> Maybe Time.Day
parseFlowDateValue = iso8601ParseM . Text.unpack . Text.tail . Text.init

parseFlowTimeValue :: Text.Text -> Maybe Time.TimeOfDay
parseFlowTimeValue = iso8601ParseM . Text.unpack . Text.tail . Text.init

parseFlowTimestampValue :: Text.Text -> Maybe Time.ZonedTime
parseFlowTimestampValue = iso8601ParseM . Text.unpack . Text.tail . Text.init

parseFlowIntervalValue :: Text.Text -> Maybe Time.CalendarDiffTime
parseFlowIntervalValue = iso8601ParseM . Text.unpack . Text.tail . Text.init
