{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
#if __GLASGOW_HASKELL__ <= 708
{-# LANGUAGE OverlappingInstances #-}
#endif

{-# OPTIONS_GHC -fno-warn-incomplete-patterns #-}

-- | Pretty-printer for HStream.
--   Generated by the BNF converter.

module HStream.SQL.Print where

import Prelude
  ( ($), (.)
  , Bool(..), (==), (<)
  , Int, Integer, Double, (+), (-), (*)
  , String, (++)
  , ShowS, showChar, showString
  , all, dropWhile, elem, foldr, id, map, null, replicate, shows, span
  )
import Data.Char ( Char, isSpace )
import qualified HStream.SQL.Abs
import qualified Data.Text

-- | The top-level printing method.

printTree :: Print a => a -> String
printTree = render . prt 0

type Doc = [ShowS] -> [ShowS]

doc :: ShowS -> Doc
doc = (:)

render :: Doc -> String
render d = rend 0 (map ($ "") $ d []) "" where
  rend i = \case
    "["      :ts -> showChar '[' . rend i ts
    "("      :ts -> showChar '(' . rend i ts
    "{"      :ts -> showChar '{' . new (i+1) . rend (i+1) ts
    "}" : ";":ts -> new (i-1) . space "}" . showChar ';' . new (i-1) . rend (i-1) ts
    "}"      :ts -> new (i-1) . showChar '}' . new (i-1) . rend (i-1) ts
    [";"]        -> showChar ';'
    ";"      :ts -> showChar ';' . new i . rend i ts
    t  : ts@(p:_) | closingOrPunctuation p -> showString t . rend i ts
    t        :ts -> space t . rend i ts
    _            -> id
  new i     = showChar '\n' . replicateS (2*i) (showChar ' ') . dropWhile isSpace
  space t s =
    case (all isSpace t', null spc, null rest) of
      (True , _   , True ) -> []              -- remove trailing space
      (False, _   , True ) -> t'              -- remove trailing space
      (False, True, False) -> t' ++ ' ' : s   -- add space if none
      _                    -> t' ++ s
    where
      t'          = showString t []
      (spc, rest) = span isSpace s

  closingOrPunctuation :: String -> Bool
  closingOrPunctuation [c] = c `elem` closerOrPunct
  closingOrPunctuation _   = False

  closerOrPunct :: String
  closerOrPunct = ")],;"

parenth :: Doc -> Doc
parenth ss = doc (showChar '(') . ss . doc (showChar ')')

concatS :: [ShowS] -> ShowS
concatS = foldr (.) id

concatD :: [Doc] -> Doc
concatD = foldr (.) id

replicateS :: Int -> ShowS -> ShowS
replicateS n f = concatS (replicate n f)

-- | The printer class does the job.

class Print a where
  prt :: Int -> a -> Doc
  prtList :: Int -> [a] -> Doc
  prtList i = concatD . map (prt i)

instance {-# OVERLAPPABLE #-} Print a => Print [a] where
  prt = prtList

instance Print Char where
  prt     _ s = doc (showChar '\'' . mkEsc '\'' s . showChar '\'')
  prtList _ s = doc (showChar '"' . concatS (map (mkEsc '"') s) . showChar '"')

mkEsc :: Char -> Char -> ShowS
mkEsc q = \case
  s | s == q -> showChar '\\' . showChar s
  '\\' -> showString "\\\\"
  '\n' -> showString "\\n"
  '\t' -> showString "\\t"
  s -> showChar s

prPrec :: Int -> Int -> Doc -> Doc
prPrec i j = if j < i then parenth else id

instance Print Integer where
  prt _ x = doc (shows x)

instance Print Double where
  prt _ x = doc (shows x)

instance Print HStream.SQL.Abs.Ident where
  prt _ (HStream.SQL.Abs.Ident i) = doc $ showString (Data.Text.unpack i)
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print HStream.SQL.Abs.SString where
  prt _ (HStream.SQL.Abs.SString i) = doc $ showString (Data.Text.unpack i)

instance Print (HStream.SQL.Abs.PNInteger' a) where
  prt i = \case
    HStream.SQL.Abs.PInteger _ n -> prPrec i 0 (concatD [doc (showString "+"), prt 0 n])
    HStream.SQL.Abs.IPInteger _ n -> prPrec i 0 (concatD [prt 0 n])
    HStream.SQL.Abs.NInteger _ n -> prPrec i 0 (concatD [doc (showString "-"), prt 0 n])

instance Print (HStream.SQL.Abs.PNDouble' a) where
  prt i = \case
    HStream.SQL.Abs.PDouble _ d -> prPrec i 0 (concatD [doc (showString "+"), prt 0 d])
    HStream.SQL.Abs.IPDouble _ d -> prPrec i 0 (concatD [prt 0 d])
    HStream.SQL.Abs.NDouble _ d -> prPrec i 0 (concatD [doc (showString "-"), prt 0 d])

instance Print (HStream.SQL.Abs.SQL' a) where
  prt i = \case
    HStream.SQL.Abs.QSelect _ select -> prPrec i 0 (concatD [prt 0 select, doc (showString ";")])
    HStream.SQL.Abs.QCreate _ create -> prPrec i 0 (concatD [prt 0 create, doc (showString ";")])
    HStream.SQL.Abs.QInsert _ insert -> prPrec i 0 (concatD [prt 0 insert, doc (showString ";")])
    HStream.SQL.Abs.QShow _ showq -> prPrec i 0 (concatD [prt 0 showq, doc (showString ";")])
    HStream.SQL.Abs.QDrop _ drop -> prPrec i 0 (concatD [prt 0 drop, doc (showString ";")])

instance Print (HStream.SQL.Abs.Create' a) where
  prt i = \case
    HStream.SQL.Abs.DCreate _ id_ -> prPrec i 0 (concatD [doc (showString "CREATE"), doc (showString "STREAM"), prt 0 id_])
    HStream.SQL.Abs.CreateOp _ id_ streamoptions -> prPrec i 0 (concatD [doc (showString "CREATE"), doc (showString "STREAM"), prt 0 id_, doc (showString "WITH"), doc (showString "("), prt 0 streamoptions, doc (showString ")")])
    HStream.SQL.Abs.CreateAs _ id_ select -> prPrec i 0 (concatD [doc (showString "CREATE"), doc (showString "STREAM"), prt 0 id_, doc (showString "AS"), prt 0 select])
    HStream.SQL.Abs.CreateAsOp _ id_ select streamoptions -> prPrec i 0 (concatD [doc (showString "CREATE"), doc (showString "STREAM"), prt 0 id_, doc (showString "AS"), prt 0 select, doc (showString "WITH"), doc (showString "("), prt 0 streamoptions, doc (showString ")")])

instance Print [HStream.SQL.Abs.StreamOption' a] where
  prt = prtList

instance Print (HStream.SQL.Abs.StreamOption' a) where
  prt i = \case
    HStream.SQL.Abs.OptionRepFactor _ pninteger -> prPrec i 0 (concatD [doc (showString "REPLICATE"), doc (showString "="), prt 0 pninteger])
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print (HStream.SQL.Abs.Insert' a) where
  prt i = \case
    HStream.SQL.Abs.DInsert _ id_ ids valueexprs -> prPrec i 0 (concatD [doc (showString "INSERT"), doc (showString "INTO"), prt 0 id_, doc (showString "("), prt 0 ids, doc (showString ")"), doc (showString "VALUES"), doc (showString "("), prt 0 valueexprs, doc (showString ")")])
    HStream.SQL.Abs.InsertBinary _ id_ str -> prPrec i 0 (concatD [doc (showString "INSERT"), doc (showString "INTO"), prt 0 id_, doc (showString "VALUES"), prt 0 str])
    HStream.SQL.Abs.InsertJson _ id_ sstring -> prPrec i 0 (concatD [doc (showString "INSERT"), doc (showString "INTO"), prt 0 id_, doc (showString "VALUES"), prt 0 sstring])

instance Print [HStream.SQL.Abs.Ident] where
  prt = prtList

instance Print [HStream.SQL.Abs.ValueExpr' a] where
  prt = prtList

instance Print (HStream.SQL.Abs.ShowQ' a) where
  prt i = \case
    HStream.SQL.Abs.DShow _ showoption -> prPrec i 0 (concatD [doc (showString "SHOW"), prt 0 showoption])

instance Print (HStream.SQL.Abs.ShowOption' a) where
  prt i = \case
    HStream.SQL.Abs.ShowQueries _ -> prPrec i 0 (concatD [doc (showString "QUERIES")])
    HStream.SQL.Abs.ShowStreams _ -> prPrec i 0 (concatD [doc (showString "STREAMS")])

instance Print (HStream.SQL.Abs.Drop' a) where
  prt i = \case
    HStream.SQL.Abs.DDrop _ id_ -> prPrec i 0 (concatD [doc (showString "DROP"), doc (showString "STREAM"), prt 0 id_])
    HStream.SQL.Abs.DropIf _ id_ -> prPrec i 0 (concatD [doc (showString "DROP"), doc (showString "STREAM"), doc (showString "IF"), doc (showString "EXIST"), prt 0 id_])

instance Print (HStream.SQL.Abs.Select' a) where
  prt i = \case
    HStream.SQL.Abs.DSelect _ sel from where_ groupby having -> prPrec i 0 (concatD [prt 0 sel, prt 0 from, prt 0 where_, prt 0 groupby, prt 0 having, doc (showString "EMIT"), doc (showString "CHANGES")])

instance Print (HStream.SQL.Abs.Sel' a) where
  prt i = \case
    HStream.SQL.Abs.DSel _ sellist -> prPrec i 0 (concatD [doc (showString "SELECT"), prt 0 sellist])

instance Print (HStream.SQL.Abs.SelList' a) where
  prt i = \case
    HStream.SQL.Abs.SelListAsterisk _ -> prPrec i 0 (concatD [doc (showString "*")])
    HStream.SQL.Abs.SelListSublist _ derivedcols -> prPrec i 0 (concatD [prt 0 derivedcols])

instance Print [HStream.SQL.Abs.DerivedCol' a] where
  prt = prtList

instance Print (HStream.SQL.Abs.DerivedCol' a) where
  prt i = \case
    HStream.SQL.Abs.DerivedColSimpl _ valueexpr -> prPrec i 0 (concatD [prt 0 valueexpr])
    HStream.SQL.Abs.DerivedColAs _ valueexpr id_ -> prPrec i 0 (concatD [prt 0 valueexpr, doc (showString "AS"), prt 0 id_])
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print (HStream.SQL.Abs.From' a) where
  prt i = \case
    HStream.SQL.Abs.DFrom _ tablerefs -> prPrec i 0 (concatD [doc (showString "FROM"), prt 0 tablerefs])

instance Print [HStream.SQL.Abs.TableRef' a] where
  prt = prtList

instance Print (HStream.SQL.Abs.TableRef' a) where
  prt i = \case
    HStream.SQL.Abs.TableRefSimple _ id_ -> prPrec i 0 (concatD [prt 0 id_])
    HStream.SQL.Abs.TableRefAs _ tableref id_ -> prPrec i 0 (concatD [prt 0 tableref, doc (showString "AS"), prt 0 id_])
    HStream.SQL.Abs.TableRefJoin _ tableref1 jointype tableref2 joinwindow joincond -> prPrec i 0 (concatD [prt 0 tableref1, prt 0 jointype, doc (showString "JOIN"), prt 0 tableref2, prt 0 joinwindow, prt 0 joincond])
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print (HStream.SQL.Abs.JoinType' a) where
  prt i = \case
    HStream.SQL.Abs.JoinInner _ -> prPrec i 0 (concatD [doc (showString "INNER")])
    HStream.SQL.Abs.JoinLeft _ -> prPrec i 0 (concatD [doc (showString "LEFT")])
    HStream.SQL.Abs.JoinOuter _ -> prPrec i 0 (concatD [doc (showString "OUTER")])

instance Print (HStream.SQL.Abs.JoinWindow' a) where
  prt i = \case
    HStream.SQL.Abs.DJoinWindow _ interval -> prPrec i 0 (concatD [doc (showString "WITHIN"), doc (showString "("), prt 0 interval, doc (showString ")")])

instance Print (HStream.SQL.Abs.JoinCond' a) where
  prt i = \case
    HStream.SQL.Abs.DJoinCond _ searchcond -> prPrec i 0 (concatD [doc (showString "ON"), prt 0 searchcond])

instance Print (HStream.SQL.Abs.Where' a) where
  prt i = \case
    HStream.SQL.Abs.DWhereEmpty _ -> prPrec i 0 (concatD [])
    HStream.SQL.Abs.DWhere _ searchcond -> prPrec i 0 (concatD [doc (showString "WHERE"), prt 0 searchcond])

instance Print (HStream.SQL.Abs.GroupBy' a) where
  prt i = \case
    HStream.SQL.Abs.DGroupByEmpty _ -> prPrec i 0 (concatD [])
    HStream.SQL.Abs.DGroupBy _ grpitems -> prPrec i 0 (concatD [doc (showString "GROUP"), doc (showString "BY"), prt 0 grpitems])

instance Print [HStream.SQL.Abs.GrpItem' a] where
  prt = prtList

instance Print (HStream.SQL.Abs.GrpItem' a) where
  prt i = \case
    HStream.SQL.Abs.GrpItemCol _ colname -> prPrec i 0 (concatD [prt 0 colname])
    HStream.SQL.Abs.GrpItemWin _ window -> prPrec i 0 (concatD [prt 0 window])
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print (HStream.SQL.Abs.Window' a) where
  prt i = \case
    HStream.SQL.Abs.TumblingWindow _ interval -> prPrec i 0 (concatD [doc (showString "TUMBLING"), doc (showString "("), prt 0 interval, doc (showString ")")])
    HStream.SQL.Abs.HoppingWindow _ interval1 interval2 -> prPrec i 0 (concatD [doc (showString "HOPPING"), doc (showString "("), prt 0 interval1, doc (showString ","), prt 0 interval2, doc (showString ")")])
    HStream.SQL.Abs.SessionWindow _ interval -> prPrec i 0 (concatD [doc (showString "SESSION"), doc (showString "("), prt 0 interval, doc (showString ")")])

instance Print (HStream.SQL.Abs.Having' a) where
  prt i = \case
    HStream.SQL.Abs.DHavingEmpty _ -> prPrec i 0 (concatD [])
    HStream.SQL.Abs.DHaving _ searchcond -> prPrec i 0 (concatD [doc (showString "HAVING"), prt 0 searchcond])

instance Print (HStream.SQL.Abs.ValueExpr' a) where
  prt i = \case
    HStream.SQL.Abs.ExprOr _ valueexpr1 valueexpr2 -> prPrec i 0 (concatD [prt 0 valueexpr1, doc (showString "||"), prt 1 valueexpr2])
    HStream.SQL.Abs.ExprAnd _ valueexpr1 valueexpr2 -> prPrec i 1 (concatD [prt 1 valueexpr1, doc (showString "&&"), prt 2 valueexpr2])
    HStream.SQL.Abs.ExprAdd _ valueexpr1 valueexpr2 -> prPrec i 2 (concatD [prt 2 valueexpr1, doc (showString "+"), prt 3 valueexpr2])
    HStream.SQL.Abs.ExprSub _ valueexpr1 valueexpr2 -> prPrec i 2 (concatD [prt 2 valueexpr1, doc (showString "-"), prt 3 valueexpr2])
    HStream.SQL.Abs.ExprMul _ valueexpr1 valueexpr2 -> prPrec i 3 (concatD [prt 3 valueexpr1, doc (showString "*"), prt 4 valueexpr2])
    HStream.SQL.Abs.ExprInt _ pninteger -> prPrec i 4 (concatD [prt 0 pninteger])
    HStream.SQL.Abs.ExprNum _ pndouble -> prPrec i 4 (concatD [prt 0 pndouble])
    HStream.SQL.Abs.ExprString _ str -> prPrec i 4 (concatD [prt 0 str])
    HStream.SQL.Abs.ExprBool _ boolean -> prPrec i 4 (concatD [prt 0 boolean])
    HStream.SQL.Abs.ExprDate _ date -> prPrec i 4 (concatD [prt 0 date])
    HStream.SQL.Abs.ExprTime _ time -> prPrec i 4 (concatD [prt 0 time])
    HStream.SQL.Abs.ExprInterval _ interval -> prPrec i 4 (concatD [prt 0 interval])
    HStream.SQL.Abs.ExprArr _ valueexprs -> prPrec i 0 (concatD [doc (showString "["), prt 0 valueexprs, doc (showString "]")])
    HStream.SQL.Abs.ExprMap _ labelledvalueexprs -> prPrec i 0 (concatD [doc (showString "{"), prt 0 labelledvalueexprs, doc (showString "}")])
    HStream.SQL.Abs.ExprColName _ colname -> prPrec i 4 (concatD [prt 0 colname])
    HStream.SQL.Abs.ExprSetFunc _ setfunc -> prPrec i 4 (concatD [prt 0 setfunc])
    HStream.SQL.Abs.ExprScalarFunc _ scalarfunc -> prPrec i 0 (concatD [prt 0 scalarfunc])
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print (HStream.SQL.Abs.Boolean' a) where
  prt i = \case
    HStream.SQL.Abs.BoolTrue _ -> prPrec i 0 (concatD [doc (showString "TRUE")])
    HStream.SQL.Abs.BoolFalse _ -> prPrec i 0 (concatD [doc (showString "FALSE")])

instance Print (HStream.SQL.Abs.Date' a) where
  prt i = \case
    HStream.SQL.Abs.DDate _ pninteger1 pninteger2 pninteger3 -> prPrec i 0 (concatD [doc (showString "DATE"), prt 0 pninteger1, doc (showString "-"), prt 0 pninteger2, doc (showString "-"), prt 0 pninteger3])

instance Print (HStream.SQL.Abs.Time' a) where
  prt i = \case
    HStream.SQL.Abs.DTime _ pninteger1 pninteger2 pninteger3 -> prPrec i 0 (concatD [doc (showString "TIME"), prt 0 pninteger1, doc (showString ":"), prt 0 pninteger2, doc (showString ":"), prt 0 pninteger3])

instance Print (HStream.SQL.Abs.TimeUnit' a) where
  prt i = \case
    HStream.SQL.Abs.TimeUnitYear _ -> prPrec i 0 (concatD [doc (showString "YEAR")])
    HStream.SQL.Abs.TimeUnitMonth _ -> prPrec i 0 (concatD [doc (showString "MONTH")])
    HStream.SQL.Abs.TimeUnitWeek _ -> prPrec i 0 (concatD [doc (showString "WEEK")])
    HStream.SQL.Abs.TimeUnitDay _ -> prPrec i 0 (concatD [doc (showString "DAY")])
    HStream.SQL.Abs.TimeUnitMin _ -> prPrec i 0 (concatD [doc (showString "MINUTE")])
    HStream.SQL.Abs.TimeUnitSec _ -> prPrec i 0 (concatD [doc (showString "SECOND")])

instance Print (HStream.SQL.Abs.Interval' a) where
  prt i = \case
    HStream.SQL.Abs.DInterval _ pninteger timeunit -> prPrec i 0 (concatD [doc (showString "INTERVAL"), prt 0 pninteger, prt 0 timeunit])

instance Print [HStream.SQL.Abs.LabelledValueExpr' a] where
  prt = prtList

instance Print (HStream.SQL.Abs.LabelledValueExpr' a) where
  prt i = \case
    HStream.SQL.Abs.DLabelledValueExpr _ id_ valueexpr -> prPrec i 0 (concatD [prt 0 id_, doc (showString ":"), prt 0 valueexpr])
  prtList _ [] = concatD []
  prtList _ [x] = concatD [prt 0 x]
  prtList _ (x:xs) = concatD [prt 0 x, doc (showString ","), prt 0 xs]

instance Print (HStream.SQL.Abs.ColName' a) where
  prt i = \case
    HStream.SQL.Abs.ColNameSimple _ id_ -> prPrec i 0 (concatD [prt 0 id_])
    HStream.SQL.Abs.ColNameStream _ id_1 id_2 -> prPrec i 0 (concatD [prt 0 id_1, doc (showString "."), prt 0 id_2])
    HStream.SQL.Abs.ColNameInner _ colname id_ -> prPrec i 0 (concatD [prt 0 colname, doc (showString "["), prt 0 id_, doc (showString "]")])
    HStream.SQL.Abs.ColNameIndex _ colname pninteger -> prPrec i 0 (concatD [prt 0 colname, doc (showString "["), prt 0 pninteger, doc (showString "]")])

instance Print (HStream.SQL.Abs.SetFunc' a) where
  prt i = \case
    HStream.SQL.Abs.SetFuncCountAll _ -> prPrec i 0 (concatD [doc (showString "COUNT(*)")])
    HStream.SQL.Abs.SetFuncCount _ valueexpr -> prPrec i 0 (concatD [doc (showString "COUNT"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.SetFuncAvg _ valueexpr -> prPrec i 0 (concatD [doc (showString "AVG"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.SetFuncSum _ valueexpr -> prPrec i 0 (concatD [doc (showString "SUM"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.SetFuncMax _ valueexpr -> prPrec i 0 (concatD [doc (showString "MAX"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.SetFuncMin _ valueexpr -> prPrec i 0 (concatD [doc (showString "MIN"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])

instance Print (HStream.SQL.Abs.ScalarFunc' a) where
  prt i = \case
    HStream.SQL.Abs.ScalarFuncSin _ valueexpr -> prPrec i 0 (concatD [doc (showString "SIN"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncSinh _ valueexpr -> prPrec i 0 (concatD [doc (showString "SINH"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAsin _ valueexpr -> prPrec i 0 (concatD [doc (showString "ASIN"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAsinh _ valueexpr -> prPrec i 0 (concatD [doc (showString "ASINH"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncCos _ valueexpr -> prPrec i 0 (concatD [doc (showString "COS"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncCosh _ valueexpr -> prPrec i 0 (concatD [doc (showString "COSH"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAcos _ valueexpr -> prPrec i 0 (concatD [doc (showString "ACOS"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAcosh _ valueexpr -> prPrec i 0 (concatD [doc (showString "ACOSH"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncTan _ valueexpr -> prPrec i 0 (concatD [doc (showString "TAN"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncTanh _ valueexpr -> prPrec i 0 (concatD [doc (showString "TANH"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAtan _ valueexpr -> prPrec i 0 (concatD [doc (showString "ATAN"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAtanh _ valueexpr -> prPrec i 0 (concatD [doc (showString "ATANH"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncAbs _ valueexpr -> prPrec i 0 (concatD [doc (showString "ABS"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncCeil _ valueexpr -> prPrec i 0 (concatD [doc (showString "CEIL"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncFloor _ valueexpr -> prPrec i 0 (concatD [doc (showString "FLOOR"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncRound _ valueexpr -> prPrec i 0 (concatD [doc (showString "ROUND"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncSqrt _ valueexpr -> prPrec i 0 (concatD [doc (showString "SQRT"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncLog _ valueexpr -> prPrec i 0 (concatD [doc (showString "LOG"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncLog2 _ valueexpr -> prPrec i 0 (concatD [doc (showString "LOG2"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncLog10 _ valueexpr -> prPrec i 0 (concatD [doc (showString "LOG10"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncExp _ valueexpr -> prPrec i 0 (concatD [doc (showString "EXP"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsInt _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_INT"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsFloat _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_FLOAT"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsNum _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_NUM"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsBool _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_BOOL"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsStr _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_STR"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsMap _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_MAP"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsArr _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_ARRAY"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsDate _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_DATE"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncIsTime _ valueexpr -> prPrec i 0 (concatD [doc (showString "IS_TIME"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncToStr _ valueexpr -> prPrec i 0 (concatD [doc (showString "TO_STR"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncToLower _ valueexpr -> prPrec i 0 (concatD [doc (showString "TO_LOWER"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncToUpper _ valueexpr -> prPrec i 0 (concatD [doc (showString "TO_UPPER"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncTrim _ valueexpr -> prPrec i 0 (concatD [doc (showString "TRIM"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncLTrim _ valueexpr -> prPrec i 0 (concatD [doc (showString "LEFT_TRIM"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncRTrim _ valueexpr -> prPrec i 0 (concatD [doc (showString "RIGHT_TRIM"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncRev _ valueexpr -> prPrec i 0 (concatD [doc (showString "REVERSE"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])
    HStream.SQL.Abs.ScalarFuncStrlen _ valueexpr -> prPrec i 0 (concatD [doc (showString "STRLEN"), doc (showString "("), prt 0 valueexpr, doc (showString ")")])

instance Print (HStream.SQL.Abs.SearchCond' a) where
  prt i = \case
    HStream.SQL.Abs.CondOr _ searchcond1 searchcond2 -> prPrec i 0 (concatD [prt 0 searchcond1, doc (showString "OR"), prt 1 searchcond2])
    HStream.SQL.Abs.CondAnd _ searchcond1 searchcond2 -> prPrec i 1 (concatD [prt 1 searchcond1, doc (showString "AND"), prt 2 searchcond2])
    HStream.SQL.Abs.CondNot _ searchcond -> prPrec i 2 (concatD [doc (showString "NOT"), prt 3 searchcond])
    HStream.SQL.Abs.CondOp _ valueexpr1 compop valueexpr2 -> prPrec i 3 (concatD [prt 0 valueexpr1, prt 0 compop, prt 0 valueexpr2])
    HStream.SQL.Abs.CondBetween _ valueexpr1 valueexpr2 valueexpr3 -> prPrec i 3 (concatD [prt 0 valueexpr1, doc (showString "BETWEEN"), prt 0 valueexpr2, doc (showString "AND"), prt 0 valueexpr3])

instance Print (HStream.SQL.Abs.CompOp' a) where
  prt i = \case
    HStream.SQL.Abs.CompOpEQ _ -> prPrec i 0 (concatD [doc (showString "=")])
    HStream.SQL.Abs.CompOpNE _ -> prPrec i 0 (concatD [doc (showString "<>")])
    HStream.SQL.Abs.CompOpLT _ -> prPrec i 0 (concatD [doc (showString "<")])
    HStream.SQL.Abs.CompOpGT _ -> prPrec i 0 (concatD [doc (showString ">")])
    HStream.SQL.Abs.CompOpLEQ _ -> prPrec i 0 (concatD [doc (showString "<=")])
    HStream.SQL.Abs.CompOpGEQ _ -> prPrec i 0 (concatD [doc (showString ">=")])

