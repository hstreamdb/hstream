-- Haskell data types for the abstract syntax.
-- Generated by the BNF converter.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Language.SQL.Abs where

import Prelude (Char, Double, Integer, String, map, fmap)
import qualified Prelude as C (Eq, Ord, Show, Read, Functor)
import qualified Data.String

import qualified Data.Text

newtype Ident = Ident Data.Text.Text
  deriving (C.Eq, C.Ord, C.Show, C.Read, Data.String.IsString)

data SQL a
    = QSelect a (Select a)
    | QCreate a (Create a)
    | QInsert a (Insert a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor SQL where
    fmap f x = case x of
        QSelect a select -> QSelect (f a) (fmap f select)
        QCreate a create -> QCreate (f a) (fmap f create)
        QInsert a insert -> QInsert (f a) (fmap f insert)

data Create a
    = DCreate a Ident [SchemaElem a] [StreamOption a]
    | CreateAs a Ident (Select a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Create where
    fmap f x = case x of
        DCreate a ident schemaelems streamoptions -> DCreate (f a) ident (map (fmap f) schemaelems) (map (fmap f) streamoptions)
        CreateAs a ident select -> CreateAs (f a) ident (fmap f select)

data SchemaElem a = DSchemaElem a Ident (DataType a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor SchemaElem where
    fmap f x = case x of
        DSchemaElem a ident datatype -> DSchemaElem (f a) ident (fmap f datatype)

data DataType a
    = TypeInt a
    | TypeNum a
    | TypeString a
    | TypeDateTime a
    | TypeInterval a
    | TypeArr a
    | TypeMap a
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor DataType where
    fmap f x = case x of
        TypeInt a -> TypeInt (f a)
        TypeNum a -> TypeNum (f a)
        TypeString a -> TypeString (f a)
        TypeDateTime a -> TypeDateTime (f a)
        TypeInterval a -> TypeInterval (f a)
        TypeArr a -> TypeArr (f a)
        TypeMap a -> TypeMap (f a)

data StreamOption a
    = OptionSource a String
    | OptionFormat a String
    | OptionSink a String
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor StreamOption where
    fmap f x = case x of
        OptionSource a string -> OptionSource (f a) string
        OptionFormat a string -> OptionFormat (f a) string
        OptionSink a string -> OptionSink (f a) string

data Insert a
    = DInsert a Ident [ValueExpr a]
    | IndertWithSchema a Ident [Ident] [ValueExpr a]
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Insert where
    fmap f x = case x of
        DInsert a ident valueexprs -> DInsert (f a) ident (map (fmap f) valueexprs)
        IndertWithSchema a ident idents valueexprs -> IndertWithSchema (f a) ident idents (map (fmap f) valueexprs)

data Select a
    = DSelect a (Sel a) (From a) (Where a) (GroupBy a) (Having a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Select where
    fmap f x = case x of
        DSelect a sel from where_ groupby having -> DSelect (f a) (fmap f sel) (fmap f from) (fmap f where_) (fmap f groupby) (fmap f having)

data Sel a = DSel a (SelList a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Sel where
    fmap f x = case x of
        DSel a sellist -> DSel (f a) (fmap f sellist)

data SelList a
    = SelListAsterisk a | SelListSublist a [DerivedCol a]
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor SelList where
    fmap f x = case x of
        SelListAsterisk a -> SelListAsterisk (f a)
        SelListSublist a derivedcols -> SelListSublist (f a) (map (fmap f) derivedcols)

data DerivedCol a
    = DerivedColSimpl a (ValueExpr a)
    | DerivedColAs a (ValueExpr a) Ident
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor DerivedCol where
    fmap f x = case x of
        DerivedColSimpl a valueexpr -> DerivedColSimpl (f a) (fmap f valueexpr)
        DerivedColAs a valueexpr ident -> DerivedColAs (f a) (fmap f valueexpr) ident

data From a = DFrom a [TableRef a]
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor From where
    fmap f x = case x of
        DFrom a tablerefs -> DFrom (f a) (map (fmap f) tablerefs)

data TableRef a
    = TableRefSimple a Ident
    | TableRefAs a (TableRef a) Ident
    | TableRefJoin a (TableRef a) (JoinType a) (TableRef a) (JoinWindow a) (JoinCond a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor TableRef where
    fmap f x = case x of
        TableRefSimple a ident -> TableRefSimple (f a) ident
        TableRefAs a tableref ident -> TableRefAs (f a) (fmap f tableref) ident
        TableRefJoin a tableref1 jointype tableref2 joinwindow joincond -> TableRefJoin (f a) (fmap f tableref1) (fmap f jointype) (fmap f tableref2) (fmap f joinwindow) (fmap f joincond)

data JoinType a
    = JoinLeft a | JoinRight a | JoinFull a | JoinCross a
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor JoinType where
    fmap f x = case x of
        JoinLeft a -> JoinLeft (f a)
        JoinRight a -> JoinRight (f a)
        JoinFull a -> JoinFull (f a)
        JoinCross a -> JoinCross (f a)

data JoinWindow a = DJoinWindow a (Interval a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor JoinWindow where
    fmap f x = case x of
        DJoinWindow a interval -> DJoinWindow (f a) (fmap f interval)

data JoinCond a = DJoinCond a (SearchCond a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor JoinCond where
    fmap f x = case x of
        DJoinCond a searchcond -> DJoinCond (f a) (fmap f searchcond)

data Where a = DWhereEmpty a | DWhere a (SearchCond a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Where where
    fmap f x = case x of
        DWhereEmpty a -> DWhereEmpty (f a)
        DWhere a searchcond -> DWhere (f a) (fmap f searchcond)

data GroupBy a = DGroupByEmpty a | DGroupBy a [GrpItem a]
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor GroupBy where
    fmap f x = case x of
        DGroupByEmpty a -> DGroupByEmpty (f a)
        DGroupBy a grpitems -> DGroupBy (f a) (map (fmap f) grpitems)

data GrpItem a = GrpItemCol a (ColName a) | GrpItemWin a (Window a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor GrpItem where
    fmap f x = case x of
        GrpItemCol a colname -> GrpItemCol (f a) (fmap f colname)
        GrpItemWin a window -> GrpItemWin (f a) (fmap f window)

data Window a
    = TumblingWindow a (Interval a)
    | HoppingWindow a (Interval a) (Interval a)
    | SessionWindow a (Interval a) (Interval a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Window where
    fmap f x = case x of
        TumblingWindow a interval -> TumblingWindow (f a) (fmap f interval)
        HoppingWindow a interval1 interval2 -> HoppingWindow (f a) (fmap f interval1) (fmap f interval2)
        SessionWindow a interval1 interval2 -> SessionWindow (f a) (fmap f interval1) (fmap f interval2)

data Having a = DHavingEmpty a | DHaving a (SearchCond a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Having where
    fmap f x = case x of
        DHavingEmpty a -> DHavingEmpty (f a)
        DHaving a searchcond -> DHaving (f a) (fmap f searchcond)

data ValueExpr a
    = ExprAdd a (ValueExpr a) (ValueExpr a)
    | ExprSub a (ValueExpr a) (ValueExpr a)
    | ExprMul a (ValueExpr a) (ValueExpr a)
    | ExprDiv a (ValueExpr a) (ValueExpr a)
    | ExprInt a Integer
    | ExprNum a Double
    | ExprString a String
    | ExprDate a (Date a)
    | ExprTime a (Time a)
    | ExprInterval a (Interval a)
    | ExprArr a [ValueExpr a]
    | ExprMap a [LabelledValueExpr a]
    | ExprColName a (ColName a)
    | ExprSetFunc a (SetFunc a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor ValueExpr where
    fmap f x = case x of
        ExprAdd a valueexpr1 valueexpr2 -> ExprAdd (f a) (fmap f valueexpr1) (fmap f valueexpr2)
        ExprSub a valueexpr1 valueexpr2 -> ExprSub (f a) (fmap f valueexpr1) (fmap f valueexpr2)
        ExprMul a valueexpr1 valueexpr2 -> ExprMul (f a) (fmap f valueexpr1) (fmap f valueexpr2)
        ExprDiv a valueexpr1 valueexpr2 -> ExprDiv (f a) (fmap f valueexpr1) (fmap f valueexpr2)
        ExprInt a integer -> ExprInt (f a) integer
        ExprNum a double -> ExprNum (f a) double
        ExprString a string -> ExprString (f a) string
        ExprDate a date -> ExprDate (f a) (fmap f date)
        ExprTime a time -> ExprTime (f a) (fmap f time)
        ExprInterval a interval -> ExprInterval (f a) (fmap f interval)
        ExprArr a valueexprs -> ExprArr (f a) (map (fmap f) valueexprs)
        ExprMap a labelledvalueexprs -> ExprMap (f a) (map (fmap f) labelledvalueexprs)
        ExprColName a colname -> ExprColName (f a) (fmap f colname)
        ExprSetFunc a setfunc -> ExprSetFunc (f a) (fmap f setfunc)

data Date a = DDate a Integer Integer Integer
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Date where
    fmap f x = case x of
        DDate a integer1 integer2 integer3 -> DDate (f a) integer1 integer2 integer3

data Time a = DTime a Integer Integer Integer
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Time where
    fmap f x = case x of
        DTime a integer1 integer2 integer3 -> DTime (f a) integer1 integer2 integer3

data TimeUnit a
    = TimeUnitYear a
    | TimeUnitMonth a
    | TimeUnitWeek a
    | TimeUnitDay a
    | TimeUnitMin a
    | TimeUnitSec a
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor TimeUnit where
    fmap f x = case x of
        TimeUnitYear a -> TimeUnitYear (f a)
        TimeUnitMonth a -> TimeUnitMonth (f a)
        TimeUnitWeek a -> TimeUnitWeek (f a)
        TimeUnitDay a -> TimeUnitDay (f a)
        TimeUnitMin a -> TimeUnitMin (f a)
        TimeUnitSec a -> TimeUnitSec (f a)

data Interval a = DInterval a Integer (TimeUnit a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor Interval where
    fmap f x = case x of
        DInterval a integer timeunit -> DInterval (f a) integer (fmap f timeunit)

data LabelledValueExpr a = DLabelledValueExpr a Ident (ValueExpr a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor LabelledValueExpr where
    fmap f x = case x of
        DLabelledValueExpr a ident valueexpr -> DLabelledValueExpr (f a) ident (fmap f valueexpr)

data ColName a
    = ColNameSimple a Ident
    | ColNameStream a Ident Ident
    | ColNameInner a (ColName a) Ident
    | ColNameIndex a (ColName a) Integer
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor ColName where
    fmap f x = case x of
        ColNameSimple a ident -> ColNameSimple (f a) ident
        ColNameStream a ident1 ident2 -> ColNameStream (f a) ident1 ident2
        ColNameInner a colname ident -> ColNameInner (f a) (fmap f colname) ident
        ColNameIndex a colname integer -> ColNameIndex (f a) (fmap f colname) integer

data SetFunc a
    = SetFuncCountAll a
    | SetFuncCount a (ValueExpr a)
    | SetFuncAvg a (ValueExpr a)
    | SetFuncSum a (ValueExpr a)
    | SetFuncMax a (ValueExpr a)
    | SetFuncMin a (ValueExpr a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor SetFunc where
    fmap f x = case x of
        SetFuncCountAll a -> SetFuncCountAll (f a)
        SetFuncCount a valueexpr -> SetFuncCount (f a) (fmap f valueexpr)
        SetFuncAvg a valueexpr -> SetFuncAvg (f a) (fmap f valueexpr)
        SetFuncSum a valueexpr -> SetFuncSum (f a) (fmap f valueexpr)
        SetFuncMax a valueexpr -> SetFuncMax (f a) (fmap f valueexpr)
        SetFuncMin a valueexpr -> SetFuncMin (f a) (fmap f valueexpr)

data SearchCond a
    = CondOr a (SearchCond a) (SearchCond a)
    | CondAnd a (SearchCond a) (SearchCond a)
    | CondNot a (SearchCond a)
    | CondOp a (ValueExpr a) (CompOp a) (ValueExpr a)
    | CondBetween a (ValueExpr a) (ValueExpr a) (ValueExpr a)
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor SearchCond where
    fmap f x = case x of
        CondOr a searchcond1 searchcond2 -> CondOr (f a) (fmap f searchcond1) (fmap f searchcond2)
        CondAnd a searchcond1 searchcond2 -> CondAnd (f a) (fmap f searchcond1) (fmap f searchcond2)
        CondNot a searchcond -> CondNot (f a) (fmap f searchcond)
        CondOp a valueexpr1 compop valueexpr2 -> CondOp (f a) (fmap f valueexpr1) (fmap f compop) (fmap f valueexpr2)
        CondBetween a valueexpr1 valueexpr2 valueexpr3 -> CondBetween (f a) (fmap f valueexpr1) (fmap f valueexpr2) (fmap f valueexpr3)

data CompOp a
    = CompOpEQ a
    | CompOpNE a
    | CompOpLT a
    | CompOpGT a
    | CompOpLEQ a
    | CompOpGEQ a
  deriving (C.Eq, C.Ord, C.Show, C.Read)

instance C.Functor CompOp where
    fmap f x = case x of
        CompOpEQ a -> CompOpEQ (f a)
        CompOpNE a -> CompOpNE (f a)
        CompOpLT a -> CompOpLT (f a)
        CompOpGT a -> CompOpGT (f a)
        CompOpLEQ a -> CompOpLEQ (f a)
        CompOpGEQ a -> CompOpGEQ (f a)

