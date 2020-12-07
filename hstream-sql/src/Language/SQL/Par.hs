{-# OPTIONS_GHC -w #-}
{-# OPTIONS -XMagicHash -XBangPatterns -XTypeSynonymInstances -XFlexibleInstances -cpp #-}
#if __GLASGOW_HASKELL__ >= 710
{-# OPTIONS_GHC -XPartialTypeSignatures #-}
#endif
{-# OPTIONS_GHC -fno-warn-incomplete-patterns -fno-warn-overlapping-patterns #-}
module Language.SQL.Par where
import qualified Language.SQL.Abs
import Language.SQL.Lex
import qualified Data.Text
import qualified Data.Array as Happy_Data_Array
import qualified Data.Bits as Bits
import qualified GHC.Exts as Happy_GHC_Exts
import Control.Applicative(Applicative(..))
import Control.Monad (ap)

-- parser produced by Happy Version 1.20.0

newtype HappyAbsSyn  = HappyAbsSyn HappyAny
#if __GLASGOW_HASKELL__ >= 607
type HappyAny = Happy_GHC_Exts.Any
#else
type HappyAny = forall a . a
#endif
newtype HappyWrap46 = HappyWrap46 ((Maybe (Int, Int), Language.SQL.Abs.Ident))
happyIn46 :: ((Maybe (Int, Int), Language.SQL.Abs.Ident)) -> (HappyAbsSyn )
happyIn46 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap46 x)
{-# INLINE happyIn46 #-}
happyOut46 :: (HappyAbsSyn ) -> HappyWrap46
happyOut46 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut46 #-}
newtype HappyWrap47 = HappyWrap47 ((Maybe (Int, Int), Double))
happyIn47 :: ((Maybe (Int, Int), Double)) -> (HappyAbsSyn )
happyIn47 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap47 x)
{-# INLINE happyIn47 #-}
happyOut47 :: (HappyAbsSyn ) -> HappyWrap47
happyOut47 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut47 #-}
newtype HappyWrap48 = HappyWrap48 ((Maybe (Int, Int), Integer))
happyIn48 :: ((Maybe (Int, Int), Integer)) -> (HappyAbsSyn )
happyIn48 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap48 x)
{-# INLINE happyIn48 #-}
happyOut48 :: (HappyAbsSyn ) -> HappyWrap48
happyOut48 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut48 #-}
newtype HappyWrap49 = HappyWrap49 ((Maybe (Int, Int), String))
happyIn49 :: ((Maybe (Int, Int), String)) -> (HappyAbsSyn )
happyIn49 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap49 x)
{-# INLINE happyIn49 #-}
happyOut49 :: (HappyAbsSyn ) -> HappyWrap49
happyOut49 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut49 #-}
newtype HappyWrap50 = HappyWrap50 ((Maybe (Int, Int), Language.SQL.Abs.SQL (Maybe (Int, Int))))
happyIn50 :: ((Maybe (Int, Int), Language.SQL.Abs.SQL (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn50 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap50 x)
{-# INLINE happyIn50 #-}
happyOut50 :: (HappyAbsSyn ) -> HappyWrap50
happyOut50 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut50 #-}
newtype HappyWrap51 = HappyWrap51 ((Maybe (Int, Int), Language.SQL.Abs.Create (Maybe (Int, Int))))
happyIn51 :: ((Maybe (Int, Int), Language.SQL.Abs.Create (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn51 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap51 x)
{-# INLINE happyIn51 #-}
happyOut51 :: (HappyAbsSyn ) -> HappyWrap51
happyOut51 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut51 #-}
newtype HappyWrap52 = HappyWrap52 ((Maybe (Int, Int), [Language.SQL.Abs.SchemaElem (Maybe (Int, Int))]))
happyIn52 :: ((Maybe (Int, Int), [Language.SQL.Abs.SchemaElem (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn52 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap52 x)
{-# INLINE happyIn52 #-}
happyOut52 :: (HappyAbsSyn ) -> HappyWrap52
happyOut52 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut52 #-}
newtype HappyWrap53 = HappyWrap53 ((Maybe (Int, Int), [Language.SQL.Abs.StreamOption (Maybe (Int, Int))]))
happyIn53 :: ((Maybe (Int, Int), [Language.SQL.Abs.StreamOption (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn53 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap53 x)
{-# INLINE happyIn53 #-}
happyOut53 :: (HappyAbsSyn ) -> HappyWrap53
happyOut53 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut53 #-}
newtype HappyWrap54 = HappyWrap54 ((Maybe (Int, Int), Language.SQL.Abs.SchemaElem (Maybe (Int, Int))))
happyIn54 :: ((Maybe (Int, Int), Language.SQL.Abs.SchemaElem (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn54 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap54 x)
{-# INLINE happyIn54 #-}
happyOut54 :: (HappyAbsSyn ) -> HappyWrap54
happyOut54 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut54 #-}
newtype HappyWrap55 = HappyWrap55 ((Maybe (Int, Int), Language.SQL.Abs.DataType (Maybe (Int, Int))))
happyIn55 :: ((Maybe (Int, Int), Language.SQL.Abs.DataType (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn55 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap55 x)
{-# INLINE happyIn55 #-}
happyOut55 :: (HappyAbsSyn ) -> HappyWrap55
happyOut55 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut55 #-}
newtype HappyWrap56 = HappyWrap56 ((Maybe (Int, Int), Language.SQL.Abs.StreamOption (Maybe (Int, Int))))
happyIn56 :: ((Maybe (Int, Int), Language.SQL.Abs.StreamOption (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn56 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap56 x)
{-# INLINE happyIn56 #-}
happyOut56 :: (HappyAbsSyn ) -> HappyWrap56
happyOut56 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut56 #-}
newtype HappyWrap57 = HappyWrap57 ((Maybe (Int, Int), Language.SQL.Abs.Insert (Maybe (Int, Int))))
happyIn57 :: ((Maybe (Int, Int), Language.SQL.Abs.Insert (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn57 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap57 x)
{-# INLINE happyIn57 #-}
happyOut57 :: (HappyAbsSyn ) -> HappyWrap57
happyOut57 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut57 #-}
newtype HappyWrap58 = HappyWrap58 ((Maybe (Int, Int), [Language.SQL.Abs.ValueExpr (Maybe (Int, Int))]))
happyIn58 :: ((Maybe (Int, Int), [Language.SQL.Abs.ValueExpr (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn58 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap58 x)
{-# INLINE happyIn58 #-}
happyOut58 :: (HappyAbsSyn ) -> HappyWrap58
happyOut58 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut58 #-}
newtype HappyWrap59 = HappyWrap59 ((Maybe (Int, Int), [Language.SQL.Abs.Ident]))
happyIn59 :: ((Maybe (Int, Int), [Language.SQL.Abs.Ident])) -> (HappyAbsSyn )
happyIn59 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap59 x)
{-# INLINE happyIn59 #-}
happyOut59 :: (HappyAbsSyn ) -> HappyWrap59
happyOut59 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut59 #-}
newtype HappyWrap60 = HappyWrap60 ((Maybe (Int, Int), Language.SQL.Abs.Select (Maybe (Int, Int))))
happyIn60 :: ((Maybe (Int, Int), Language.SQL.Abs.Select (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn60 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap60 x)
{-# INLINE happyIn60 #-}
happyOut60 :: (HappyAbsSyn ) -> HappyWrap60
happyOut60 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut60 #-}
newtype HappyWrap61 = HappyWrap61 ((Maybe (Int, Int), Language.SQL.Abs.Sel (Maybe (Int, Int))))
happyIn61 :: ((Maybe (Int, Int), Language.SQL.Abs.Sel (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn61 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap61 x)
{-# INLINE happyIn61 #-}
happyOut61 :: (HappyAbsSyn ) -> HappyWrap61
happyOut61 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut61 #-}
newtype HappyWrap62 = HappyWrap62 ((Maybe (Int, Int), Language.SQL.Abs.SelList (Maybe (Int, Int))))
happyIn62 :: ((Maybe (Int, Int), Language.SQL.Abs.SelList (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn62 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap62 x)
{-# INLINE happyIn62 #-}
happyOut62 :: (HappyAbsSyn ) -> HappyWrap62
happyOut62 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut62 #-}
newtype HappyWrap63 = HappyWrap63 ((Maybe (Int, Int), [Language.SQL.Abs.DerivedCol (Maybe (Int, Int))]))
happyIn63 :: ((Maybe (Int, Int), [Language.SQL.Abs.DerivedCol (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn63 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap63 x)
{-# INLINE happyIn63 #-}
happyOut63 :: (HappyAbsSyn ) -> HappyWrap63
happyOut63 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut63 #-}
newtype HappyWrap64 = HappyWrap64 ((Maybe (Int, Int), Language.SQL.Abs.DerivedCol (Maybe (Int, Int))))
happyIn64 :: ((Maybe (Int, Int), Language.SQL.Abs.DerivedCol (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn64 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap64 x)
{-# INLINE happyIn64 #-}
happyOut64 :: (HappyAbsSyn ) -> HappyWrap64
happyOut64 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut64 #-}
newtype HappyWrap65 = HappyWrap65 ((Maybe (Int, Int), Language.SQL.Abs.From (Maybe (Int, Int))))
happyIn65 :: ((Maybe (Int, Int), Language.SQL.Abs.From (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn65 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap65 x)
{-# INLINE happyIn65 #-}
happyOut65 :: (HappyAbsSyn ) -> HappyWrap65
happyOut65 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut65 #-}
newtype HappyWrap66 = HappyWrap66 ((Maybe (Int, Int), [Language.SQL.Abs.TableRef (Maybe (Int, Int))]))
happyIn66 :: ((Maybe (Int, Int), [Language.SQL.Abs.TableRef (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn66 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap66 x)
{-# INLINE happyIn66 #-}
happyOut66 :: (HappyAbsSyn ) -> HappyWrap66
happyOut66 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut66 #-}
newtype HappyWrap67 = HappyWrap67 ((Maybe (Int, Int), Language.SQL.Abs.TableRef (Maybe (Int, Int))))
happyIn67 :: ((Maybe (Int, Int), Language.SQL.Abs.TableRef (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn67 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap67 x)
{-# INLINE happyIn67 #-}
happyOut67 :: (HappyAbsSyn ) -> HappyWrap67
happyOut67 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut67 #-}
newtype HappyWrap68 = HappyWrap68 ((Maybe (Int, Int), Language.SQL.Abs.JoinType (Maybe (Int, Int))))
happyIn68 :: ((Maybe (Int, Int), Language.SQL.Abs.JoinType (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn68 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap68 x)
{-# INLINE happyIn68 #-}
happyOut68 :: (HappyAbsSyn ) -> HappyWrap68
happyOut68 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut68 #-}
newtype HappyWrap69 = HappyWrap69 ((Maybe (Int, Int), Language.SQL.Abs.JoinWindow (Maybe (Int, Int))))
happyIn69 :: ((Maybe (Int, Int), Language.SQL.Abs.JoinWindow (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn69 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap69 x)
{-# INLINE happyIn69 #-}
happyOut69 :: (HappyAbsSyn ) -> HappyWrap69
happyOut69 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut69 #-}
newtype HappyWrap70 = HappyWrap70 ((Maybe (Int, Int), Language.SQL.Abs.JoinCond (Maybe (Int, Int))))
happyIn70 :: ((Maybe (Int, Int), Language.SQL.Abs.JoinCond (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn70 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap70 x)
{-# INLINE happyIn70 #-}
happyOut70 :: (HappyAbsSyn ) -> HappyWrap70
happyOut70 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut70 #-}
newtype HappyWrap71 = HappyWrap71 ((Maybe (Int, Int), Language.SQL.Abs.Where (Maybe (Int, Int))))
happyIn71 :: ((Maybe (Int, Int), Language.SQL.Abs.Where (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn71 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap71 x)
{-# INLINE happyIn71 #-}
happyOut71 :: (HappyAbsSyn ) -> HappyWrap71
happyOut71 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut71 #-}
newtype HappyWrap72 = HappyWrap72 ((Maybe (Int, Int), Language.SQL.Abs.GroupBy (Maybe (Int, Int))))
happyIn72 :: ((Maybe (Int, Int), Language.SQL.Abs.GroupBy (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn72 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap72 x)
{-# INLINE happyIn72 #-}
happyOut72 :: (HappyAbsSyn ) -> HappyWrap72
happyOut72 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut72 #-}
newtype HappyWrap73 = HappyWrap73 ((Maybe (Int, Int), [Language.SQL.Abs.GrpItem (Maybe (Int, Int))]))
happyIn73 :: ((Maybe (Int, Int), [Language.SQL.Abs.GrpItem (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn73 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap73 x)
{-# INLINE happyIn73 #-}
happyOut73 :: (HappyAbsSyn ) -> HappyWrap73
happyOut73 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut73 #-}
newtype HappyWrap74 = HappyWrap74 ((Maybe (Int, Int), Language.SQL.Abs.GrpItem (Maybe (Int, Int))))
happyIn74 :: ((Maybe (Int, Int), Language.SQL.Abs.GrpItem (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn74 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap74 x)
{-# INLINE happyIn74 #-}
happyOut74 :: (HappyAbsSyn ) -> HappyWrap74
happyOut74 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut74 #-}
newtype HappyWrap75 = HappyWrap75 ((Maybe (Int, Int), Language.SQL.Abs.Window (Maybe (Int, Int))))
happyIn75 :: ((Maybe (Int, Int), Language.SQL.Abs.Window (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn75 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap75 x)
{-# INLINE happyIn75 #-}
happyOut75 :: (HappyAbsSyn ) -> HappyWrap75
happyOut75 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut75 #-}
newtype HappyWrap76 = HappyWrap76 ((Maybe (Int, Int), Language.SQL.Abs.Having (Maybe (Int, Int))))
happyIn76 :: ((Maybe (Int, Int), Language.SQL.Abs.Having (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn76 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap76 x)
{-# INLINE happyIn76 #-}
happyOut76 :: (HappyAbsSyn ) -> HappyWrap76
happyOut76 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut76 #-}
newtype HappyWrap77 = HappyWrap77 ((Maybe (Int, Int), Language.SQL.Abs.ValueExpr (Maybe (Int, Int))))
happyIn77 :: ((Maybe (Int, Int), Language.SQL.Abs.ValueExpr (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn77 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap77 x)
{-# INLINE happyIn77 #-}
happyOut77 :: (HappyAbsSyn ) -> HappyWrap77
happyOut77 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut77 #-}
newtype HappyWrap78 = HappyWrap78 ((Maybe (Int, Int), Language.SQL.Abs.ValueExpr (Maybe (Int, Int))))
happyIn78 :: ((Maybe (Int, Int), Language.SQL.Abs.ValueExpr (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn78 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap78 x)
{-# INLINE happyIn78 #-}
happyOut78 :: (HappyAbsSyn ) -> HappyWrap78
happyOut78 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut78 #-}
newtype HappyWrap79 = HappyWrap79 ((Maybe (Int, Int), Language.SQL.Abs.ValueExpr (Maybe (Int, Int))))
happyIn79 :: ((Maybe (Int, Int), Language.SQL.Abs.ValueExpr (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn79 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap79 x)
{-# INLINE happyIn79 #-}
happyOut79 :: (HappyAbsSyn ) -> HappyWrap79
happyOut79 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut79 #-}
newtype HappyWrap80 = HappyWrap80 ((Maybe (Int, Int), Language.SQL.Abs.Date (Maybe (Int, Int))))
happyIn80 :: ((Maybe (Int, Int), Language.SQL.Abs.Date (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn80 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap80 x)
{-# INLINE happyIn80 #-}
happyOut80 :: (HappyAbsSyn ) -> HappyWrap80
happyOut80 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut80 #-}
newtype HappyWrap81 = HappyWrap81 ((Maybe (Int, Int), Language.SQL.Abs.Time (Maybe (Int, Int))))
happyIn81 :: ((Maybe (Int, Int), Language.SQL.Abs.Time (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn81 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap81 x)
{-# INLINE happyIn81 #-}
happyOut81 :: (HappyAbsSyn ) -> HappyWrap81
happyOut81 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut81 #-}
newtype HappyWrap82 = HappyWrap82 ((Maybe (Int, Int), Language.SQL.Abs.TimeUnit (Maybe (Int, Int))))
happyIn82 :: ((Maybe (Int, Int), Language.SQL.Abs.TimeUnit (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn82 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap82 x)
{-# INLINE happyIn82 #-}
happyOut82 :: (HappyAbsSyn ) -> HappyWrap82
happyOut82 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut82 #-}
newtype HappyWrap83 = HappyWrap83 ((Maybe (Int, Int), Language.SQL.Abs.Interval (Maybe (Int, Int))))
happyIn83 :: ((Maybe (Int, Int), Language.SQL.Abs.Interval (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn83 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap83 x)
{-# INLINE happyIn83 #-}
happyOut83 :: (HappyAbsSyn ) -> HappyWrap83
happyOut83 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut83 #-}
newtype HappyWrap84 = HappyWrap84 ((Maybe (Int, Int), [Language.SQL.Abs.LabelledValueExpr (Maybe (Int, Int))]))
happyIn84 :: ((Maybe (Int, Int), [Language.SQL.Abs.LabelledValueExpr (Maybe (Int, Int))])) -> (HappyAbsSyn )
happyIn84 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap84 x)
{-# INLINE happyIn84 #-}
happyOut84 :: (HappyAbsSyn ) -> HappyWrap84
happyOut84 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut84 #-}
newtype HappyWrap85 = HappyWrap85 ((Maybe (Int, Int), Language.SQL.Abs.LabelledValueExpr (Maybe (Int, Int))))
happyIn85 :: ((Maybe (Int, Int), Language.SQL.Abs.LabelledValueExpr (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn85 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap85 x)
{-# INLINE happyIn85 #-}
happyOut85 :: (HappyAbsSyn ) -> HappyWrap85
happyOut85 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut85 #-}
newtype HappyWrap86 = HappyWrap86 ((Maybe (Int, Int), Language.SQL.Abs.ColName (Maybe (Int, Int))))
happyIn86 :: ((Maybe (Int, Int), Language.SQL.Abs.ColName (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn86 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap86 x)
{-# INLINE happyIn86 #-}
happyOut86 :: (HappyAbsSyn ) -> HappyWrap86
happyOut86 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut86 #-}
newtype HappyWrap87 = HappyWrap87 ((Maybe (Int, Int), Language.SQL.Abs.SetFunc (Maybe (Int, Int))))
happyIn87 :: ((Maybe (Int, Int), Language.SQL.Abs.SetFunc (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn87 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap87 x)
{-# INLINE happyIn87 #-}
happyOut87 :: (HappyAbsSyn ) -> HappyWrap87
happyOut87 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut87 #-}
newtype HappyWrap88 = HappyWrap88 ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int))))
happyIn88 :: ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn88 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap88 x)
{-# INLINE happyIn88 #-}
happyOut88 :: (HappyAbsSyn ) -> HappyWrap88
happyOut88 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut88 #-}
newtype HappyWrap89 = HappyWrap89 ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int))))
happyIn89 :: ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn89 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap89 x)
{-# INLINE happyIn89 #-}
happyOut89 :: (HappyAbsSyn ) -> HappyWrap89
happyOut89 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut89 #-}
newtype HappyWrap90 = HappyWrap90 ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int))))
happyIn90 :: ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn90 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap90 x)
{-# INLINE happyIn90 #-}
happyOut90 :: (HappyAbsSyn ) -> HappyWrap90
happyOut90 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut90 #-}
newtype HappyWrap91 = HappyWrap91 ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int))))
happyIn91 :: ((Maybe (Int, Int), Language.SQL.Abs.SearchCond (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn91 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap91 x)
{-# INLINE happyIn91 #-}
happyOut91 :: (HappyAbsSyn ) -> HappyWrap91
happyOut91 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut91 #-}
newtype HappyWrap92 = HappyWrap92 ((Maybe (Int, Int), Language.SQL.Abs.CompOp (Maybe (Int, Int))))
happyIn92 :: ((Maybe (Int, Int), Language.SQL.Abs.CompOp (Maybe (Int, Int)))) -> (HappyAbsSyn )
happyIn92 x = Happy_GHC_Exts.unsafeCoerce# (HappyWrap92 x)
{-# INLINE happyIn92 #-}
happyOut92 :: (HappyAbsSyn ) -> HappyWrap92
happyOut92 x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOut92 #-}
happyInTok :: (Token) -> (HappyAbsSyn )
happyInTok x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyInTok #-}
happyOutTok :: (HappyAbsSyn ) -> (Token)
happyOutTok x = Happy_GHC_Exts.unsafeCoerce# x
{-# INLINE happyOutTok #-}


happyExpList :: HappyAddr
happyExpList = HappyA# "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x80\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x80\x00\x23\x08\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x50\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x08\x10\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x02\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x02\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x02\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x83\x00\x44\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x0c\x00\xc0\x00\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc0\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\xc2\x0f\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x20\x08\x10\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x40\x20\x08\x10\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x02\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x50\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x80\x00\x23\x08\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x02\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x02\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x04\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x83\x00\x44\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\xc2\x0f\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x02\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x40\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x40\x20\x08\x10\x40\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x80\x4c\x00\xc2\x00\xc0\x80\x7a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"#

{-# NOINLINE happyExpListPerState #-}
happyExpListPerState st =
    token_strs_expected
  where token_strs = ["error","%dummy","%start_pSQL_internal","%start_pCreate_internal","%start_pListSchemaElem_internal","%start_pListStreamOption_internal","%start_pSchemaElem_internal","%start_pDataType_internal","%start_pStreamOption_internal","%start_pInsert_internal","%start_pListValueExpr_internal","%start_pListIdent_internal","%start_pSelect_internal","%start_pSel_internal","%start_pSelList_internal","%start_pListDerivedCol_internal","%start_pDerivedCol_internal","%start_pFrom_internal","%start_pListTableRef_internal","%start_pTableRef_internal","%start_pJoinType_internal","%start_pJoinWindow_internal","%start_pJoinCond_internal","%start_pWhere_internal","%start_pGroupBy_internal","%start_pListGrpItem_internal","%start_pGrpItem_internal","%start_pWindow_internal","%start_pHaving_internal","%start_pValueExpr_internal","%start_pValueExpr1_internal","%start_pValueExpr2_internal","%start_pDate_internal","%start_pTime_internal","%start_pTimeUnit_internal","%start_pInterval_internal","%start_pListLabelledValueExpr_internal","%start_pLabelledValueExpr_internal","%start_pColName_internal","%start_pSetFunc_internal","%start_pSearchCond_internal","%start_pSearchCond1_internal","%start_pSearchCond2_internal","%start_pSearchCond3_internal","%start_pCompOp_internal","Ident","Double","Integer","String","SQL","Create","ListSchemaElem","ListStreamOption","SchemaElem","DataType","StreamOption","Insert","ListValueExpr","ListIdent","Select","Sel","SelList","ListDerivedCol","DerivedCol","From","ListTableRef","TableRef","JoinType","JoinWindow","JoinCond","Where","GroupBy","ListGrpItem","GrpItem","Window","Having","ValueExpr","ValueExpr1","ValueExpr2","Date","Time","TimeUnit","Interval","ListLabelledValueExpr","LabelledValueExpr","ColName","SetFunc","SearchCond","SearchCond1","SearchCond2","SearchCond3","CompOp","'('","')'","'*'","'+'","','","'-'","'.'","'/'","':'","';'","'<'","'<='","'<>'","'='","'>'","'>='","'AND'","'ARRAY'","'AS'","'AVG'","'BETWEEN'","'BY'","'COUNT'","'COUNT(*)'","'CREATE'","'CROSS'","'DATE'","'DATETIME'","'DAY'","'FORMAT'","'FROM'","'FULL'","'GROUP'","'HAVING'","'HOP'","'INSERT'","'INT'","'INTERVAL'","'INTO'","'JOIN'","'LEFT'","'MAP'","'MAX'","'MIN'","'MINUTE'","'MONTH'","'NOT'","'NUMBER'","'ON'","'OR'","'RIGHT'","'SECOND'","'SELECT'","'SESSION'","'SINK'","'SOURCE'","'STREAM'","'STRING'","'SUM'","'TIME'","'TUMBLE'","'VALUES'","'WEEK'","'WHERE'","'WITH'","'WITHIN'","'YEAR'","'['","']'","'{'","'}'","L_ident","L_doubl","L_integ","L_quoted","%eof"]
        bit_start = st Prelude.* 168
        bit_end = (st Prelude.+ 1) Prelude.* 168
        read_bit = readArrayBit happyExpList
        bits = Prelude.map read_bit [bit_start..bit_end Prelude.- 1]
        bits_indexed = Prelude.zip bits [0..167]
        token_strs_expected = Prelude.concatMap f bits_indexed
        f (Prelude.False, _) = []
        f (Prelude.True, nr) = [token_strs Prelude.!! nr]

happyActOffsets :: HappyAddr
happyActOffsets = HappyA# "\xfa\xff\xf8\xff\xe2\xff\x9a\x00\xe2\xff\x8e\x04\x9a\x00\x3a\x00\x99\x00\x26\x00\x98\x00\x98\x00\x01\x00\x99\x00\x99\x00\x8d\x00\xa1\x00\xa1\x00\x5d\x00\xb6\x00\xd3\x00\xd2\x00\xfd\x00\xde\xff\xde\xff\x0b\x00\x03\x01\x99\x00\x57\x01\x57\x01\x22\x01\xf0\x00\x77\x00\x12\x01\x02\x01\x02\x01\x02\x01\xc4\x00\x27\x00\x27\x00\x27\x00\xbf\x00\x7d\x01\x02\x01\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x6a\x01\x00\x00\x00\x00\x00\x00\x07\x02\xe4\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3d\x01\x00\x00\x43\x01\x27\x00\x90\x01\x9d\x01\x00\x00\x5e\x01\x5e\x01\xa9\x01\xaa\x01\xb1\x01\x6e\x01\xe5\x00\x85\x01\x00\x00\x00\x00\x00\x00\x83\x01\x00\x00\x0b\x01\xf7\xff\x00\x00\x14\x00\xcc\x01\x8f\x01\xd9\xff\xdd\x01\x9c\x01\x9c\x01\xe4\x01\xab\x01\xab\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xab\x01\xab\x01\xab\x01\x31\x01\x03\x00\x08\x00\xab\x01\x27\x00\xab\x01\xf4\x01\xf7\x01\x02\x02\xb8\x01\x00\x00\xc1\x01\xba\x01\x03\x02\xbb\x01\xf3\x01\xbe\x01\x27\x00\xbe\x01\x27\x00\xbe\x01\x0b\x02\xcd\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\x00\xcd\x01\x4c\x00\xcd\x01\xd0\x01\xce\x01\x1d\x00\xce\x01\x16\x02\xd7\x01\x00\x00\x00\x00\xd7\x01\x4d\x00\xd7\x01\xfe\x01\x20\x02\xe6\x01\xe6\x01\x7a\x01\xe6\x01\x0d\x02\xe9\x01\x18\x02\x2a\x02\x32\x02\xf5\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x8e\x04\xf5\x01\xf5\x01\x3d\x02\xf8\x01\x3e\x02\xf9\x01\x0f\x02\xfa\x01\x3f\x02\x40\x02\x45\x02\x00\x00\x00\x00\x00\x00\x08\x02\x08\x02\xca\x00\x00\x00\xfc\x01\xfc\x01\xfc\x01\x09\x02\x57\x01\x31\x01\x57\x01\x09\x02\x12\x02\x00\x00\x31\x01\x0c\x02\x00\x00\x2b\x02\x10\x02\x10\x02\x2f\x02\x24\x02\x24\x02\xde\xff\xde\xff\x1b\x01\x31\x02\x31\x02\x31\x02\x27\x02\x57\x01\x57\x01\x4c\x01\x1a\x02\x31\x01\x73\x00\x73\x00\x00\x00\x13\x02\x1b\x02\x5a\x02\x31\x01\x31\x01\x31\x01\x77\x00\x60\x02\x31\x01\x31\x01\xca\x01\x05\x00\x31\x01\x31\x01\x29\x02\x00\x00\x24\x01\x72\x01\x00\x00\x00\x00\x96\x01\x97\x01\x25\x02\x00\x00\xc4\x01\xc5\x01\xda\x01\x25\x02\x00\x00\x00\x00\x61\x02\x00\x00\x72\x01\x00\x00\x00\x00\x00\x00\x70\x02\x71\x02\x7b\x02\x39\x02\x3a\x02\x00\x00\x00\x00\x7e\x02\x00\x00\x00\x00\x3b\x02\x00\x00\x00\x00\x63\x02\x00\x00\xe4\x00\x00\x00\xe4\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x44\x02\x4c\x02\x46\x02\x81\x02\x64\x02\xf6\xff\x00\x00\x00\x00\x00\x00\x00\x00\x5f\x02\x5f\x02\x84\x02\x00\x00\x00\x00\x00\x00\x89\x02\x00\x00\x00\x00\x31\x01\x72\x01\x47\x02\x47\x02\x85\x02\x8e\x02\x62\x02\x00\x00\x31\x01\x90\x02\x00\x00\x92\x02\x54\x02\x58\x02\x95\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9c\x02\x9e\x02\xca\x00\x31\x01\xa1\x02\xa2\x02\x00\x00\x00\x00\x00\x00"#

happyGotoOffsets :: HappyAddr
happyGotoOffsets = HappyA# "\x65\x01\x9b\x02\x1a\x00\xab\x00\x9b\x00\xa3\x02\xa4\x02\xa7\x02\xa7\x00\x80\x00\x49\x00\xa0\x02\xd0\x02\xff\x02\xf4\x00\xa8\x02\x55\x00\xcb\x00\x9d\x02\xa5\x02\xa6\x02\xa9\x02\x9a\x02\xa6\x00\x67\x00\xaa\x02\x9f\x02\x68\x03\x52\x04\x59\x00\x98\x02\xab\x02\xb0\x02\xb5\x02\x0d\x00\x12\x00\x0a\x00\x96\x02\x84\x01\x0e\x02\x4b\x02\x88\x02\x93\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x97\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x94\x01\x00\x00\x00\x00\x00\x00\xbe\x02\xc1\x02\x00\x00\x00\x00\x00\x00\xc2\x02\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb6\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x03\x00\x00\x00\x00\x00\x00\xc2\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd1\x01\x00\x00\xff\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb2\x02\x00\x00\xb2\x02\x00\x00\x8c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xea\x02\x00\x00\xb3\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc0\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xca\x02\x33\x01\x11\x01\x00\x00\xc8\x02\xc9\x02\xcc\x02\xcd\x02\x64\x04\x2c\x03\x6e\x04\x85\x00\xc3\x02\x00\x00\x14\x03\xdd\x02\x00\x00\x00\x00\xd9\x00\xe4\x02\xc4\x02\x00\x00\x00\x00\xf2\x00\x3e\x01\x8e\x01\xcf\x02\xd1\x02\xd2\x02\x00\x00\x7c\x04\x98\x04\x00\x00\x76\x00\x92\x03\x79\x02\x3c\x02\x00\x00\x00\x00\x00\x00\x00\x00\xaa\x03\xbc\x03\xd4\x03\xd9\x02\x00\x00\xe6\x03\xfe\x03\xb7\x02\x00\x00\x10\x04\x28\x04\xe6\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe5\x02\x00\x00\x00\x00\x00\x00\x00\x00\xec\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xde\x00\x00\x00\x00\x00\xce\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x34\x01\x35\x01\xf1\x00\x00\x00\xe0\x02\xa4\x01\x00\x00\x00\x00\x00\x00\x00\x00\xde\x02\xdf\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3a\x04\x00\x00\x03\x03\x04\x03\x00\x00\x00\x00\xef\x02\x00\x00\x3e\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1d\x01\x56\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"#

happyAdjustOffset :: Happy_GHC_Exts.Int# -> Happy_GHC_Exts.Int#
happyAdjustOffset off = off

happyDefActions :: HappyAddr
happyDefActions = HappyA# "\x00\x00\x00\x00\xcb\xff\xc8\xff\x00\x00\x00\x00\x00\x00\x00\x00\xb8\xff\xb5\xff\x00\x00\x00\x00\xae\xff\xae\xff\x00\x00\x00\x00\xa8\xff\x00\x00\x00\x00\x00\x00\x00\x00\x9c\xff\x9a\xff\x98\xff\x00\x00\x00\x00\x90\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x74\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd4\xff\x00\x00\x5b\xff\x59\xff\x5c\xff\x5d\xff\x5a\xff\x58\xff\x70\xff\x85\xff\x86\xff\x84\xff\x00\x00\x8a\xff\x87\xff\x83\xff\x82\xff\x81\xff\x80\xff\x7f\xff\x00\x00\x00\x00\x00\x00\x00\x00\x6c\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb8\xff\x74\xff\xd3\xff\xd2\xff\xd1\xff\x00\x00\x61\xff\x00\x00\x00\x00\x63\xff\x00\x00\x65\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x73\xff\x00\x00\x00\x00\x78\xff\x77\xff\x7a\xff\x76\xff\x79\xff\x7b\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x94\xff\x95\xff\x00\x00\x97\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9f\xff\xa0\xff\xa2\xff\xa1\xff\xa5\xff\x00\x00\x00\x00\xa7\xff\x00\x00\xa8\xff\x00\x00\xab\xff\x00\x00\xad\xff\x00\x00\xaf\xff\xb0\xff\x00\x00\xae\xff\x00\x00\x00\x00\xb4\xff\x00\x00\x00\x00\xb7\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xbf\xff\xc1\xff\xc4\xff\xc0\xff\xbe\xff\xc3\xff\xc2\xff\x00\x00\x00\x00\x00\x00\xc7\xff\x00\x00\xca\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd0\xff\xce\xff\xcf\xff\x00\x00\xcb\xff\xc8\xff\xc5\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb8\xff\x00\x00\xb5\xff\x9c\xff\xb1\xff\xae\xff\x00\x00\xa9\xff\x00\x00\xa8\xff\x00\x00\x00\x00\x9d\xff\x9b\xff\x98\xff\x98\xff\x00\x00\x00\x00\x00\x00\x00\x00\x8f\xff\x00\x00\x00\x00\x00\x00\x74\xff\x00\x00\x00\x00\x00\x00\x62\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x6f\xff\x00\x00\x60\xff\x5e\xff\x7e\xff\x00\x00\x00\x00\x00\x00\x75\xff\x00\x00\x00\x00\x00\x00\x00\x00\x8c\xff\x8b\xff\x66\xff\x64\xff\x71\xff\x72\xff\x88\xff\x89\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x96\xff\x99\xff\x00\x00\xa4\xff\xa6\xff\x00\x00\xaa\xff\xac\xff\x9a\xff\xb3\xff\x8d\xff\xb6\xff\x8e\xff\x00\x00\xbc\xff\xbb\xff\xbd\xff\xc6\xff\xc9\xff\x00\x00\xcb\xff\x00\x00\xb5\xff\x00\x00\x90\xff\x00\x00\x9e\xff\x6d\xff\x6e\xff\x93\xff\x00\x00\x00\x00\x00\x00\x69\xff\x67\xff\x68\xff\x00\x00\x6b\xff\x6a\xff\x00\x00\x5f\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb2\xff\xb8\xff\x00\x00\xcc\xff\x00\x00\x00\x00\x00\x00\x00\x00\xa3\xff\x91\xff\x92\xff\x7c\xff\x7d\xff\xba\xff\x00\x00\x00\x00\xc8\xff\xb8\xff\x00\x00\x00\x00\xcd\xff\xb9\xff"#

happyCheck :: HappyAddr
happyCheck = HappyA# "\xff\xff\x23\x00\x01\x00\x01\x00\x03\x00\x01\x00\x03\x00\x02\x00\x11\x00\x13\x00\x00\x00\x08\x00\x04\x00\x00\x00\x06\x00\x00\x00\x1a\x00\x19\x00\x00\x00\x19\x00\x36\x00\x14\x00\x20\x00\x13\x00\x17\x00\x18\x00\x00\x00\x3d\x00\x1b\x00\x44\x00\x24\x00\x29\x00\x06\x00\x04\x00\x08\x00\x06\x00\x13\x00\x4c\x00\x48\x00\x26\x00\x01\x00\x33\x00\x48\x00\x1a\x00\x2b\x00\x2c\x00\x23\x00\x35\x00\x13\x00\x20\x00\x28\x00\x26\x00\x27\x00\x26\x00\x27\x00\x32\x00\x42\x00\x27\x00\x29\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x3e\x00\x36\x00\x1b\x00\x4c\x00\x33\x00\x44\x00\x32\x00\x46\x00\x3d\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x4c\x00\x03\x00\x05\x00\x2b\x00\x2c\x00\x4c\x00\x00\x00\x2f\x00\x0e\x00\x0f\x00\x00\x00\x01\x00\x02\x00\x03\x00\x4c\x00\x24\x00\x13\x00\x4c\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x1a\x00\x00\x00\x1b\x00\x14\x00\x15\x00\x44\x00\x20\x00\x46\x00\x48\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x29\x00\x00\x00\x1a\x00\x2b\x00\x2c\x00\x21\x00\x22\x00\x23\x00\x20\x00\x25\x00\x33\x00\x00\x00\x28\x00\x29\x00\x1c\x00\x1d\x00\x00\x00\x29\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x00\x00\x0d\x00\x1b\x00\x28\x00\x33\x00\x44\x00\x0d\x00\x46\x00\x1d\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x00\x00\x26\x00\x27\x00\x2b\x00\x2c\x00\x14\x00\x15\x00\x2f\x00\x08\x00\x2d\x00\x2e\x00\x00\x00\x00\x00\x01\x00\x02\x00\x03\x00\x34\x00\x1f\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x07\x00\x0c\x00\x1b\x00\x0a\x00\x3f\x00\x44\x00\x1e\x00\x46\x00\x43\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x1b\x00\x1c\x00\x1d\x00\x2b\x00\x2c\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x00\x00\x25\x00\x35\x00\x28\x00\x28\x00\x29\x00\x37\x00\x38\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x14\x00\x00\x00\x1b\x00\x17\x00\x18\x00\x44\x00\x00\x00\x46\x00\x15\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x03\x00\x1e\x00\x48\x00\x2b\x00\x2c\x00\x08\x00\x14\x00\x15\x00\x2b\x00\x2c\x00\x00\x00\x00\x00\x15\x00\x00\x00\x01\x00\x02\x00\x03\x00\x42\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x0d\x00\x3b\x00\x1b\x00\x37\x00\x38\x00\x44\x00\x31\x00\x46\x00\x12\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x1b\x00\x1c\x00\x1d\x00\x2b\x00\x2c\x00\x40\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x07\x00\x25\x00\x28\x00\x0a\x00\x28\x00\x29\x00\x21\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x07\x00\x22\x00\x1b\x00\x0a\x00\x04\x00\x44\x00\x06\x00\x46\x00\x3c\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x00\x00\x00\x00\x11\x00\x2b\x00\x2c\x00\x26\x00\x06\x00\x06\x00\x08\x00\x08\x00\x1b\x00\x00\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0e\x00\x0f\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x48\x00\x0c\x00\x1b\x00\x4c\x00\x02\x00\x44\x00\x04\x00\x46\x00\x06\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x01\x00\x1b\x00\x1c\x00\x1d\x00\x2b\x00\x2c\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x48\x00\x25\x00\x4a\x00\x28\x00\x28\x00\x29\x00\x04\x00\x05\x00\x14\x00\x3b\x00\x3c\x00\x17\x00\x18\x00\x0b\x00\x07\x00\x1b\x00\x0e\x00\x0f\x00\x44\x00\x04\x00\x46\x00\x06\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x26\x00\x04\x00\x05\x00\x06\x00\x44\x00\x2b\x00\x2c\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0b\x00\x0c\x00\x0d\x00\x0e\x00\x0f\x00\x10\x00\x00\x00\x4c\x00\x02\x00\x01\x00\x3b\x00\x3c\x00\x00\x00\x01\x00\x02\x00\x03\x00\x02\x00\x02\x00\x04\x00\x04\x00\x06\x00\x06\x00\x01\x00\x48\x00\x49\x00\x4a\x00\x4b\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x4a\x00\x25\x00\x01\x00\x01\x00\x28\x00\x29\x00\x2a\x00\x2b\x00\x2c\x00\x2d\x00\x01\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x4a\x00\x25\x00\x16\x00\x17\x00\x28\x00\x29\x00\x2a\x00\x2b\x00\x2c\x00\x2d\x00\x00\x00\x01\x00\x02\x00\x03\x00\x02\x00\x02\x00\x04\x00\x04\x00\x06\x00\x06\x00\x02\x00\x48\x00\x04\x00\x4c\x00\x06\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0b\x00\x0c\x00\x0d\x00\x0e\x00\x0f\x00\x10\x00\x4c\x00\x02\x00\x11\x00\x04\x00\x15\x00\x06\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x09\x00\x25\x00\x4c\x00\x05\x00\x28\x00\x29\x00\x2a\x00\x2b\x00\x2c\x00\x2d\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x01\x00\x25\x00\x4c\x00\x01\x00\x28\x00\x29\x00\x2a\x00\x2b\x00\x2c\x00\x2d\x00\x00\x00\x01\x00\x02\x00\x03\x00\x01\x00\x4c\x00\x44\x00\x4c\x00\x4c\x00\x05\x00\x16\x00\x4c\x00\x04\x00\x01\x00\x06\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0b\x00\x0c\x00\x0d\x00\x0e\x00\x0f\x00\x10\x00\x48\x00\x4c\x00\x4c\x00\x05\x00\x15\x00\x1f\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x4c\x00\x25\x00\x05\x00\x0e\x00\x28\x00\x29\x00\x2a\x00\x2b\x00\x2c\x00\x2d\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x4c\x00\x25\x00\x27\x00\x4c\x00\x28\x00\x29\x00\x0e\x00\x2b\x00\x2c\x00\x2d\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0e\x00\x4c\x00\x05\x00\x05\x00\x4c\x00\x4c\x00\x4c\x00\x4b\x00\x39\x00\x0a\x00\x0a\x00\x00\x00\x01\x00\x02\x00\x03\x00\x0a\x00\x48\x00\x48\x00\x40\x00\x28\x00\x48\x00\x26\x00\x32\x00\x26\x00\x48\x00\x32\x00\x47\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x45\x00\x25\x00\x48\x00\x09\x00\x28\x00\x29\x00\x06\x00\x2b\x00\x2c\x00\x2d\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x4a\x00\x25\x00\x48\x00\x11\x00\x28\x00\x29\x00\x05\x00\x05\x00\x2c\x00\x2d\x00\x00\x00\x01\x00\x02\x00\x03\x00\x02\x00\x45\x00\x45\x00\x02\x00\x35\x00\x01\x00\x48\x00\x21\x00\x26\x00\x22\x00\x02\x00\x00\x00\x01\x00\x02\x00\x03\x00\x48\x00\x09\x00\x48\x00\x06\x00\x02\x00\x4a\x00\x02\x00\x31\x00\x02\x00\x41\x00\x3e\x00\x02\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x01\x00\x25\x00\x01\x00\x05\x00\x28\x00\x29\x00\x02\x00\x02\x00\x2c\x00\x2d\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x09\x00\x25\x00\x0a\x00\x0f\x00\x28\x00\x29\x00\x0b\x00\x16\x00\x1a\x00\x2d\x00\x00\x00\x01\x00\x02\x00\x03\x00\x22\x00\x13\x00\x17\x00\x1e\x00\x18\x00\x29\x00\x02\x00\x2e\x00\x19\x00\x02\x00\x02\x00\x2e\x00\x13\x00\x1d\x00\x16\x00\x09\x00\x00\x00\x03\x00\x03\x00\x00\x00\x23\x00\x03\x00\x00\x00\x01\x00\x02\x00\x03\x00\x24\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x25\x00\x25\x00\x19\x00\x00\x00\x28\x00\x29\x00\x10\x00\x11\x00\x12\x00\x2d\x00\x00\x00\x2e\x00\x00\x00\x02\x00\x1a\x00\x25\x00\x00\x00\x01\x00\x02\x00\x03\x00\x02\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x25\x00\x25\x00\x25\x00\x25\x00\x28\x00\x29\x00\x10\x00\x11\x00\x12\x00\x24\x00\x1e\x00\x00\x00\x01\x00\x02\x00\x03\x00\x25\x00\x25\x00\x02\x00\x02\x00\x18\x00\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\x11\x00\x12\x00\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\x11\x00\x12\x00\x28\x00\x29\x00\xff\xff\xff\xff\xff\xff\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x0c\x00\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x0c\x00\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\x0c\x00\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\x1f\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x01\x00\x02\x00\x03\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\xff\xff\xff\xff\xff\xff\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x20\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\x00\x00\x01\x00\x02\x00\x03\x00\xff\xff\x21\x00\x22\x00\x23\x00\x12\x00\x25\x00\xff\xff\xff\xff\x28\x00\x29\x00\xff\xff\xff\xff\xff\xff\xff\xff\x1c\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x25\x00\x26\x00\xff\xff\xff\xff\xff\xff\x2a\x00\x21\x00\x22\x00\x23\x00\xff\xff\x25\x00\x30\x00\xff\xff\x28\x00\x29\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x3a\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"#

happyTable :: HappyAddr
happyTable = HappyA# "\x00\x00\x6f\x00\x69\x00\x1b\x01\x90\x00\x19\x01\xd4\x00\xee\x00\xd9\x00\xc9\x00\x34\x00\xd5\x00\xbe\x00\x59\x00\xc0\x00\x59\x00\x80\x00\xae\x00\x59\x00\xae\x00\x70\x00\x43\x00\x81\x00\x1a\x01\x44\x00\x45\x00\xa6\x00\x71\x00\x46\x00\xcf\x00\x9a\x00\x82\x00\xaa\x00\xbe\x00\xab\x00\xc0\x00\xc9\x00\xff\xff\x2d\x00\x47\x00\x42\x00\x83\x00\x2d\x00\x80\x00\x48\x00\x49\x00\x6f\x00\x92\x00\xc5\x00\x81\x00\x58\x00\x5b\x00\x5c\x00\xdb\x00\x5c\x00\xda\x00\x7e\x00\x5a\x00\x82\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x1c\x01\x70\x00\x46\x00\xff\xff\x83\x00\x4c\x00\xda\x00\x4d\x00\x71\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x69\x00\xff\xff\x90\x00\xc8\x00\x48\x00\x49\x00\xff\xff\x83\x00\x53\x00\x92\x00\x93\x00\x34\x00\x35\x00\x36\x00\x37\x00\xff\xff\x9a\x00\xc9\x00\xff\xff\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x80\x00\x34\x00\x46\x00\x85\x00\x86\x00\x4c\x00\x81\x00\x4d\x00\x2d\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x42\x00\x82\x00\x59\x00\x80\x00\x48\x00\x49\x00\x67\x00\x3b\x00\x3c\x00\x81\x00\x3d\x00\x83\x00\x94\x00\x3e\x00\x3f\x00\x71\x00\x72\x00\x94\x00\x82\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x83\x00\x95\x00\x46\x00\x73\x00\x83\x00\x4c\x00\x0d\x01\x4d\x00\x60\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x69\x00\xa6\x00\xfc\x00\x5c\x00\x48\x00\x49\x00\xc5\x00\x86\x00\x53\x00\xa7\x00\x61\x00\x62\x00\x34\x00\x34\x00\x35\x00\x36\x00\x37\x00\x63\x00\x89\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\xa8\x00\x96\x00\x46\x00\xa9\x00\x64\x00\x4c\x00\x9c\x00\x4d\x00\x65\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x42\x00\x74\x00\x75\x00\x72\x00\x48\x00\x49\x00\x97\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x83\x00\x3d\x00\x92\x00\x73\x00\x3e\x00\x3f\x00\x9d\x00\x9e\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x43\x00\x83\x00\x46\x00\x44\x00\x45\x00\x4c\x00\x83\x00\x4d\x00\x84\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x69\x00\xd4\x00\x9c\x00\x2d\x00\x48\x00\x49\x00\xd5\x00\x08\x01\x86\x00\x48\x00\x49\x00\x94\x00\x34\x00\x1d\x01\x34\x00\x35\x00\x36\x00\x37\x00\x7e\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x34\x01\x4a\x00\x46\x00\x9d\x00\x9e\x00\x4c\x00\x7c\x00\x4d\x00\x89\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x42\x00\x05\x01\x75\x00\x72\x00\x48\x00\x49\x00\x7a\x00\x8a\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x15\x01\x3d\x00\x73\x00\xa9\x00\x3e\x00\x3f\x00\x78\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x45\x01\x6d\x00\x46\x00\xa9\x00\xbe\x00\x4c\x00\xc0\x00\x4d\x00\x4b\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x69\x00\xa6\x00\xa6\x00\x2c\x01\x48\x00\x49\x00\x47\x00\x16\x01\x36\x01\xab\x00\xab\x00\x46\x00\x34\x00\x34\x00\x35\x00\x36\x00\x37\x00\x35\x01\x93\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\x2d\x00\xdc\x00\x46\x00\xff\xff\xef\x00\x4c\x00\xbe\x00\x4d\x00\xc0\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\x69\x00\x04\x01\x75\x00\x72\x00\x48\x00\x49\x00\x97\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x2d\x00\x3d\x00\x4f\x00\x73\x00\x3e\x00\x3f\x00\xae\x00\xaf\x00\x43\x00\x4a\x00\x4b\x00\x44\x00\x45\x00\xb0\x00\xea\x00\x46\x00\xb1\x00\x93\x00\x4c\x00\xbe\x00\x4d\x00\xc0\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x47\x00\xbe\x00\xbf\x00\xc0\x00\xcf\x00\x48\x00\x49\x00\x34\x00\x35\x00\x36\x00\x37\x00\x2f\x00\x30\x00\x31\x00\x32\x00\x33\x00\x34\x00\x02\x01\xff\xff\x03\x01\xe5\x00\x4a\x00\x4b\x00\x34\x00\x35\x00\x36\x00\x37\x00\x2b\x01\x2a\x01\xbe\x00\xbe\x00\xc0\x00\xc0\x00\xe4\x00\x2d\x00\x4e\x00\x4f\x00\x50\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x4f\x00\x3d\x00\xe1\x00\xe0\x00\x3e\x00\x3f\x00\x55\x00\x56\x00\x54\x00\x51\x00\xdf\x00\xe5\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x4f\x00\x3d\x00\xc6\x00\x31\x01\x3e\x00\x3f\x00\xe6\x00\x56\x00\x54\x00\x51\x00\x34\x00\x35\x00\x36\x00\x37\x00\x28\x01\x27\x01\xbe\x00\xbe\x00\xc0\x00\xc0\x00\xef\x00\x2d\x00\xbe\x00\xff\xff\xc0\x00\x34\x00\x35\x00\x36\x00\x37\x00\x2f\x00\x30\x00\x31\x00\x32\x00\x33\x00\x34\x00\xff\xff\x26\x01\xd9\x00\xbe\x00\xe9\x00\xc0\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\xd8\x00\x3d\x00\xff\xff\xd7\x00\x3e\x00\x3f\x00\xd2\x00\x56\x00\x54\x00\x51\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\xd2\x00\x3d\x00\xff\xff\xd1\x00\x3e\x00\x3f\x00\xcb\x00\x56\x00\x54\x00\x51\x00\x34\x00\x35\x00\x36\x00\x37\x00\xd0\x00\xff\xff\xcf\x00\xff\xff\xff\xff\xce\x00\xcd\x00\xff\xff\xbe\x00\xca\x00\xc0\x00\x34\x00\x35\x00\x36\x00\x37\x00\x2f\x00\x30\x00\x31\x00\x32\x00\x33\x00\x34\x00\x2d\x00\xff\xff\xff\xff\xc4\x00\xe9\x00\x89\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\xff\xff\x3d\x00\xc1\x00\xbc\x00\x3e\x00\x3f\x00\xca\x00\x56\x00\x54\x00\x51\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\xff\xff\x3d\x00\xbd\x00\xff\xff\x3e\x00\x3f\x00\xbb\x00\x53\x00\x54\x00\x51\x00\x34\x00\x35\x00\x36\x00\x37\x00\xba\x00\xff\xff\xb8\x00\xb7\x00\xff\xff\xff\xff\xff\xff\x50\x00\xb6\x00\xb5\x00\xb4\x00\x34\x00\x35\x00\x36\x00\x37\x00\xb3\x00\x2d\x00\x2d\x00\x7a\x00\x0a\x01\x2d\x00\x47\x00\xda\x00\x47\x00\x2d\x00\xda\x00\xf9\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\xf8\x00\x3d\x00\x2d\x00\xf7\x00\x3e\x00\x3f\x00\xf2\x00\xf9\x00\x54\x00\x51\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x4f\x00\x3d\x00\x2d\x00\xd9\x00\x3e\x00\x3f\x00\x24\x01\x23\x01\x50\x00\x51\x00\x34\x00\x35\x00\x36\x00\x37\x00\x22\x01\x21\x01\x20\x01\x1f\x01\x92\x00\x34\x01\x2d\x00\x78\x00\x47\x00\x6d\x00\x3d\x01\x34\x00\x35\x00\x36\x00\x37\x00\x2d\x00\x2f\x01\x2d\x00\x2e\x01\x3c\x01\x4f\x00\x39\x01\x7c\x00\x38\x01\x42\x01\x41\x01\x40\x01\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x44\x01\x3d\x00\x43\x01\xac\x00\x3e\x00\x3f\x00\x48\x01\x47\x01\xfa\x00\x51\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x9e\x00\x3d\x00\x9a\x00\x90\x00\x3e\x00\x3f\x00\x98\x00\x7e\x00\x76\x00\x40\x00\x34\x00\x35\x00\x36\x00\x37\x00\x66\x00\x87\x00\x7c\x00\x6b\x00\x7a\x00\x57\x00\xe2\x00\x2d\x00\x78\x00\xe1\x00\xdd\x00\xe7\x00\xc1\x00\x6d\x00\xc6\x00\xb8\x00\x17\x01\x14\x01\x13\x01\x11\x01\x65\x00\x12\x01\x34\x00\x35\x00\x36\x00\x37\x00\x5e\x00\x38\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x5d\x00\x3d\x00\x0c\x01\x0a\x01\x3e\x00\x3f\x00\x8d\x00\x8e\x00\x8c\x00\xda\x00\x07\x01\xe7\x00\xea\x00\x28\x01\x1c\x01\x06\x01\x34\x00\x35\x00\x36\x00\x37\x00\x24\x01\x8a\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x01\x01\x3d\x00\x00\x01\xff\x00\x3e\x00\x3f\x00\xc2\x00\x8e\x00\x8c\x00\xf2\x00\x32\x01\x34\x00\x35\x00\x36\x00\x37\x00\x30\x01\x2f\x01\x3e\x01\x3d\x01\x3a\x01\x00\x00\x8a\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x8b\x00\x8c\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x8a\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x0b\x01\x8c\x00\x3e\x00\x3f\x00\x00\x00\x00\x00\x00\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x8a\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x0f\x01\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x39\x01\x97\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x97\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x44\x01\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x97\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x6a\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd5\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\xfb\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf5\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\xf4\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf3\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\xf0\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xef\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\xec\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xeb\x00\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x2c\x01\x39\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x34\x00\x35\x00\x36\x00\x37\x00\x69\x00\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x01\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x0e\x01\x3a\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x34\x00\x35\x00\x36\x00\x37\x00\x00\x00\xfe\x00\x3b\x00\x3c\x00\xa0\x00\x3d\x00\x00\x00\x00\x00\x3e\x00\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa2\x00\xa3\x00\x00\x00\x00\x00\x00\x00\xa4\x00\xfd\x00\x3b\x00\x3c\x00\x00\x00\x3d\x00\xa5\x00\x00\x00\x3e\x00\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xa6\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"#

happyReduceArr = Happy_Data_Array.array (43, 167) [
	(43 , happyReduce_43),
	(44 , happyReduce_44),
	(45 , happyReduce_45),
	(46 , happyReduce_46),
	(47 , happyReduce_47),
	(48 , happyReduce_48),
	(49 , happyReduce_49),
	(50 , happyReduce_50),
	(51 , happyReduce_51),
	(52 , happyReduce_52),
	(53 , happyReduce_53),
	(54 , happyReduce_54),
	(55 , happyReduce_55),
	(56 , happyReduce_56),
	(57 , happyReduce_57),
	(58 , happyReduce_58),
	(59 , happyReduce_59),
	(60 , happyReduce_60),
	(61 , happyReduce_61),
	(62 , happyReduce_62),
	(63 , happyReduce_63),
	(64 , happyReduce_64),
	(65 , happyReduce_65),
	(66 , happyReduce_66),
	(67 , happyReduce_67),
	(68 , happyReduce_68),
	(69 , happyReduce_69),
	(70 , happyReduce_70),
	(71 , happyReduce_71),
	(72 , happyReduce_72),
	(73 , happyReduce_73),
	(74 , happyReduce_74),
	(75 , happyReduce_75),
	(76 , happyReduce_76),
	(77 , happyReduce_77),
	(78 , happyReduce_78),
	(79 , happyReduce_79),
	(80 , happyReduce_80),
	(81 , happyReduce_81),
	(82 , happyReduce_82),
	(83 , happyReduce_83),
	(84 , happyReduce_84),
	(85 , happyReduce_85),
	(86 , happyReduce_86),
	(87 , happyReduce_87),
	(88 , happyReduce_88),
	(89 , happyReduce_89),
	(90 , happyReduce_90),
	(91 , happyReduce_91),
	(92 , happyReduce_92),
	(93 , happyReduce_93),
	(94 , happyReduce_94),
	(95 , happyReduce_95),
	(96 , happyReduce_96),
	(97 , happyReduce_97),
	(98 , happyReduce_98),
	(99 , happyReduce_99),
	(100 , happyReduce_100),
	(101 , happyReduce_101),
	(102 , happyReduce_102),
	(103 , happyReduce_103),
	(104 , happyReduce_104),
	(105 , happyReduce_105),
	(106 , happyReduce_106),
	(107 , happyReduce_107),
	(108 , happyReduce_108),
	(109 , happyReduce_109),
	(110 , happyReduce_110),
	(111 , happyReduce_111),
	(112 , happyReduce_112),
	(113 , happyReduce_113),
	(114 , happyReduce_114),
	(115 , happyReduce_115),
	(116 , happyReduce_116),
	(117 , happyReduce_117),
	(118 , happyReduce_118),
	(119 , happyReduce_119),
	(120 , happyReduce_120),
	(121 , happyReduce_121),
	(122 , happyReduce_122),
	(123 , happyReduce_123),
	(124 , happyReduce_124),
	(125 , happyReduce_125),
	(126 , happyReduce_126),
	(127 , happyReduce_127),
	(128 , happyReduce_128),
	(129 , happyReduce_129),
	(130 , happyReduce_130),
	(131 , happyReduce_131),
	(132 , happyReduce_132),
	(133 , happyReduce_133),
	(134 , happyReduce_134),
	(135 , happyReduce_135),
	(136 , happyReduce_136),
	(137 , happyReduce_137),
	(138 , happyReduce_138),
	(139 , happyReduce_139),
	(140 , happyReduce_140),
	(141 , happyReduce_141),
	(142 , happyReduce_142),
	(143 , happyReduce_143),
	(144 , happyReduce_144),
	(145 , happyReduce_145),
	(146 , happyReduce_146),
	(147 , happyReduce_147),
	(148 , happyReduce_148),
	(149 , happyReduce_149),
	(150 , happyReduce_150),
	(151 , happyReduce_151),
	(152 , happyReduce_152),
	(153 , happyReduce_153),
	(154 , happyReduce_154),
	(155 , happyReduce_155),
	(156 , happyReduce_156),
	(157 , happyReduce_157),
	(158 , happyReduce_158),
	(159 , happyReduce_159),
	(160 , happyReduce_160),
	(161 , happyReduce_161),
	(162 , happyReduce_162),
	(163 , happyReduce_163),
	(164 , happyReduce_164),
	(165 , happyReduce_165),
	(166 , happyReduce_166),
	(167 , happyReduce_167)
	]

happy_n_terms = 77 :: Prelude.Int
happy_n_nonterms = 47 :: Prelude.Int

happyReduce_43 = happySpecReduce_1  0# happyReduction_43
happyReduction_43 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn46
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.Ident (Data.Text.pack (prToken happy_var_1)))
	)}

happyReduce_44 = happySpecReduce_1  1# happyReduction_44
happyReduction_44 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn47
		 ((Just (tokenLineCol happy_var_1), read (prToken happy_var_1))
	)}

happyReduce_45 = happySpecReduce_1  2# happyReduction_45
happyReduction_45 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn48
		 ((Just (tokenLineCol happy_var_1), read (prToken happy_var_1))
	)}

happyReduce_46 = happySpecReduce_1  3# happyReduction_46
happyReduction_46 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn49
		 ((Just (tokenLineCol happy_var_1), prToken happy_var_1)
	)}

happyReduce_47 = happySpecReduce_2  4# happyReduction_47
happyReduction_47 happy_x_2
	happy_x_1
	 =  case happyOut60 happy_x_1 of { (HappyWrap60 happy_var_1) -> 
	happyIn50
		 ((fst happy_var_1, Language.SQL.Abs.QSelect (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_48 = happySpecReduce_2  4# happyReduction_48
happyReduction_48 happy_x_2
	happy_x_1
	 =  case happyOut51 happy_x_1 of { (HappyWrap51 happy_var_1) -> 
	happyIn50
		 ((fst happy_var_1, Language.SQL.Abs.QCreate (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_49 = happySpecReduce_2  4# happyReduction_49
happyReduction_49 happy_x_2
	happy_x_1
	 =  case happyOut57 happy_x_1 of { (HappyWrap57 happy_var_1) -> 
	happyIn50
		 ((fst happy_var_1, Language.SQL.Abs.QInsert (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_50 = happyReduce 10# 5# happyReduction_50
happyReduction_50 (happy_x_10 `HappyStk`
	happy_x_9 `HappyStk`
	happy_x_8 `HappyStk`
	happy_x_7 `HappyStk`
	happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	case happyOut52 happy_x_5 of { (HappyWrap52 happy_var_5) -> 
	case happyOut53 happy_x_9 of { (HappyWrap53 happy_var_9) -> 
	happyIn51
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DCreate (Just (tokenLineCol happy_var_1)) (snd happy_var_3) (snd happy_var_5) (snd happy_var_9))
	) `HappyStk` happyRest}}}}

happyReduce_51 = happyReduce 5# 5# happyReduction_51
happyReduction_51 (happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	case happyOut60 happy_x_5 of { (HappyWrap60 happy_var_5) -> 
	happyIn51
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CreateAs (Just (tokenLineCol happy_var_1)) (snd happy_var_3) (snd happy_var_5))
	) `HappyStk` happyRest}}}

happyReduce_52 = happySpecReduce_0  6# happyReduction_52
happyReduction_52  =  happyIn52
		 ((Nothing, [])
	)

happyReduce_53 = happySpecReduce_1  6# happyReduction_53
happyReduction_53 happy_x_1
	 =  case happyOut54 happy_x_1 of { (HappyWrap54 happy_var_1) -> 
	happyIn52
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_54 = happySpecReduce_3  6# happyReduction_54
happyReduction_54 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut54 happy_x_1 of { (HappyWrap54 happy_var_1) -> 
	case happyOut52 happy_x_3 of { (HappyWrap52 happy_var_3) -> 
	happyIn52
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_55 = happySpecReduce_0  7# happyReduction_55
happyReduction_55  =  happyIn53
		 ((Nothing, [])
	)

happyReduce_56 = happySpecReduce_1  7# happyReduction_56
happyReduction_56 happy_x_1
	 =  case happyOut56 happy_x_1 of { (HappyWrap56 happy_var_1) -> 
	happyIn53
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_57 = happySpecReduce_3  7# happyReduction_57
happyReduction_57 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut56 happy_x_1 of { (HappyWrap56 happy_var_1) -> 
	case happyOut53 happy_x_3 of { (HappyWrap53 happy_var_3) -> 
	happyIn53
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_58 = happySpecReduce_2  8# happyReduction_58
happyReduction_58 happy_x_2
	happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	case happyOut55 happy_x_2 of { (HappyWrap55 happy_var_2) -> 
	happyIn54
		 ((fst happy_var_1, Language.SQL.Abs.DSchemaElem (fst happy_var_1) (snd happy_var_1) (snd happy_var_2))
	)}}

happyReduce_59 = happySpecReduce_1  9# happyReduction_59
happyReduction_59 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeInt (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_60 = happySpecReduce_1  9# happyReduction_60
happyReduction_60 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeNum (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_61 = happySpecReduce_1  9# happyReduction_61
happyReduction_61 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeString (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_62 = happySpecReduce_1  9# happyReduction_62
happyReduction_62 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeDateTime (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_63 = happySpecReduce_1  9# happyReduction_63
happyReduction_63 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeInterval (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_64 = happySpecReduce_1  9# happyReduction_64
happyReduction_64 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeArr (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_65 = happySpecReduce_1  9# happyReduction_65
happyReduction_65 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn55
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TypeMap (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_66 = happySpecReduce_3  10# happyReduction_66
happyReduction_66 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut49 happy_x_3 of { (HappyWrap49 happy_var_3) -> 
	happyIn56
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.OptionSource (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	)}}

happyReduce_67 = happySpecReduce_3  10# happyReduction_67
happyReduction_67 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut49 happy_x_3 of { (HappyWrap49 happy_var_3) -> 
	happyIn56
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.OptionFormat (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	)}}

happyReduce_68 = happySpecReduce_3  10# happyReduction_68
happyReduction_68 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut49 happy_x_3 of { (HappyWrap49 happy_var_3) -> 
	happyIn56
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.OptionSink (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	)}}

happyReduce_69 = happyReduce 7# 11# happyReduction_69
happyReduction_69 (happy_x_7 `HappyStk`
	happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	case happyOut58 happy_x_6 of { (HappyWrap58 happy_var_6) -> 
	happyIn57
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DInsert (Just (tokenLineCol happy_var_1)) (snd happy_var_3) (snd happy_var_6))
	) `HappyStk` happyRest}}}

happyReduce_70 = happyReduce 10# 11# happyReduction_70
happyReduction_70 (happy_x_10 `HappyStk`
	happy_x_9 `HappyStk`
	happy_x_8 `HappyStk`
	happy_x_7 `HappyStk`
	happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	case happyOut59 happy_x_5 of { (HappyWrap59 happy_var_5) -> 
	case happyOut58 happy_x_9 of { (HappyWrap58 happy_var_9) -> 
	happyIn57
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.IndertWithSchema (Just (tokenLineCol happy_var_1)) (snd happy_var_3) (snd happy_var_5) (snd happy_var_9))
	) `HappyStk` happyRest}}}}

happyReduce_71 = happySpecReduce_0  12# happyReduction_71
happyReduction_71  =  happyIn58
		 ((Nothing, [])
	)

happyReduce_72 = happySpecReduce_1  12# happyReduction_72
happyReduction_72 happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	happyIn58
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_73 = happySpecReduce_3  12# happyReduction_73
happyReduction_73 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	case happyOut58 happy_x_3 of { (HappyWrap58 happy_var_3) -> 
	happyIn58
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_74 = happySpecReduce_0  13# happyReduction_74
happyReduction_74  =  happyIn59
		 ((Nothing, [])
	)

happyReduce_75 = happySpecReduce_1  13# happyReduction_75
happyReduction_75 happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	happyIn59
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_76 = happySpecReduce_3  13# happyReduction_76
happyReduction_76 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	case happyOut59 happy_x_3 of { (HappyWrap59 happy_var_3) -> 
	happyIn59
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_77 = happyReduce 5# 14# happyReduction_77
happyReduction_77 (happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOut61 happy_x_1 of { (HappyWrap61 happy_var_1) -> 
	case happyOut65 happy_x_2 of { (HappyWrap65 happy_var_2) -> 
	case happyOut71 happy_x_3 of { (HappyWrap71 happy_var_3) -> 
	case happyOut72 happy_x_4 of { (HappyWrap72 happy_var_4) -> 
	case happyOut76 happy_x_5 of { (HappyWrap76 happy_var_5) -> 
	happyIn60
		 ((fst happy_var_1, Language.SQL.Abs.DSelect (fst happy_var_1) (snd happy_var_1) (snd happy_var_2) (snd happy_var_3) (snd happy_var_4) (snd happy_var_5))
	) `HappyStk` happyRest}}}}}

happyReduce_78 = happySpecReduce_2  15# happyReduction_78
happyReduction_78 happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut62 happy_x_2 of { (HappyWrap62 happy_var_2) -> 
	happyIn61
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DSel (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_79 = happySpecReduce_1  16# happyReduction_79
happyReduction_79 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn62
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SelListAsterisk (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_80 = happySpecReduce_1  16# happyReduction_80
happyReduction_80 happy_x_1
	 =  case happyOut63 happy_x_1 of { (HappyWrap63 happy_var_1) -> 
	happyIn62
		 ((fst happy_var_1, Language.SQL.Abs.SelListSublist (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_81 = happySpecReduce_0  17# happyReduction_81
happyReduction_81  =  happyIn63
		 ((Nothing, [])
	)

happyReduce_82 = happySpecReduce_1  17# happyReduction_82
happyReduction_82 happy_x_1
	 =  case happyOut64 happy_x_1 of { (HappyWrap64 happy_var_1) -> 
	happyIn63
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_83 = happySpecReduce_3  17# happyReduction_83
happyReduction_83 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut64 happy_x_1 of { (HappyWrap64 happy_var_1) -> 
	case happyOut63 happy_x_3 of { (HappyWrap63 happy_var_3) -> 
	happyIn63
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_84 = happySpecReduce_1  18# happyReduction_84
happyReduction_84 happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	happyIn64
		 ((fst happy_var_1, Language.SQL.Abs.DerivedColSimpl (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_85 = happySpecReduce_3  18# happyReduction_85
happyReduction_85 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	happyIn64
		 ((fst happy_var_1, Language.SQL.Abs.DerivedColAs (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_86 = happySpecReduce_2  19# happyReduction_86
happyReduction_86 happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut66 happy_x_2 of { (HappyWrap66 happy_var_2) -> 
	happyIn65
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DFrom (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_87 = happySpecReduce_0  20# happyReduction_87
happyReduction_87  =  happyIn66
		 ((Nothing, [])
	)

happyReduce_88 = happySpecReduce_1  20# happyReduction_88
happyReduction_88 happy_x_1
	 =  case happyOut67 happy_x_1 of { (HappyWrap67 happy_var_1) -> 
	happyIn66
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_89 = happySpecReduce_3  20# happyReduction_89
happyReduction_89 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut67 happy_x_1 of { (HappyWrap67 happy_var_1) -> 
	case happyOut66 happy_x_3 of { (HappyWrap66 happy_var_3) -> 
	happyIn66
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_90 = happySpecReduce_1  21# happyReduction_90
happyReduction_90 happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	happyIn67
		 ((fst happy_var_1, Language.SQL.Abs.TableRefSimple (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_91 = happySpecReduce_3  21# happyReduction_91
happyReduction_91 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut67 happy_x_1 of { (HappyWrap67 happy_var_1) -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	happyIn67
		 ((fst happy_var_1, Language.SQL.Abs.TableRefAs (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_92 = happyReduce 6# 21# happyReduction_92
happyReduction_92 (happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOut67 happy_x_1 of { (HappyWrap67 happy_var_1) -> 
	case happyOut68 happy_x_2 of { (HappyWrap68 happy_var_2) -> 
	case happyOut67 happy_x_4 of { (HappyWrap67 happy_var_4) -> 
	case happyOut69 happy_x_5 of { (HappyWrap69 happy_var_5) -> 
	case happyOut70 happy_x_6 of { (HappyWrap70 happy_var_6) -> 
	happyIn67
		 ((fst happy_var_1, Language.SQL.Abs.TableRefJoin (fst happy_var_1) (snd happy_var_1) (snd happy_var_2) (snd happy_var_4) (snd happy_var_5) (snd happy_var_6))
	) `HappyStk` happyRest}}}}}

happyReduce_93 = happySpecReduce_1  22# happyReduction_93
happyReduction_93 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn68
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.JoinLeft (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_94 = happySpecReduce_1  22# happyReduction_94
happyReduction_94 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn68
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.JoinRight (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_95 = happySpecReduce_1  22# happyReduction_95
happyReduction_95 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn68
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.JoinFull (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_96 = happySpecReduce_1  22# happyReduction_96
happyReduction_96 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn68
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.JoinCross (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_97 = happyReduce 4# 23# happyReduction_97
happyReduction_97 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut83 happy_x_3 of { (HappyWrap83 happy_var_3) -> 
	happyIn69
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DJoinWindow (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_98 = happySpecReduce_2  24# happyReduction_98
happyReduction_98 happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut88 happy_x_2 of { (HappyWrap88 happy_var_2) -> 
	happyIn70
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DJoinCond (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_99 = happySpecReduce_0  25# happyReduction_99
happyReduction_99  =  happyIn71
		 ((Nothing, Language.SQL.Abs.DWhereEmpty Nothing)
	)

happyReduce_100 = happySpecReduce_2  25# happyReduction_100
happyReduction_100 happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut88 happy_x_2 of { (HappyWrap88 happy_var_2) -> 
	happyIn71
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DWhere (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_101 = happySpecReduce_0  26# happyReduction_101
happyReduction_101  =  happyIn72
		 ((Nothing, Language.SQL.Abs.DGroupByEmpty Nothing)
	)

happyReduce_102 = happySpecReduce_3  26# happyReduction_102
happyReduction_102 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut73 happy_x_3 of { (HappyWrap73 happy_var_3) -> 
	happyIn72
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DGroupBy (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	)}}

happyReduce_103 = happySpecReduce_0  27# happyReduction_103
happyReduction_103  =  happyIn73
		 ((Nothing, [])
	)

happyReduce_104 = happySpecReduce_1  27# happyReduction_104
happyReduction_104 happy_x_1
	 =  case happyOut74 happy_x_1 of { (HappyWrap74 happy_var_1) -> 
	happyIn73
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_105 = happySpecReduce_3  27# happyReduction_105
happyReduction_105 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut74 happy_x_1 of { (HappyWrap74 happy_var_1) -> 
	case happyOut73 happy_x_3 of { (HappyWrap73 happy_var_3) -> 
	happyIn73
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_106 = happySpecReduce_1  28# happyReduction_106
happyReduction_106 happy_x_1
	 =  case happyOut86 happy_x_1 of { (HappyWrap86 happy_var_1) -> 
	happyIn74
		 ((fst happy_var_1, Language.SQL.Abs.GrpItemCol (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_107 = happySpecReduce_1  28# happyReduction_107
happyReduction_107 happy_x_1
	 =  case happyOut75 happy_x_1 of { (HappyWrap75 happy_var_1) -> 
	happyIn74
		 ((fst happy_var_1, Language.SQL.Abs.GrpItemWin (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_108 = happyReduce 4# 29# happyReduction_108
happyReduction_108 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut83 happy_x_3 of { (HappyWrap83 happy_var_3) -> 
	happyIn75
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TumblingWindow (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_109 = happyReduce 6# 29# happyReduction_109
happyReduction_109 (happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut83 happy_x_3 of { (HappyWrap83 happy_var_3) -> 
	case happyOut83 happy_x_5 of { (HappyWrap83 happy_var_5) -> 
	happyIn75
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.HoppingWindow (Just (tokenLineCol happy_var_1)) (snd happy_var_3) (snd happy_var_5))
	) `HappyStk` happyRest}}}

happyReduce_110 = happyReduce 6# 29# happyReduction_110
happyReduction_110 (happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut83 happy_x_3 of { (HappyWrap83 happy_var_3) -> 
	case happyOut83 happy_x_5 of { (HappyWrap83 happy_var_5) -> 
	happyIn75
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SessionWindow (Just (tokenLineCol happy_var_1)) (snd happy_var_3) (snd happy_var_5))
	) `HappyStk` happyRest}}}

happyReduce_111 = happySpecReduce_0  30# happyReduction_111
happyReduction_111  =  happyIn76
		 ((Nothing, Language.SQL.Abs.DHavingEmpty Nothing)
	)

happyReduce_112 = happySpecReduce_2  30# happyReduction_112
happyReduction_112 happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut88 happy_x_2 of { (HappyWrap88 happy_var_2) -> 
	happyIn76
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DHaving (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_113 = happySpecReduce_3  31# happyReduction_113
happyReduction_113 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	case happyOut78 happy_x_3 of { (HappyWrap78 happy_var_3) -> 
	happyIn77
		 ((fst happy_var_1, Language.SQL.Abs.ExprAdd (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_114 = happySpecReduce_3  31# happyReduction_114
happyReduction_114 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	case happyOut78 happy_x_3 of { (HappyWrap78 happy_var_3) -> 
	happyIn77
		 ((fst happy_var_1, Language.SQL.Abs.ExprSub (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_115 = happySpecReduce_3  31# happyReduction_115
happyReduction_115 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut58 happy_x_2 of { (HappyWrap58 happy_var_2) -> 
	happyIn77
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.ExprArr (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_116 = happySpecReduce_3  31# happyReduction_116
happyReduction_116 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut84 happy_x_2 of { (HappyWrap84 happy_var_2) -> 
	happyIn77
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.ExprMap (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_117 = happySpecReduce_1  31# happyReduction_117
happyReduction_117 happy_x_1
	 =  case happyOut78 happy_x_1 of { (HappyWrap78 happy_var_1) -> 
	happyIn77
		 ((fst happy_var_1, snd happy_var_1)
	)}

happyReduce_118 = happySpecReduce_3  32# happyReduction_118
happyReduction_118 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut78 happy_x_1 of { (HappyWrap78 happy_var_1) -> 
	case happyOut79 happy_x_3 of { (HappyWrap79 happy_var_3) -> 
	happyIn78
		 ((fst happy_var_1, Language.SQL.Abs.ExprMul (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_119 = happySpecReduce_3  32# happyReduction_119
happyReduction_119 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut78 happy_x_1 of { (HappyWrap78 happy_var_1) -> 
	case happyOut79 happy_x_3 of { (HappyWrap79 happy_var_3) -> 
	happyIn78
		 ((fst happy_var_1, Language.SQL.Abs.ExprDiv (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_120 = happySpecReduce_1  32# happyReduction_120
happyReduction_120 happy_x_1
	 =  case happyOut79 happy_x_1 of { (HappyWrap79 happy_var_1) -> 
	happyIn78
		 ((fst happy_var_1, snd happy_var_1)
	)}

happyReduce_121 = happySpecReduce_1  33# happyReduction_121
happyReduction_121 happy_x_1
	 =  case happyOut48 happy_x_1 of { (HappyWrap48 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprInt (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_122 = happySpecReduce_1  33# happyReduction_122
happyReduction_122 happy_x_1
	 =  case happyOut47 happy_x_1 of { (HappyWrap47 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprNum (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_123 = happySpecReduce_1  33# happyReduction_123
happyReduction_123 happy_x_1
	 =  case happyOut49 happy_x_1 of { (HappyWrap49 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprString (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_124 = happySpecReduce_1  33# happyReduction_124
happyReduction_124 happy_x_1
	 =  case happyOut80 happy_x_1 of { (HappyWrap80 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprDate (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_125 = happySpecReduce_1  33# happyReduction_125
happyReduction_125 happy_x_1
	 =  case happyOut81 happy_x_1 of { (HappyWrap81 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprTime (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_126 = happySpecReduce_1  33# happyReduction_126
happyReduction_126 happy_x_1
	 =  case happyOut83 happy_x_1 of { (HappyWrap83 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprInterval (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_127 = happySpecReduce_1  33# happyReduction_127
happyReduction_127 happy_x_1
	 =  case happyOut86 happy_x_1 of { (HappyWrap86 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprColName (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_128 = happySpecReduce_1  33# happyReduction_128
happyReduction_128 happy_x_1
	 =  case happyOut87 happy_x_1 of { (HappyWrap87 happy_var_1) -> 
	happyIn79
		 ((fst happy_var_1, Language.SQL.Abs.ExprSetFunc (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_129 = happySpecReduce_3  33# happyReduction_129
happyReduction_129 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut77 happy_x_2 of { (HappyWrap77 happy_var_2) -> 
	happyIn79
		 ((Just (tokenLineCol happy_var_1), snd happy_var_2)
	)}}

happyReduce_130 = happyReduce 6# 34# happyReduction_130
happyReduction_130 (happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut48 happy_x_2 of { (HappyWrap48 happy_var_2) -> 
	case happyOut48 happy_x_4 of { (HappyWrap48 happy_var_4) -> 
	case happyOut48 happy_x_6 of { (HappyWrap48 happy_var_6) -> 
	happyIn80
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DDate (Just (tokenLineCol happy_var_1)) (snd happy_var_2) (snd happy_var_4) (snd happy_var_6))
	) `HappyStk` happyRest}}}}

happyReduce_131 = happyReduce 6# 35# happyReduction_131
happyReduction_131 (happy_x_6 `HappyStk`
	happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut48 happy_x_2 of { (HappyWrap48 happy_var_2) -> 
	case happyOut48 happy_x_4 of { (HappyWrap48 happy_var_4) -> 
	case happyOut48 happy_x_6 of { (HappyWrap48 happy_var_6) -> 
	happyIn81
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DTime (Just (tokenLineCol happy_var_1)) (snd happy_var_2) (snd happy_var_4) (snd happy_var_6))
	) `HappyStk` happyRest}}}}

happyReduce_132 = happySpecReduce_1  36# happyReduction_132
happyReduction_132 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn82
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TimeUnitYear (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_133 = happySpecReduce_1  36# happyReduction_133
happyReduction_133 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn82
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TimeUnitMonth (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_134 = happySpecReduce_1  36# happyReduction_134
happyReduction_134 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn82
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TimeUnitWeek (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_135 = happySpecReduce_1  36# happyReduction_135
happyReduction_135 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn82
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TimeUnitDay (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_136 = happySpecReduce_1  36# happyReduction_136
happyReduction_136 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn82
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TimeUnitMin (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_137 = happySpecReduce_1  36# happyReduction_137
happyReduction_137 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn82
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.TimeUnitSec (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_138 = happySpecReduce_3  37# happyReduction_138
happyReduction_138 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut48 happy_x_2 of { (HappyWrap48 happy_var_2) -> 
	case happyOut82 happy_x_3 of { (HappyWrap82 happy_var_3) -> 
	happyIn83
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.DInterval (Just (tokenLineCol happy_var_1)) (snd happy_var_2) (snd happy_var_3))
	)}}}

happyReduce_139 = happySpecReduce_0  38# happyReduction_139
happyReduction_139  =  happyIn84
		 ((Nothing, [])
	)

happyReduce_140 = happySpecReduce_1  38# happyReduction_140
happyReduction_140 happy_x_1
	 =  case happyOut85 happy_x_1 of { (HappyWrap85 happy_var_1) -> 
	happyIn84
		 ((fst happy_var_1, (:[]) (snd happy_var_1))
	)}

happyReduce_141 = happySpecReduce_3  38# happyReduction_141
happyReduction_141 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut85 happy_x_1 of { (HappyWrap85 happy_var_1) -> 
	case happyOut84 happy_x_3 of { (HappyWrap84 happy_var_3) -> 
	happyIn84
		 ((fst happy_var_1, (:) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_142 = happySpecReduce_3  39# happyReduction_142
happyReduction_142 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn85
		 ((fst happy_var_1, Language.SQL.Abs.DLabelledValueExpr (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_143 = happySpecReduce_1  40# happyReduction_143
happyReduction_143 happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	happyIn86
		 ((fst happy_var_1, Language.SQL.Abs.ColNameSimple (fst happy_var_1) (snd happy_var_1))
	)}

happyReduce_144 = happySpecReduce_3  40# happyReduction_144
happyReduction_144 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut46 happy_x_1 of { (HappyWrap46 happy_var_1) -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	happyIn86
		 ((fst happy_var_1, Language.SQL.Abs.ColNameStream (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_145 = happyReduce 4# 40# happyReduction_145
happyReduction_145 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOut86 happy_x_1 of { (HappyWrap86 happy_var_1) -> 
	case happyOut46 happy_x_3 of { (HappyWrap46 happy_var_3) -> 
	happyIn86
		 ((fst happy_var_1, Language.SQL.Abs.ColNameInner (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_146 = happyReduce 4# 40# happyReduction_146
happyReduction_146 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOut86 happy_x_1 of { (HappyWrap86 happy_var_1) -> 
	case happyOut48 happy_x_3 of { (HappyWrap48 happy_var_3) -> 
	happyIn86
		 ((fst happy_var_1, Language.SQL.Abs.ColNameIndex (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_147 = happySpecReduce_1  41# happyReduction_147
happyReduction_147 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn87
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SetFuncCountAll (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_148 = happyReduce 4# 41# happyReduction_148
happyReduction_148 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn87
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SetFuncCount (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_149 = happyReduce 4# 41# happyReduction_149
happyReduction_149 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn87
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SetFuncAvg (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_150 = happyReduce 4# 41# happyReduction_150
happyReduction_150 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn87
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SetFuncSum (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_151 = happyReduce 4# 41# happyReduction_151
happyReduction_151 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn87
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SetFuncMax (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_152 = happyReduce 4# 41# happyReduction_152
happyReduction_152 (happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn87
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.SetFuncMin (Just (tokenLineCol happy_var_1)) (snd happy_var_3))
	) `HappyStk` happyRest}}

happyReduce_153 = happySpecReduce_3  42# happyReduction_153
happyReduction_153 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut88 happy_x_1 of { (HappyWrap88 happy_var_1) -> 
	case happyOut89 happy_x_3 of { (HappyWrap89 happy_var_3) -> 
	happyIn88
		 ((fst happy_var_1, Language.SQL.Abs.CondOr (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_154 = happySpecReduce_1  42# happyReduction_154
happyReduction_154 happy_x_1
	 =  case happyOut89 happy_x_1 of { (HappyWrap89 happy_var_1) -> 
	happyIn88
		 ((fst happy_var_1, snd happy_var_1)
	)}

happyReduce_155 = happySpecReduce_3  43# happyReduction_155
happyReduction_155 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut89 happy_x_1 of { (HappyWrap89 happy_var_1) -> 
	case happyOut90 happy_x_3 of { (HappyWrap90 happy_var_3) -> 
	happyIn89
		 ((fst happy_var_1, Language.SQL.Abs.CondAnd (fst happy_var_1) (snd happy_var_1) (snd happy_var_3))
	)}}

happyReduce_156 = happySpecReduce_1  43# happyReduction_156
happyReduction_156 happy_x_1
	 =  case happyOut90 happy_x_1 of { (HappyWrap90 happy_var_1) -> 
	happyIn89
		 ((fst happy_var_1, snd happy_var_1)
	)}

happyReduce_157 = happySpecReduce_2  44# happyReduction_157
happyReduction_157 happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut91 happy_x_2 of { (HappyWrap91 happy_var_2) -> 
	happyIn90
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CondNot (Just (tokenLineCol happy_var_1)) (snd happy_var_2))
	)}}

happyReduce_158 = happySpecReduce_1  44# happyReduction_158
happyReduction_158 happy_x_1
	 =  case happyOut91 happy_x_1 of { (HappyWrap91 happy_var_1) -> 
	happyIn90
		 ((fst happy_var_1, snd happy_var_1)
	)}

happyReduce_159 = happySpecReduce_3  45# happyReduction_159
happyReduction_159 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	case happyOut92 happy_x_2 of { (HappyWrap92 happy_var_2) -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	happyIn91
		 ((fst happy_var_1, Language.SQL.Abs.CondOp (fst happy_var_1) (snd happy_var_1) (snd happy_var_2) (snd happy_var_3))
	)}}}

happyReduce_160 = happyReduce 5# 45# happyReduction_160
happyReduction_160 (happy_x_5 `HappyStk`
	happy_x_4 `HappyStk`
	happy_x_3 `HappyStk`
	happy_x_2 `HappyStk`
	happy_x_1 `HappyStk`
	happyRest)
	 = case happyOut77 happy_x_1 of { (HappyWrap77 happy_var_1) -> 
	case happyOut77 happy_x_3 of { (HappyWrap77 happy_var_3) -> 
	case happyOut77 happy_x_5 of { (HappyWrap77 happy_var_5) -> 
	happyIn91
		 ((fst happy_var_1, Language.SQL.Abs.CondBetween (fst happy_var_1) (snd happy_var_1) (snd happy_var_3) (snd happy_var_5))
	) `HappyStk` happyRest}}}

happyReduce_161 = happySpecReduce_3  45# happyReduction_161
happyReduction_161 happy_x_3
	happy_x_2
	happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	case happyOut88 happy_x_2 of { (HappyWrap88 happy_var_2) -> 
	happyIn91
		 ((Just (tokenLineCol happy_var_1), snd happy_var_2)
	)}}

happyReduce_162 = happySpecReduce_1  46# happyReduction_162
happyReduction_162 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn92
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CompOpEQ (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_163 = happySpecReduce_1  46# happyReduction_163
happyReduction_163 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn92
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CompOpNE (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_164 = happySpecReduce_1  46# happyReduction_164
happyReduction_164 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn92
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CompOpLT (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_165 = happySpecReduce_1  46# happyReduction_165
happyReduction_165 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn92
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CompOpGT (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_166 = happySpecReduce_1  46# happyReduction_166
happyReduction_166 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn92
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CompOpLEQ (Just (tokenLineCol happy_var_1)))
	)}

happyReduce_167 = happySpecReduce_1  46# happyReduction_167
happyReduction_167 happy_x_1
	 =  case happyOutTok happy_x_1 of { happy_var_1 -> 
	happyIn92
		 ((Just (tokenLineCol happy_var_1), Language.SQL.Abs.CompOpGEQ (Just (tokenLineCol happy_var_1)))
	)}

happyNewToken action sts stk [] =
	happyDoAction 76# notHappyAtAll action sts stk []

happyNewToken action sts stk (tk:tks) =
	let cont i = happyDoAction i tk action sts stk tks in
	case tk of {
	PT _ (TS _ 1) -> cont 1#;
	PT _ (TS _ 2) -> cont 2#;
	PT _ (TS _ 3) -> cont 3#;
	PT _ (TS _ 4) -> cont 4#;
	PT _ (TS _ 5) -> cont 5#;
	PT _ (TS _ 6) -> cont 6#;
	PT _ (TS _ 7) -> cont 7#;
	PT _ (TS _ 8) -> cont 8#;
	PT _ (TS _ 9) -> cont 9#;
	PT _ (TS _ 10) -> cont 10#;
	PT _ (TS _ 11) -> cont 11#;
	PT _ (TS _ 12) -> cont 12#;
	PT _ (TS _ 13) -> cont 13#;
	PT _ (TS _ 14) -> cont 14#;
	PT _ (TS _ 15) -> cont 15#;
	PT _ (TS _ 16) -> cont 16#;
	PT _ (TS _ 17) -> cont 17#;
	PT _ (TS _ 18) -> cont 18#;
	PT _ (TS _ 19) -> cont 19#;
	PT _ (TS _ 20) -> cont 20#;
	PT _ (TS _ 21) -> cont 21#;
	PT _ (TS _ 22) -> cont 22#;
	PT _ (TS _ 23) -> cont 23#;
	PT _ (TS _ 24) -> cont 24#;
	PT _ (TS _ 25) -> cont 25#;
	PT _ (TS _ 26) -> cont 26#;
	PT _ (TS _ 27) -> cont 27#;
	PT _ (TS _ 28) -> cont 28#;
	PT _ (TS _ 29) -> cont 29#;
	PT _ (TS _ 30) -> cont 30#;
	PT _ (TS _ 31) -> cont 31#;
	PT _ (TS _ 32) -> cont 32#;
	PT _ (TS _ 33) -> cont 33#;
	PT _ (TS _ 34) -> cont 34#;
	PT _ (TS _ 35) -> cont 35#;
	PT _ (TS _ 36) -> cont 36#;
	PT _ (TS _ 37) -> cont 37#;
	PT _ (TS _ 38) -> cont 38#;
	PT _ (TS _ 39) -> cont 39#;
	PT _ (TS _ 40) -> cont 40#;
	PT _ (TS _ 41) -> cont 41#;
	PT _ (TS _ 42) -> cont 42#;
	PT _ (TS _ 43) -> cont 43#;
	PT _ (TS _ 44) -> cont 44#;
	PT _ (TS _ 45) -> cont 45#;
	PT _ (TS _ 46) -> cont 46#;
	PT _ (TS _ 47) -> cont 47#;
	PT _ (TS _ 48) -> cont 48#;
	PT _ (TS _ 49) -> cont 49#;
	PT _ (TS _ 50) -> cont 50#;
	PT _ (TS _ 51) -> cont 51#;
	PT _ (TS _ 52) -> cont 52#;
	PT _ (TS _ 53) -> cont 53#;
	PT _ (TS _ 54) -> cont 54#;
	PT _ (TS _ 55) -> cont 55#;
	PT _ (TS _ 56) -> cont 56#;
	PT _ (TS _ 57) -> cont 57#;
	PT _ (TS _ 58) -> cont 58#;
	PT _ (TS _ 59) -> cont 59#;
	PT _ (TS _ 60) -> cont 60#;
	PT _ (TS _ 61) -> cont 61#;
	PT _ (TS _ 62) -> cont 62#;
	PT _ (TS _ 63) -> cont 63#;
	PT _ (TS _ 64) -> cont 64#;
	PT _ (TS _ 65) -> cont 65#;
	PT _ (TS _ 66) -> cont 66#;
	PT _ (TS _ 67) -> cont 67#;
	PT _ (TS _ 68) -> cont 68#;
	PT _ (TS _ 69) -> cont 69#;
	PT _ (TS _ 70) -> cont 70#;
	PT _ (TS _ 71) -> cont 71#;
	PT _ (TV _) -> cont 72#;
	PT _ (TD _) -> cont 73#;
	PT _ (TI _) -> cont 74#;
	PT _ (TL _) -> cont 75#;
	_ -> happyError' ((tk:tks), [])
	}

happyError_ explist 76# tk tks = happyError' (tks, explist)
happyError_ explist _ tk tks = happyError' ((tk:tks), explist)

happyThen :: () => Either String a -> (a -> Either String b) -> Either String b
happyThen = ((>>=))
happyReturn :: () => a -> Either String a
happyReturn = (return)
happyThen1 m k tks = ((>>=)) m (\a -> k a tks)
happyReturn1 :: () => a -> b -> Either String a
happyReturn1 = \a tks -> (return) a
happyError' :: () => ([(Token)], [Prelude.String]) -> Either String a
happyError' = (\(tokens, _) -> happyError tokens)
pSQL_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 0# tks) (\x -> happyReturn (let {(HappyWrap50 x') = happyOut50 x} in x'))

pCreate_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 1# tks) (\x -> happyReturn (let {(HappyWrap51 x') = happyOut51 x} in x'))

pListSchemaElem_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 2# tks) (\x -> happyReturn (let {(HappyWrap52 x') = happyOut52 x} in x'))

pListStreamOption_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 3# tks) (\x -> happyReturn (let {(HappyWrap53 x') = happyOut53 x} in x'))

pSchemaElem_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 4# tks) (\x -> happyReturn (let {(HappyWrap54 x') = happyOut54 x} in x'))

pDataType_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 5# tks) (\x -> happyReturn (let {(HappyWrap55 x') = happyOut55 x} in x'))

pStreamOption_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 6# tks) (\x -> happyReturn (let {(HappyWrap56 x') = happyOut56 x} in x'))

pInsert_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 7# tks) (\x -> happyReturn (let {(HappyWrap57 x') = happyOut57 x} in x'))

pListValueExpr_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 8# tks) (\x -> happyReturn (let {(HappyWrap58 x') = happyOut58 x} in x'))

pListIdent_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 9# tks) (\x -> happyReturn (let {(HappyWrap59 x') = happyOut59 x} in x'))

pSelect_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 10# tks) (\x -> happyReturn (let {(HappyWrap60 x') = happyOut60 x} in x'))

pSel_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 11# tks) (\x -> happyReturn (let {(HappyWrap61 x') = happyOut61 x} in x'))

pSelList_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 12# tks) (\x -> happyReturn (let {(HappyWrap62 x') = happyOut62 x} in x'))

pListDerivedCol_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 13# tks) (\x -> happyReturn (let {(HappyWrap63 x') = happyOut63 x} in x'))

pDerivedCol_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 14# tks) (\x -> happyReturn (let {(HappyWrap64 x') = happyOut64 x} in x'))

pFrom_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 15# tks) (\x -> happyReturn (let {(HappyWrap65 x') = happyOut65 x} in x'))

pListTableRef_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 16# tks) (\x -> happyReturn (let {(HappyWrap66 x') = happyOut66 x} in x'))

pTableRef_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 17# tks) (\x -> happyReturn (let {(HappyWrap67 x') = happyOut67 x} in x'))

pJoinType_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 18# tks) (\x -> happyReturn (let {(HappyWrap68 x') = happyOut68 x} in x'))

pJoinWindow_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 19# tks) (\x -> happyReturn (let {(HappyWrap69 x') = happyOut69 x} in x'))

pJoinCond_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 20# tks) (\x -> happyReturn (let {(HappyWrap70 x') = happyOut70 x} in x'))

pWhere_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 21# tks) (\x -> happyReturn (let {(HappyWrap71 x') = happyOut71 x} in x'))

pGroupBy_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 22# tks) (\x -> happyReturn (let {(HappyWrap72 x') = happyOut72 x} in x'))

pListGrpItem_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 23# tks) (\x -> happyReturn (let {(HappyWrap73 x') = happyOut73 x} in x'))

pGrpItem_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 24# tks) (\x -> happyReturn (let {(HappyWrap74 x') = happyOut74 x} in x'))

pWindow_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 25# tks) (\x -> happyReturn (let {(HappyWrap75 x') = happyOut75 x} in x'))

pHaving_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 26# tks) (\x -> happyReturn (let {(HappyWrap76 x') = happyOut76 x} in x'))

pValueExpr_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 27# tks) (\x -> happyReturn (let {(HappyWrap77 x') = happyOut77 x} in x'))

pValueExpr1_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 28# tks) (\x -> happyReturn (let {(HappyWrap78 x') = happyOut78 x} in x'))

pValueExpr2_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 29# tks) (\x -> happyReturn (let {(HappyWrap79 x') = happyOut79 x} in x'))

pDate_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 30# tks) (\x -> happyReturn (let {(HappyWrap80 x') = happyOut80 x} in x'))

pTime_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 31# tks) (\x -> happyReturn (let {(HappyWrap81 x') = happyOut81 x} in x'))

pTimeUnit_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 32# tks) (\x -> happyReturn (let {(HappyWrap82 x') = happyOut82 x} in x'))

pInterval_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 33# tks) (\x -> happyReturn (let {(HappyWrap83 x') = happyOut83 x} in x'))

pListLabelledValueExpr_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 34# tks) (\x -> happyReturn (let {(HappyWrap84 x') = happyOut84 x} in x'))

pLabelledValueExpr_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 35# tks) (\x -> happyReturn (let {(HappyWrap85 x') = happyOut85 x} in x'))

pColName_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 36# tks) (\x -> happyReturn (let {(HappyWrap86 x') = happyOut86 x} in x'))

pSetFunc_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 37# tks) (\x -> happyReturn (let {(HappyWrap87 x') = happyOut87 x} in x'))

pSearchCond_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 38# tks) (\x -> happyReturn (let {(HappyWrap88 x') = happyOut88 x} in x'))

pSearchCond1_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 39# tks) (\x -> happyReturn (let {(HappyWrap89 x') = happyOut89 x} in x'))

pSearchCond2_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 40# tks) (\x -> happyReturn (let {(HappyWrap90 x') = happyOut90 x} in x'))

pSearchCond3_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 41# tks) (\x -> happyReturn (let {(HappyWrap91 x') = happyOut91 x} in x'))

pCompOp_internal tks = happySomeParser where
 happySomeParser = happyThen (happyParse 42# tks) (\x -> happyReturn (let {(HappyWrap92 x') = happyOut92 x} in x'))

happySeq = happyDontSeq


happyError :: [Token] -> Either String a
happyError ts = Left $
  "syntax error at " ++ tokenPos ts ++
  case ts of
    [] -> []
    [Err _] -> " due to lexer error"
    t:_ -> " before `" ++ (prToken t) ++ "'"

myLexer = tokens

pSQL = (>>= return . snd) . pSQL_internal
pCreate = (>>= return . snd) . pCreate_internal
pListSchemaElem = (>>= return . snd) . pListSchemaElem_internal
pListStreamOption = (>>= return . snd) . pListStreamOption_internal
pSchemaElem = (>>= return . snd) . pSchemaElem_internal
pDataType = (>>= return . snd) . pDataType_internal
pStreamOption = (>>= return . snd) . pStreamOption_internal
pInsert = (>>= return . snd) . pInsert_internal
pListValueExpr = (>>= return . snd) . pListValueExpr_internal
pListIdent = (>>= return . snd) . pListIdent_internal
pSelect = (>>= return . snd) . pSelect_internal
pSel = (>>= return . snd) . pSel_internal
pSelList = (>>= return . snd) . pSelList_internal
pListDerivedCol = (>>= return . snd) . pListDerivedCol_internal
pDerivedCol = (>>= return . snd) . pDerivedCol_internal
pFrom = (>>= return . snd) . pFrom_internal
pListTableRef = (>>= return . snd) . pListTableRef_internal
pTableRef = (>>= return . snd) . pTableRef_internal
pJoinType = (>>= return . snd) . pJoinType_internal
pJoinWindow = (>>= return . snd) . pJoinWindow_internal
pJoinCond = (>>= return . snd) . pJoinCond_internal
pWhere = (>>= return . snd) . pWhere_internal
pGroupBy = (>>= return . snd) . pGroupBy_internal
pListGrpItem = (>>= return . snd) . pListGrpItem_internal
pGrpItem = (>>= return . snd) . pGrpItem_internal
pWindow = (>>= return . snd) . pWindow_internal
pHaving = (>>= return . snd) . pHaving_internal
pValueExpr = (>>= return . snd) . pValueExpr_internal
pValueExpr1 = (>>= return . snd) . pValueExpr1_internal
pValueExpr2 = (>>= return . snd) . pValueExpr2_internal
pDate = (>>= return . snd) . pDate_internal
pTime = (>>= return . snd) . pTime_internal
pTimeUnit = (>>= return . snd) . pTimeUnit_internal
pInterval = (>>= return . snd) . pInterval_internal
pListLabelledValueExpr = (>>= return . snd) . pListLabelledValueExpr_internal
pLabelledValueExpr = (>>= return . snd) . pLabelledValueExpr_internal
pColName = (>>= return . snd) . pColName_internal
pSetFunc = (>>= return . snd) . pSetFunc_internal
pSearchCond = (>>= return . snd) . pSearchCond_internal
pSearchCond1 = (>>= return . snd) . pSearchCond1_internal
pSearchCond2 = (>>= return . snd) . pSearchCond2_internal
pSearchCond3 = (>>= return . snd) . pSearchCond3_internal
pCompOp = (>>= return . snd) . pCompOp_internal
{-# LINE 1 "templates/GenericTemplate.hs" #-}
-- $Id: GenericTemplate.hs,v 1.26 2005/01/14 14:47:22 simonmar Exp $













-- Do not remove this comment. Required to fix CPP parsing when using GCC and a clang-compiled alex.
#if __GLASGOW_HASKELL__ > 706
#define LT(n,m) ((Happy_GHC_Exts.tagToEnum# (n Happy_GHC_Exts.<# m)) :: Prelude.Bool)
#define GTE(n,m) ((Happy_GHC_Exts.tagToEnum# (n Happy_GHC_Exts.>=# m)) :: Prelude.Bool)
#define EQ(n,m) ((Happy_GHC_Exts.tagToEnum# (n Happy_GHC_Exts.==# m)) :: Prelude.Bool)
#else
#define LT(n,m) (n Happy_GHC_Exts.<# m)
#define GTE(n,m) (n Happy_GHC_Exts.>=# m)
#define EQ(n,m) (n Happy_GHC_Exts.==# m)
#endif



















data Happy_IntList = HappyCons Happy_GHC_Exts.Int# Happy_IntList








































infixr 9 `HappyStk`
data HappyStk a = HappyStk a (HappyStk a)

-----------------------------------------------------------------------------
-- starting the parse

happyParse start_state = happyNewToken start_state notHappyAtAll notHappyAtAll

-----------------------------------------------------------------------------
-- Accepting the parse

-- If the current token is ERROR_TOK, it means we've just accepted a partial
-- parse (a %partial parser).  We must ignore the saved token on the top of
-- the stack in this case.
happyAccept 0# tk st sts (_ `HappyStk` ans `HappyStk` _) =
        happyReturn1 ans
happyAccept j tk st sts (HappyStk ans _) = 
        (happyTcHack j (happyTcHack st)) (happyReturn1 ans)

-----------------------------------------------------------------------------
-- Arrays only: do the next action



happyDoAction i tk st
        = {- nothing -}
          case action of
                0#           -> {- nothing -}
                                     happyFail (happyExpListPerState ((Happy_GHC_Exts.I# (st)) :: Prelude.Int)) i tk st
                -1#          -> {- nothing -}
                                     happyAccept i tk st
                n | LT(n,(0# :: Happy_GHC_Exts.Int#)) -> {- nothing -}
                                                   (happyReduceArr Happy_Data_Array.! rule) i tk st
                                                   where rule = (Happy_GHC_Exts.I# ((Happy_GHC_Exts.negateInt# ((n Happy_GHC_Exts.+# (1# :: Happy_GHC_Exts.Int#))))))
                n                 -> {- nothing -}
                                     happyShift new_state i tk st
                                     where new_state = (n Happy_GHC_Exts.-# (1# :: Happy_GHC_Exts.Int#))
   where off    = happyAdjustOffset (indexShortOffAddr happyActOffsets st)
         off_i  = (off Happy_GHC_Exts.+# i)
         check  = if GTE(off_i,(0# :: Happy_GHC_Exts.Int#))
                  then EQ(indexShortOffAddr happyCheck off_i, i)
                  else Prelude.False
         action
          | check     = indexShortOffAddr happyTable off_i
          | Prelude.otherwise = indexShortOffAddr happyDefActions st




indexShortOffAddr (HappyA# arr) off =
        Happy_GHC_Exts.narrow16Int# i
  where
        i = Happy_GHC_Exts.word2Int# (Happy_GHC_Exts.or# (Happy_GHC_Exts.uncheckedShiftL# high 8#) low)
        high = Happy_GHC_Exts.int2Word# (Happy_GHC_Exts.ord# (Happy_GHC_Exts.indexCharOffAddr# arr (off' Happy_GHC_Exts.+# 1#)))
        low  = Happy_GHC_Exts.int2Word# (Happy_GHC_Exts.ord# (Happy_GHC_Exts.indexCharOffAddr# arr off'))
        off' = off Happy_GHC_Exts.*# 2#




{-# INLINE happyLt #-}
happyLt x y = LT(x,y)


readArrayBit arr bit =
    Bits.testBit (Happy_GHC_Exts.I# (indexShortOffAddr arr ((unbox_int bit) `Happy_GHC_Exts.iShiftRA#` 4#))) (bit `Prelude.mod` 16)
  where unbox_int (Happy_GHC_Exts.I# x) = x






data HappyAddr = HappyA# Happy_GHC_Exts.Addr#


-----------------------------------------------------------------------------
-- HappyState data type (not arrays)













-----------------------------------------------------------------------------
-- Shifting a token

happyShift new_state 0# tk st sts stk@(x `HappyStk` _) =
     let i = (case Happy_GHC_Exts.unsafeCoerce# x of { (Happy_GHC_Exts.I# (i)) -> i }) in
--     trace "shifting the error token" $
     happyDoAction i tk new_state (HappyCons (st) (sts)) (stk)

happyShift new_state i tk st sts stk =
     happyNewToken new_state (HappyCons (st) (sts)) ((happyInTok (tk))`HappyStk`stk)

-- happyReduce is specialised for the common cases.

happySpecReduce_0 i fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happySpecReduce_0 nt fn j tk st@((action)) sts stk
     = happyGoto nt j tk st (HappyCons (st) (sts)) (fn `HappyStk` stk)

happySpecReduce_1 i fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happySpecReduce_1 nt fn j tk _ sts@((HappyCons (st@(action)) (_))) (v1`HappyStk`stk')
     = let r = fn v1 in
       happySeq r (happyGoto nt j tk st sts (r `HappyStk` stk'))

happySpecReduce_2 i fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happySpecReduce_2 nt fn j tk _ (HappyCons (_) (sts@((HappyCons (st@(action)) (_))))) (v1`HappyStk`v2`HappyStk`stk')
     = let r = fn v1 v2 in
       happySeq r (happyGoto nt j tk st sts (r `HappyStk` stk'))

happySpecReduce_3 i fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happySpecReduce_3 nt fn j tk _ (HappyCons (_) ((HappyCons (_) (sts@((HappyCons (st@(action)) (_))))))) (v1`HappyStk`v2`HappyStk`v3`HappyStk`stk')
     = let r = fn v1 v2 v3 in
       happySeq r (happyGoto nt j tk st sts (r `HappyStk` stk'))

happyReduce k i fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happyReduce k nt fn j tk st sts stk
     = case happyDrop (k Happy_GHC_Exts.-# (1# :: Happy_GHC_Exts.Int#)) sts of
         sts1@((HappyCons (st1@(action)) (_))) ->
                let r = fn stk in  -- it doesn't hurt to always seq here...
                happyDoSeq r (happyGoto nt j tk st1 sts1 r)

happyMonadReduce k nt fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happyMonadReduce k nt fn j tk st sts stk =
      case happyDrop k (HappyCons (st) (sts)) of
        sts1@((HappyCons (st1@(action)) (_))) ->
          let drop_stk = happyDropStk k stk in
          happyThen1 (fn stk tk) (\r -> happyGoto nt j tk st1 sts1 (r `HappyStk` drop_stk))

happyMonad2Reduce k nt fn 0# tk st sts stk
     = happyFail [] 0# tk st sts stk
happyMonad2Reduce k nt fn j tk st sts stk =
      case happyDrop k (HappyCons (st) (sts)) of
        sts1@((HappyCons (st1@(action)) (_))) ->
         let drop_stk = happyDropStk k stk

             off = happyAdjustOffset (indexShortOffAddr happyGotoOffsets st1)
             off_i = (off Happy_GHC_Exts.+# nt)
             new_state = indexShortOffAddr happyTable off_i




          in
          happyThen1 (fn stk tk) (\r -> happyNewToken new_state sts1 (r `HappyStk` drop_stk))

happyDrop 0# l = l
happyDrop n (HappyCons (_) (t)) = happyDrop (n Happy_GHC_Exts.-# (1# :: Happy_GHC_Exts.Int#)) t

happyDropStk 0# l = l
happyDropStk n (x `HappyStk` xs) = happyDropStk (n Happy_GHC_Exts.-# (1#::Happy_GHC_Exts.Int#)) xs

-----------------------------------------------------------------------------
-- Moving to a new state after a reduction


happyGoto nt j tk st = 
   {- nothing -}
   happyDoAction j tk new_state
   where off = happyAdjustOffset (indexShortOffAddr happyGotoOffsets st)
         off_i = (off Happy_GHC_Exts.+# nt)
         new_state = indexShortOffAddr happyTable off_i




-----------------------------------------------------------------------------
-- Error recovery (ERROR_TOK is the error token)

-- parse error if we are in recovery and we fail again
happyFail explist 0# tk old_st _ stk@(x `HappyStk` _) =
     let i = (case Happy_GHC_Exts.unsafeCoerce# x of { (Happy_GHC_Exts.I# (i)) -> i }) in
--      trace "failing" $ 
        happyError_ explist i tk

{-  We don't need state discarding for our restricted implementation of
    "error".  In fact, it can cause some bogus parses, so I've disabled it
    for now --SDM

-- discard a state
happyFail  ERROR_TOK tk old_st CONS(HAPPYSTATE(action),sts) 
                                                (saved_tok `HappyStk` _ `HappyStk` stk) =
--      trace ("discarding state, depth " ++ show (length stk))  $
        DO_ACTION(action,ERROR_TOK,tk,sts,(saved_tok`HappyStk`stk))
-}

-- Enter error recovery: generate an error token,
--                       save the old token and carry on.
happyFail explist i tk (action) sts stk =
--      trace "entering error recovery" $
        happyDoAction 0# tk action sts ((Happy_GHC_Exts.unsafeCoerce# (Happy_GHC_Exts.I# (i))) `HappyStk` stk)

-- Internal happy errors:

notHappyAtAll :: a
notHappyAtAll = Prelude.error "Internal Happy error\n"

-----------------------------------------------------------------------------
-- Hack to get the typechecker to accept our action functions


happyTcHack :: Happy_GHC_Exts.Int# -> a -> a
happyTcHack x y = y
{-# INLINE happyTcHack #-}


-----------------------------------------------------------------------------
-- Seq-ing.  If the --strict flag is given, then Happy emits 
--      happySeq = happyDoSeq
-- otherwise it emits
--      happySeq = happyDontSeq

happyDoSeq, happyDontSeq :: a -> b -> b
happyDoSeq   a b = a `Prelude.seq` b
happyDontSeq a b = b

-----------------------------------------------------------------------------
-- Don't inline any functions from the template.  GHC has a nasty habit
-- of deciding to inline happyGoto everywhere, which increases the size of
-- the generated parser quite a bit.


{-# NOINLINE happyDoAction #-}
{-# NOINLINE happyTable #-}
{-# NOINLINE happyCheck #-}
{-# NOINLINE happyActOffsets #-}
{-# NOINLINE happyGotoOffsets #-}
{-# NOINLINE happyDefActions #-}

{-# NOINLINE happyShift #-}
{-# NOINLINE happySpecReduce_0 #-}
{-# NOINLINE happySpecReduce_1 #-}
{-# NOINLINE happySpecReduce_2 #-}
{-# NOINLINE happySpecReduce_3 #-}
{-# NOINLINE happyReduce #-}
{-# NOINLINE happyMonadReduce #-}
{-# NOINLINE happyGoto #-}
{-# NOINLINE happyFail #-}

-- end of Happy Template.
