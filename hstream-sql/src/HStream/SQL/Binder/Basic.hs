{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.Binder.Basic where

import qualified Data.Aeson                as Aeson
import           Data.Hashable
import           Data.Int                  (Int64)
import           Data.Maybe                (fromJust)
import           Data.Text                 (Text)
import qualified Data.Text                 as Text
import qualified Data.Time                 as Time
import           Data.Time.Format.ISO8601  (iso8601ParseM, iso8601Show)
import           GHC.Generics              (Generic)
import           HStream.SQL.Binder.Common
import           Z.Data.CBytes             (CBytes)

import           HStream.SQL.Abs
import           HStream.SQL.Extra
import           HStream.Utils             ()

----------------------------------------
--             constants
----------------------------------------
type ConnectorType = Text

type instance BoundType PNInteger = Integer
instance Bind PNInteger where
  bind = return . extractPNInteger

type instance BoundType PNDouble = Double
instance Bind PNDouble where
  bind = return . extractPNDouble

type instance BoundType SingleQuoted = Text
instance Bind SingleQuoted where
  bind = return . extractSingleQuoted

type instance BoundType ColumnIdent = Text
instance Bind ColumnIdent where
  bind = return . extractColumnIdent

type instance BoundType HIdent = Text
instance Bind HIdent where
  bind = return . extractHIdent

type instance BoundType Boolean = Bool
instance Bind Boolean where
  bind (BoolTrue _ ) = return True
  bind (BoolFalse _) = return False


type BoundDate = Time.Day
type instance BoundType Date = BoundDate
instance Bind Date where
  bind (DDate _ (SingleQuoted date)) =
    return $ (fromJust . iso8601ParseM . tail . init) (Text.unpack date)

type BoundTime = Time.TimeOfDay
type instance BoundType Time = BoundTime
instance Bind Time where
  bind (DTime _ (SingleQuoted time)) =
    return $ (fromJust . iso8601ParseM . tail . init) (Text.unpack time)

type BoundTimestamp = Time.ZonedTime
type instance BoundType Timestamp = BoundTimestamp
instance Bind Timestamp where
  bind (DTimestamp _ (SingleQuoted timestamp)) =
    return $ (fromJust . iso8601ParseM . tail . init) (Text.unpack timestamp)

instance Eq Time.ZonedTime where
  z1 == z2 = Time.zonedTimeToUTC z1 == Time.zonedTimeToUTC z2
instance Ord Time.ZonedTime where
  z1 `compare` z2 = Time.zonedTimeToUTC z1 `compare` Time.zonedTimeToUTC z2
instance Hashable Time.ZonedTime where
  hashWithSalt salt z = hashWithSalt salt (Time.zonedTimeToUTC z)

type BoundIntervalUnit = (Integer, Integer) -- from/toMonth, Second
type instance BoundType IntervalUnit = BoundIntervalUnit
instance Bind IntervalUnit where
  bind (IntervalSecond _) = return (30 * 24 * 60 * 60, 1)
  bind (IntervalMinute _) = return (30 * 24 * 60 , 60)
  bind (IntervalHour   _) = return (30 * 24, 3600)
  bind (IntervalDay    _) = return (30, 3600 * 24)
  bind (IntervalMonth  _) = return (1, 0)
  bind (IntervalYear   _) = return (12, 0)

type BoundInterval = Time.CalendarDiffTime
type instance BoundType Interval = BoundInterval
instance Bind Interval where
  bind (DInterval _ n iUnit) = do
    unit <- bind iUnit
    return $ fromUnitToDiffTime unit n
    where
      fromUnitToDiffTime :: (Integer, Integer) -> Integer -> Time.CalendarDiffTime
      fromUnitToDiffTime (m, s) x
        | s == 0    = Time.CalendarDiffTime (x * m) 0
        | otherwise = let (m', rest) = divMod x m in Time.CalendarDiffTime m' (fromIntegral (rest * s))

instance Ord Time.CalendarDiffTime where
  d1 `compare` d2 =
    case (Time.ctMonths d1) `compare` (Time.ctMonths d2) of
      GT -> GT
      LT -> LT
      EQ -> Time.ctTime d1 `compare` Time.ctTime d2
instance Hashable Time.CalendarDiffTime where
  hashWithSalt salt d = hashWithSalt salt (show d)

data Constant = ConstantNull
              | ConstantInt       Int
              | ConstantFloat     Double
              | ConstantText      Text
              | ConstantBoolean   Bool
              | ConstantDate      BoundDate
              | ConstantTime      BoundTime
              | ConstantTimestamp BoundTimestamp
              | ConstantInterval  BoundInterval
              | ConstantBytea     CBytes
              | ConstantJsonb     Aeson.Object
              | ConstantArray     [Constant]
              deriving (Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance Show Constant where
  show ConstantNull           = "NULL"
  show (ConstantInt i)        = show i
  show (ConstantFloat f)      = show f
  show (ConstantText t)       = show t
  show (ConstantBoolean b)    = show b
  show (ConstantDate d)       = show d
  show (ConstantTime t)       = show t
  show (ConstantTimestamp ts) = show ts
  show (ConstantInterval i)   = show i
  show (ConstantBytea b)      = show b
  show (ConstantJsonb j)      = show j
  show (ConstantArray a)      = show a

----------------------------------------
--            operators
----------------------------------------

data BinaryOp = OpAnd | OpOr
              | OpEQ  | OpNEQ | OpLT | OpGT | OpLEQ | OpGEQ
              | OpAdd | OpSub | OpMul
              | OpContain | OpExcept  | OpIntersect | OpRemove | OpUnion | OpArrJoin'
              | OpIfNull  | OpNullIf  | OpDateStr   | OpStrDate
              | OpSplit   | OpChunksOf
              | OpTake    | OpTakeEnd | OpDrop      | OpDropEnd
              deriving (Eq, Show, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data UnaryOp  = OpSin      | OpSinh    | OpAsin   | OpAsinh  | OpCos   | OpCosh
              | OpAcos     | OpAcosh   | OpTan    | OpTanh   | OpAtan  | OpAtanh
              | OpAbs      | OpCeil    | OpFloor  | OpRound  | OpSign
              | OpSqrt     | OpLog     | OpLog2   | OpLog10  | OpExp
              | OpIsInt    | OpIsFloat | OpIsBool | OpIsStr
              | OpIsArr    | OpIsDate  | OpIsTime | OpIsNum
              | OpToStr
              | OpToLower  | OpToUpper | OpTrim   | OpLTrim  | OpRTrim
              | OpReverse  | OpStrLen
              | OpDistinct | OpArrJoin | OpLength | OpArrMax | OpArrMin | OpSort
              | OpNot
              deriving (Eq, Show, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data JsonOp
  = JOpArrow         -- json -> text = value
  | JOpLongArrow     -- json ->> text = text
  | JOpHashArrow     -- json #> array[text/int] = value
  | JOpHashLongArrow -- json #>> array[text/int] = text
  deriving (Eq, Show, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data TerOp
  = OpBetweenAnd
  | OpNotBetweenAnd
  | OpBetweenSymAnd
  | OpNotBetweenSymAnd
  deriving (Show, Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

----------------------------------------
--             array access
----------------------------------------
data BoundArrayAccessRhs
  = BoundArrayAccessRhsIndex Int
  | BoundArrayAccessRhsRange (Maybe Int) (Maybe Int)
  deriving (Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show BoundArrayAccessRhs where
  show (BoundArrayAccessRhsIndex n) = "[" <> show n <> "]"
  show (BoundArrayAccessRhsRange l_m r_m) =
    let l = case l_m of
              Nothing -> ""
              Just l  -> show l
        r = case r_m of
              Nothing -> ""
              Just r  -> show r
     in "[" <> l <> ":" <> r <> "]"

type instance BoundType ArrayAccessRhs = BoundArrayAccessRhs
instance Bind ArrayAccessRhs where
  bind rhs = case rhs of
    ArrayAccessRhsIndex  _ n     -> return $ BoundArrayAccessRhsIndex (fromInteger n)
    ArrayAccessRhsFrom   _ n     -> return $ BoundArrayAccessRhsRange (Just $ fromInteger n) Nothing
    ArrayAccessRhsTo     _    n  -> return $ BoundArrayAccessRhsRange Nothing (Just $ fromInteger n)
    ArrayAccessRhsFromTo _ n1 n2 -> return $ BoundArrayAccessRhsRange (Just $ fromInteger n1) (Just $ fromInteger n2)
