{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.Utils where

import           Data.Function         ((&))
import           Data.List             (foldl')
import           Data.Maybe            (fromMaybe)
import qualified Data.HashMap.Strict   as HM
import           Data.Scientific
import           Data.Text             (Text)
import qualified Data.Text             as T

import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeSQLException (..),
                                        throwSQLException)
import           HStream.Utils
import           Text.StringRandom     (stringRandomIO)
import qualified Z.Data.CBytes         as ZCB
import           Z.IO.Time

import           DiffFlow.Error

funcOnScientific :: RealFloat a => (a -> a) -> Scientific -> Scientific
funcOnScientific f = fromFloatDigits . f . toRealFloat

-- | Creates a flat string representation of all the elements contained in the given array.
-- Elements in the resulting string are separated by
-- the chosen 'delimiter :: Text', which is an optional
-- parameter that falls back to a comma @,@.
arrJoinPrim :: [FlowValue] -> Maybe T.Text -> T.Text
arrJoinPrim xs delimiterM | null xs = T.empty
  | otherwise = T.dropEnd 1 $ foldl' h T.empty xs where
  delimiter :: T.Text
  delimiter = fromMaybe "," delimiterM
  h :: T.Text -> FlowValue -> T.Text
  h txt val = txt <> (case val of -- Test whether the given value is a primary value.
    FlowNull         -> "NULL"
    FlowInt n        -> T.pack (show n)
    FlowFloat n      -> T.pack (show n)
    FlowNumeral n    -> T.pack (show n)
    FlowBoolean b    -> T.pack (show b)
    FlowByte b       -> T.pack (show b)
    FlowText str     -> str
    FlowDate d       -> T.pack (show d)
    FlowTime t       -> T.pack (show t)
    FlowTimestamp ts -> T.pack (show ts)
    FlowInterval i   -> T.pack (show i)
    notPrim          -> throwSQLException CodegenException Nothing
      ("Operation OpArrJoin on " <> show notPrim <> " is not supported")
    ) <> delimiter

strToDateGMT :: T.Text -> T.Text -> Scientific
strToDateGMT date fmt = parseSystemTimeGMT (case timeFmt fmt of
  Just x -> x
  _ -> throwSQLException CodegenException Nothing
    ("Operation OpStrDate on time format " <> show fmt <> " is not supported")) (textToCBytes date)
      & systemSeconds & Prelude.toInteger & flip scientific 0

dateToStrGMT :: Scientific -> T.Text -> T.Text
dateToStrGMT date fmt =
  let sysTime = MkSystemTime (case toBoundedInteger date of
        Just x -> x
        _ -> throwSQLException CodegenException Nothing "Impossible happened...") 0
  in formatSystemTimeGMT (case timeFmt fmt of
  Just x -> x
  _ -> throwSQLException CodegenException Nothing
    ("Operation OpDateStr on time format " <> show fmt <> " is not supported")) sysTime
      & cBytesToText

timeFmt :: T.Text -> Maybe ZCB.CBytes
timeFmt fmt
  | textToCBytes fmt == simpleDateFormat  = Just simpleDateFormat
  | textToCBytes fmt == iso8061DateFormat = Just iso8061DateFormat
  | textToCBytes fmt == webDateFormat     = Just webDateFormat
  | textToCBytes fmt == mailDateFormat    = Just mailDateFormat
  | otherwise                             = Nothing

genRandomSinkStream :: IO Text
genRandomSinkStream = stringRandomIO "[a-zA-Z]{20}"

--------------------------------------------------------------------------------
destructSingletonFlowObject :: FlowObject -> Either DiffFlowError (SKey,FlowValue)
destructSingletonFlowObject o =
  case HM.toList o of
    [(k,v)] -> Right (k,v)
    _       -> Left . RunShardError $ "Error on object " <> T.pack (show o) <> ": not a singleton FlowObject type. If you believe you are right, please report this as a bug."
