{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.Utils where

import           Data.ByteString       (ByteString)
import           Data.List             (foldl')
import           Data.Maybe            (fromMaybe)
import           Data.Scientific
import           Data.Text             (Text)
import qualified Data.Text             as T
import qualified Data.Text.Encoding    as Text
import           Foreign.C.Types       (CTime (CTime))
import           GHC.Stack             (HasCallStack)
import           Text.StringRandom     (stringRandomIO)

import           HStream.Base.Time     (UnixTime (..), formatUnixTimeGMT,
                                        iso8061DateFormat, mailDateFormat,
                                        parseUnixTimeGMT, simpleDateFormat,
                                        webDateFormat)
import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeSQLException (..),
                                        throwSQLException)
import           HStream.SQL.Rts

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

strToDateGMT :: HasCallStack => T.Text -> T.Text -> Int
strToDateGMT date fmt =
  let fmt' = checkTimeFmt "OpStrDate" fmt
      CTime sec = utSeconds $ parseUnixTimeGMT fmt' $ Text.encodeUtf8 date
   in fromIntegral sec

dateToStrGMT :: Int -> T.Text -> T.Text
dateToStrGMT sec fmt =
  let time = UnixTime (CTime $ fromIntegral sec) 0
      fmt' = checkTimeFmt "OpDateStr" fmt
   in Text.decodeUtf8 $ formatUnixTimeGMT fmt' time

checkTimeFmt :: HasCallStack => String -> T.Text -> ByteString
checkTimeFmt label fmt
  | Text.encodeUtf8 fmt == simpleDateFormat  = simpleDateFormat
  | Text.encodeUtf8 fmt == iso8061DateFormat = iso8061DateFormat
  | Text.encodeUtf8 fmt == webDateFormat     = webDateFormat
  | Text.encodeUtf8 fmt == mailDateFormat    = mailDateFormat
  | otherwise = throwSQLException CodegenException Nothing
      ("Operation " <> label <> " on time format " <> show fmt <> " is not supported")

genRandomSinkStream :: IO Text
genRandomSinkStream = stringRandomIO "[a-zA-Z]{20}"
