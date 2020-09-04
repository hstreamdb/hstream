module HStream.LogStore.Utils where

import           ByteString.StrictBuilder   (builderBytes, word64BE)
import           Control.Exception          (throw)
import           Data.Binary.Strict.Get     (getWord64be, runGet)
import qualified Data.ByteString            as B
import qualified Data.Sequence              as Seq
import qualified Data.Text                  as T
import           Data.Text.Encoding
import           Data.Time                  (nominalDiffTimeToSeconds)
import           Data.Time.Clock.POSIX      (POSIXTime, getPOSIXTime)
import           Data.Word                  (Word64)

import           HStream.LogStore.Exception

encodeWord64 :: Word64 -> B.ByteString
encodeWord64 = builderBytes . word64BE

decodeWord64 :: B.ByteString -> Word64
decodeWord64 bs =
  if rem' /= B.empty
    then throw $ LogStoreDecodeException "input error"
    else case res of
      Left s  -> throw $ LogStoreDecodeException s
      Right v -> v
  where
    (res, rem') = runGet getWord64be $ bs

decodeText :: B.ByteString -> T.Text
decodeText = decodeUtf8

encodeText :: T.Text -> B.ByteString
encodeText = encodeUtf8

posixTimeToMilliSeconds :: POSIXTime -> Word64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Word64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime

lastElemInSeq :: Seq.Seq a -> a
lastElemInSeq xs =
  case xs of
    Seq.Empty   -> error "empty sequence"
    _ Seq.:|> x -> x
