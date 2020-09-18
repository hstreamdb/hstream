{-# LANGUAGE LambdaCase #-}

module HStream.Utils
  ( bs2str
  , str2bs
  , textShow
  , encodeText
  , decodeText
  , encodeTextShow

  , encodeWord64BE
  , decodeWord64BE
  , decodeWord64EitherBE

  , getCurrentTimestamp

  , lastElemInSeq
  , fromLeft'
  , fromRight'
  , (.|.)
  ) where

import           ByteString.StrictBuilder (builderBytes, word64BE)
import           Data.Binary.Strict.Get   (getWord64be, runGet)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import           Data.Either              (fromLeft, fromRight)
import qualified Data.Sequence            as Seq
import           Data.Text                (Text)
import qualified Data.Text                as Text
import qualified Data.Text.Encoding       as Text
import           Data.Time                (nominalDiffTimeToSeconds)
import           Data.Time.Clock.POSIX    (POSIXTime, getPOSIXTime)
import           Data.Word                (Word64)

bs2str :: ByteString -> String
bs2str = Text.unpack . Text.decodeUtf8

str2bs :: String -> ByteString
str2bs = Text.encodeUtf8 . Text.pack

textShow :: Show a => a -> Text
textShow = Text.pack . show

decodeText :: ByteString -> Text
decodeText = Text.decodeUtf8

encodeText :: Text -> ByteString
encodeText = Text.encodeUtf8

encodeTextShow :: Show a => a -> ByteString
encodeTextShow = Text.encodeUtf8 . textShow

encodeWord64BE :: Word64 -> ByteString
encodeWord64BE = builderBytes . word64BE

decodeWord64BE :: ByteString -> Word64
decodeWord64BE bs =
  case decodeWord64EitherBE bs of
    Left emsg -> error emsg
    Right val -> val

decodeWord64EitherBE :: ByteString -> Either String Word64
decodeWord64EitherBE bs =
  let (val, rest) = runGet getWord64be $ bs
   in if BS.null rest then val else Left ("decode Word64 failed:" <> show bs)

posixTimeToMilliSeconds :: POSIXTime -> Word64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- | Return millisecond timestamp
getCurrentTimestamp :: IO Word64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime

lastElemInSeq :: Seq.Seq a -> a
lastElemInSeq = \case
  Seq.Empty   -> error "empty sequence"
  _ Seq.:|> x -> x

-- Note: @(.|.) = liftA2 (<|>)@ can get the same result, but it will
-- perform all @m (Maybe a)@ and then return the first "Just value".
(.|.) :: Monad m => m (Maybe a) -> m (Maybe a) -> m (Maybe a)
ma .|. mb = maybe mb (return . Just) =<< ma

fromLeft' :: Either a b -> a
fromLeft' = fromLeft (error "this should never happen")

fromRight' :: Either a b -> b
fromRight' = fromRight (error "this should never happen")
