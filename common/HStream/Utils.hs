module HStream.Utils
  ( module HStream.Utils.Converter
  , module HStream.Utils.Format
  , module HStream.Utils.BuildRecord
  , module HStream.Utils.RPC
  , module HStream.Utils.Concurrent
  , module HStream.Utils.TimeInterval

  , getKeyWordFromException
  , flattenJSON
  , genUnique
  , setupSigsegvHandler
  ) where

import           Control.Exception          (Exception (..))
import           Control.Monad              (join, unless)
import           Data.Aeson                 as Aeson
import           Data.Bifunctor             (first)
import           Data.Bits                  (shiftL, shiftR, (.&.), (.|.))
import qualified Data.HashMap.Strict        as HM
import           Data.Int                   (Int64)
import           Data.Text                  (Text)
import qualified Data.Text                  as Text
import qualified Data.Text.Lazy             as TL
import           Data.Word                  (Word16, Word32, Word64)
import           System.Random              (randomRIO)
import           Z.IO.Time                  (SystemTime (..), getSystemTime')

import           HStream.Utils.BuildRecord
import           HStream.Utils.Concurrent
import           HStream.Utils.Converter
import           HStream.Utils.Format
import           HStream.Utils.RPC
import           HStream.Utils.TimeInterval

getKeyWordFromException :: Exception a => a -> TL.Text
getKeyWordFromException =  TL.pack . takeWhile (/='{') . show

-- | Flatten all JSON structures.
--
-- >>> flatten (HM.fromList [("a", Aeson.Object $ HM.fromList [("b", Aeson.Number 1)])])
-- fromList [("a.b",Number 1.0)]
flattenJSON :: HM.HashMap Text Aeson.Value -> HM.HashMap Text Aeson.Value
flattenJSON jsonMap =
  let flattened = join $ map (flattenJSON' "." Text.empty) (HM.toList jsonMap)
   in HM.fromList $ map (first Text.tail) flattened

flattenJSON' :: Text -> Text -> (Text, Aeson.Value) -> [(Text, Aeson.Value)]
flattenJSON' splitor prefix (k, v) = do
  -- TODO: we will not support array?
  case v of
    Aeson.Object o -> join $ map (flattenJSON' splitor (prefix <> splitor <> k)) (HM.toList o)
    _              -> [(prefix <> splitor <> k, v)]

-- | Generate a "unique" number through a modified version of snowflake algorithm.
--
-- idx: 63...56 55...0
--      |    |  |    |
-- bit: 0....0  xx....
genUnique :: IO Word64
genUnique = do
  let startTS = 1577808000  -- 2020-01-01
  ts <- getSystemTime'
  let sec = systemSeconds ts - startTS
  unless (sec > 0) $ error "Impossible happened, make sure your system time is synchronized."
  -- 32bit
  let tsBit :: Int64 = fromIntegral (maxBound :: Word32) .&. sec
  -- 8bit
  let tsBit' :: Word32 = shiftR (systemNanoseconds ts) 24
  -- 16bit
  rdmBit :: Word16 <- randomRIO (0, maxBound :: Word16)
  return $ fromIntegral (shiftL tsBit 24)
       .|. fromIntegral (shiftL tsBit' 16)
       .|. fromIntegral rdmBit
{-# INLINE genUnique #-}

foreign import ccall unsafe "hs_common.h setup_sigsegv_handler"
  setupSigsegvHandler :: IO ()
