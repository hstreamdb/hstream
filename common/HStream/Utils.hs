module HStream.Utils
  ( module HStream.Utils.Converter
  , module HStream.Utils.Format
  , module HStream.Utils.BuildRecord
  , module HStream.Utils.RPC
  , module HStream.Utils.Concurrent
  , module HStream.Utils.TimeInterval
  , module HStream.Utils.Table
  , module HStream.Utils.Common
  , module HStream.Utils.JSON

  , genUnique
  , genUniqueId
  ) where

import           Control.Monad              (unless)
import           Data.Bits                  (shiftL, shiftR, (.&.), (.|.))
import           Data.Int                   (Int64)
import           Data.String                (IsString (fromString))
import           Data.Word                  (Word16, Word32, Word64)
import           Numeric                    (showHex)
import           System.Random              (randomRIO)
import           Z.IO.Time                  (SystemTime (..), getSystemTime')

import           HStream.Utils.BuildRecord
import           HStream.Utils.Common
import           HStream.Utils.Concurrent
import           HStream.Utils.Converter
import           HStream.Utils.Format
import           HStream.Utils.JSON
import           HStream.Utils.RPC
import           HStream.Utils.Table
import           HStream.Utils.TimeInterval

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

genUniqueId :: IsString a => IO a
genUniqueId = do
  unique <- genUnique
  return $ fromString (showHex unique "")
