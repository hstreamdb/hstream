module HStream.Base
  ( throwIOError
  , genUnique
  , withoutPrefix
  , approxNaturalTime
  , rmTrailingZeros
  , setupFatalSignalHandler

  , module HStream.Base.Concurrent
  ) where

import           Control.Monad           (unless)
import           Data.Bits               (shiftL, shiftR, (.&.), (.|.))
import           Data.Int                (Int64)
import           Data.List               (stripPrefix)
import           Data.Maybe              (fromMaybe)
import           Data.Time.Clock         (NominalDiffTime)
import           Data.Time.Clock.System  (SystemTime (..), getSystemTime)
import           Data.Word               (Word16, Word32, Word64)
import           System.Random           (randomRIO)
import           Text.Printf             (printf)

import           HStream.Base.Concurrent

-- | Generate a "unique" number through a modified version of snowflake algorithm.
--
-- idx: 63...56 55...0
--      |    |  |    |
-- bit: 0....0  xx....
genUnique :: IO Word64
genUnique = do
  let startTS = 1577808000  -- 2020-01-01
  ts <- getSystemTime
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

throwIOError :: String -> IO a
throwIOError = ioError . userError

withoutPrefix :: Eq a => [a] -> [a] -> [a]
withoutPrefix prefix ele = fromMaybe ele $ stripPrefix prefix ele

approxNaturalTime :: NominalDiffTime -> String
approxNaturalTime n
  | n < 0 = ""
  | n == 0 = "0 second"
  | n < 1 = show @Int (floor $ n * 1000) ++ " milliseconds"
  | n < 60 = show @Int (floor n) ++ " seconds"
  | n < fromIntegral hour = show @Int (floor n `div` 60) ++ " minutes"
  | n < fromIntegral day  = show @Int (floor n `div` hour) ++ " hours"
  | n < fromIntegral year = show @Int (floor n `div` day) ++ " days"
  | otherwise = show @Int (floor n `div` year) ++ " years"
  where
    hour = 60 * 60
    day = hour * 24
    year = day * 365

-- > rmTrailingZeros "1.0"
-- "1"
-- > rmTrailingZeros "0.10"
-- "0.1"
rmTrailingZeros :: Double -> String
rmTrailingZeros x
  | fromIntegral x' == x = show x'
  | otherwise            = printf "%g" x
  where
    x' = floor x :: Int

foreign import ccall unsafe "hs_common.h setup_fatal_signal_handler"
  setupFatalSignalHandler :: IO ()
