{-# LANGUAGE BangPatterns       #-}
{-# LANGUAGE NumericUnderscores #-}

module HStream.Base.Time
  ( UT.UnixTime (..)
  , UT.Format
  , simpleDateFormat
  , iso8061DateFormat
  , UT.webDateFormat
  , UT.mailDateFormat
  , UT.formatUnixTime
  , UT.formatUnixTimeGMT
  , UT.parseUnixTime
  , UT.parseUnixTimeGMT
  , formatSystemTime
  , formatSystemTimeGMT
  , parseSystemTime
  , parseSystemTimeGMT
  , getSystemMsTimestamp
  , getSystemNsTimestamp
    -- Re-export
  , CTime (CTime)
  , module Data.Time.Clock.System
  ) where

import           Data.ByteString        (ByteString)
import           Data.Int
import           Data.Time.Clock.System
import qualified Data.UnixTime          as UT
import           Foreign.C.Types        (CTime (CTime))


-- | Simple format @2020-10-16 03:15:29@.
--
-- The value is \"%Y-%m-%d %H:%M:%S\".
-- This should be used with 'formatSystemTime' and 'parseSystemTime'.
simpleDateFormat :: UT.Format
simpleDateFormat = "%Y-%m-%d %H:%M:%S"
{-# INLINABLE simpleDateFormat #-}

-- | Simple format @2020-10-16T03:15:29@.
--
-- The value is \"%Y-%m-%dT%H:%M:%S%z\".
-- This should be used with 'formatSystemTime' and 'parseSystemTime'.
iso8061DateFormat :: UT.Format
iso8061DateFormat = "%Y-%m-%dT%H:%M:%S%z"
{-# INLINABLE iso8061DateFormat #-}

-- | Formatting 'SystemTime' to 'ByteString' in local time.
--
-- This is a wrapper for strftime_l(), 'systemNanoseconds' is ignored.
-- The result depends on the TZ environment variable.
formatSystemTime :: UT.Format -> SystemTime -> IO ByteString
formatSystemTime fmt = UT.formatUnixTime fmt . systime2unix
{-# INLINABLE formatSystemTime #-}

-- | Formatting 'SystemTime' to 'ByteString' in GMT.
--
-- This is a wrapper for strftime_l(), 'systemNanoseconds' is ignored.
--
-- >>> formatSystemTimeGMT webDateFormat $ MkSystemTime 0 0
-- "Thu, 01 Jan 1970 00:00:00 GMT"
-- >>> let ut = MkSystemTime 100 200
-- >>> let str = formatSystemTimeGMT "%s" ut
-- >>> let ut' = parseSystemTimeGMT "%s" str
-- >>> ((==) `on` systemSeconds) ut ut'
-- True
-- >>> ((==) `on` systemNanoseconds) ut ut'
-- False
formatSystemTimeGMT :: UT.Format -> SystemTime -> ByteString
formatSystemTimeGMT fmt = UT.formatUnixTimeGMT fmt . systime2unix
{-# INLINABLE formatSystemTimeGMT #-}

-- | Parsing 'ByteString' to 'SystemTime' interpreting as localtime.
--
-- This is a wrapper for strptime_l().
-- Many implementations of strptime_l() do not support %Z and
-- some implementations of strptime_l() do not support %z, either.
--
-- 'systemNanoSeconds' is always set to 0.
--
-- The result depends on the TZ environment variable.
parseSystemTime :: UT.Format -> ByteString -> SystemTime
parseSystemTime fmt str = unixtime2sys $ UT.parseUnixTime fmt str
{-# INLINABLE parseSystemTime #-}

-- | Parsing 'ByteString' to 'SystemTime' interpreting as GMT.
--
-- This is a wrapper for strptime_l().
-- 'systemNanoSeconds' is always set to 0.
--
-- >>> parseSystemTimeGMT webDateFormat "Thu, 01 Jan 1970 00:00:00 GMT"
-- MkSystemTime {systemSeconds = 0, systemNanoseconds = 0}
parseSystemTimeGMT :: UT.Format -> ByteString -> SystemTime
parseSystemTimeGMT fmt str = unixtime2sys $ UT.parseUnixTimeGMT fmt str
{-# INLINABLE parseSystemTimeGMT #-}

-------------------------------------------------------------------------------

getSystemMsTimestamp :: IO Int64
getSystemMsTimestamp = do
  MkSystemTime sec nano <- getSystemTime
  let !ts = floor @Double $ (fromIntegral sec * 1e3) + (fromIntegral nano / 1e6)
  return ts

getSystemNsTimestamp :: IO Int64
getSystemNsTimestamp = do
  MkSystemTime sec nano <- getSystemTime
  return $ sec * 1_000_000_000 + (fromIntegral nano)

-------------------------------------------------------------------------------

systime2unix :: SystemTime -> UT.UnixTime
systime2unix (MkSystemTime sec _nano) = UT.UnixTime (CTime sec) 0
{-# INLINABLE systime2unix #-}

unixtime2sys :: UT.UnixTime -> SystemTime
unixtime2sys (UT.UnixTime (CTime sec) _micro) = MkSystemTime sec 0
{-# INLINABLE unixtime2sys #-}
