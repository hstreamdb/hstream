{-# LANGUAGE PatternSynonyms #-}

module HStream.Logger
  ( Log.debug
  , Log.info
  , Log.warning
  , Log.fatal

  , Log.LoggerConfig (..)
  , Log.defaultLoggerConfig
  , Log.withDefaultLogger
  , Log.flushDefaultLogger
  , Log.setDefaultLogger
  , Log.getDefaultLogger
  , Log.Logger

  , d, i, w, e

  -- * Builder
  , buildInt
  , buildString
  , buildText
  , buildLazyText

  -- * Log Level
  , setLogLevel
  , Log.Level
  , pattern Log.CRITICAL
  , pattern Log.FATAL
  , pattern Log.WARNING
  , pattern Log.INFO
  , pattern Log.DEBUG
  , pattern Log.NOTSET
  ) where

import qualified Data.Text      as Text
import qualified Data.Text.Lazy as TL
import           GHC.Stack      (HasCallStack)
import           Z.Data.Builder (Builder)
import qualified Z.Data.Builder as B
import qualified Z.IO.Logger    as Log

import qualified HStream.Utils  as U

d :: HasCallStack => Builder () -> IO ()
d = Log.withDefaultLogger . Log.debug

i :: HasCallStack => Builder () -> IO ()
i = Log.withDefaultLogger . Log.info

w :: HasCallStack => Builder () -> IO ()
w = Log.withDefaultLogger . Log.warning

e :: HasCallStack => Builder () -> IO ()
e = Log.withDefaultLogger . Log.fatal

-------------------------------------------------------------------------------

buildInt :: (Integral a, Bounded a) => a -> Builder ()
buildInt = B.int
{-# INLINE buildInt #-}

buildString :: String -> Builder ()
buildString = B.stringUTF8
{-# INLINE buildString #-}

buildText :: Text.Text -> Builder ()
buildText = U.textToZBuilder
{-# INLINE buildText #-}

buildLazyText :: TL.Text -> Builder ()
buildLazyText = U.lazyTextToZBuilder
{-# INLINE buildLazyText #-}

setLogLevel :: Log.Level -> IO ()
setLogLevel level = do
  let config = Log.defaultLoggerConfig
        { Log.loggerLevel = level
        , Log.loggerFormatter = Log.defaultColoredFmt
        }
  Log.setDefaultLogger =<< Log.newStdLogger config
