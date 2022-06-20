{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE TypeSynonymInstances #-}

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
  , buildString'
  , buildText
  , buildLazyText
  , buildByteString
  , buildCBytes

  -- * Log Level
  , setLogLevel
  , Level (..)
  , pattern Log.CRITICAL
  , pattern Log.FATAL
  , pattern Log.WARNING
  , pattern Log.INFO
  , pattern Log.DEBUG
  , pattern Log.NOTSET
  ) where

import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Text             as Text
import qualified Data.Text.Lazy        as TL
import           GHC.Stack             (HasCallStack)
import qualified Text.Read             as Read
import           Z.Data.Builder        (Builder)
import qualified Z.Data.Builder        as B
import qualified Z.Data.CBytes         as CBytes
import qualified Z.IO.Logger           as Log

import qualified HStream.Utils         as U

-------------------------------------------------------------------------------
-- Example:
--
-- @
-- import qualified HStream.Logger as Log
--
-- main :: IO ()
-- main = Log.withDefaultLogger $ do
--   Log.debug $ "..."
--   Log.info $ Log.buildString someString
-- @
--
-------------------------------------------------------------------------------

buildInt :: (Integral a, Bounded a) => a -> Builder ()
buildInt = B.int
{-# INLINE buildInt #-}

buildString :: String -> Builder ()
buildString = B.stringUTF8
{-# INLINE buildString #-}

buildString' :: Show a => a -> Builder ()
buildString' = B.stringUTF8 . show
{-# INLINE buildString' #-}

buildText :: Text.Text -> Builder ()
buildText = U.textToZBuilder
{-# INLINE buildText #-}

buildLazyText :: TL.Text -> Builder ()
buildLazyText = U.lazyTextToZBuilder
{-# INLINE buildLazyText #-}

buildByteString :: ByteString -> Builder ()
buildByteString = B.stringUTF8 . BSC.unpack
{-# INLINE buildByteString #-}

buildCBytes :: CBytes.CBytes -> Builder ()
buildCBytes = CBytes.toBuilder
{-# INLINE buildCBytes #-}

setLogLevel :: Level -> Bool -> IO ()
setLogLevel level withColor = do
  let config = Log.defaultLoggerConfig
        { Log.loggerLevel = unLevel level
        , Log.loggerFormatter = if withColor
                                then Log.defaultColoredFmt
                                else Log.defaultFmt
        }
  Log.setDefaultLogger =<< Log.newStdLogger config

newtype Level = Level {unLevel :: Log.Level}

instance Show Level where
  show (Level Log.CRITICAL) = "critical"
  show (Level Log.FATAL)    = "fatal"
  show (Level Log.WARNING)  = "warning"
  show (Level Log.INFO)     = "info"
  show (Level Log.DEBUG)    = "debug"
  show (Level Log.NOTSET)   = "notset"
  show _                    = "unknown log level"

instance Read Level where
  readPrec = do
    l <- Read.lexP
    return . Level $
      case l of
        Read.Ident "critical" -> Log.CRITICAL
        Read.Ident "fatal"    -> Log.FATAL
        Read.Ident "warning"  -> Log.WARNING
        Read.Ident "info"     -> Log.INFO
        Read.Ident "debug"    -> Log.DEBUG
        x -> errorWithoutStackTrace $ "cannot parse log level" <> show x

d :: HasCallStack => Builder () -> IO ()
d = Log.withDefaultLogger . Log.debug

i :: HasCallStack => Builder () -> IO ()
i = Log.withDefaultLogger . Log.info

w :: HasCallStack => Builder () -> IO ()
w = Log.withDefaultLogger . Log.warning

e :: HasCallStack => Builder () -> IO ()
e = Log.withDefaultLogger . Log.fatal
