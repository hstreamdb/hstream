{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.Logger
  ( trace
  , Log.debug
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

  , t, d, i, w, e

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
  , pattern TRACE
  , pattern Log.NOTSET
  ) where

import           Data.ByteString         (ByteString)
import qualified Data.ByteString.Char8   as BSC
import qualified Data.Text               as Text
import qualified Data.Text.Lazy          as TL
import           Foreign.C.Types         (CInt (..))
import           GHC.Conc.Sync           (ThreadId (..))
import           GHC.Exts                (ThreadId#)
import           GHC.Stack               (HasCallStack)
import qualified Text.Read               as Read
import qualified Z.Data.Builder          as B
import           Z.Data.Builder          (Builder)
import qualified Z.Data.CBytes           as CBytes
import qualified Z.IO.Logger             as Log
import           Z.IO.StdStream.Ansi     (AnsiColor (..), color)

import qualified HStream.Utils.Converter as U

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

-- Logging Levels
--
-- +----------+---------------+
-- | Level    | Numeric value |
-- +----------+---------------+
-- | CRITICAL | 50            |
-- +----------+---------------+
-- | FATAL    | 40            |
-- +----------+---------------+
-- | WARNING  | 30            |
-- +----------+---------------+
-- | INFO     | 20            |
-- +----------+---------------+
-- | DEBUG    | 10            |
-- +----------+---------------+
-- | TRACE    | 5             |
-- +----------+---------------+
-- | NOTSET   | 0             |
-- +----------+---------------+

pattern TRACE :: Log.Level
pattern TRACE = 5

trace :: HasCallStack => Builder () -> IO ()
trace = Log.otherLevel TRACE False{- flush immediately ? -}
{-# INLINE trace #-}

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
                                then defaultColoredFmt
                                else defaultFmt
        }
  Log.setDefaultLogger =<< Log.newStdLogger config

-- | A default log formatter
--
-- @[FATAL][2021-02-01T15:03:30+0800][<interactive>:31:1][thread#669]...@
defaultFmt :: Log.LogFormatter
defaultFmt ts level content cstack (ThreadId tid#) = do
    B.square (defaultLevelFmt level)
    B.square ts
    B.square $ Log.defaultFmtCallStack cstack
    B.square $ "thread#" >> B.int (getThreadId tid#)
    content
    B.char8 '\n'

-- | A default colored log formatter
--
-- DEBUG level is 'Cyan', WARNING level is 'Yellow', FATAL and CRITICAL level are 'Red'.
defaultColoredFmt :: Log.LogFormatter
defaultColoredFmt ts level content cstack (ThreadId tid#) = do
  let blevel = defaultLevelFmt level
  B.square (case level of
    Log.DEBUG    -> color Cyan blevel
    Log.INFO     -> color Magenta blevel
    Log.WARNING  -> color Yellow blevel
    Log.FATAL    -> color Red blevel
    Log.CRITICAL -> color Red blevel
    _            -> blevel)
  B.square ts
  B.square $ Log.defaultFmtCallStack cstack
  B.square $ "thread#" >> B.int (getThreadId tid#)
  content
  B.char8 '\n'

-- | Format 'DEBUG' to 'CRITICAL', etc.
--
-- Level other than built-in ones, are formatted in decimal numeric format, i.e.
-- @defaultLevelFmt 60 == "LEVEL60"@
defaultLevelFmt :: Log.Level -> B.Builder ()
{-# INLINE defaultLevelFmt #-}
defaultLevelFmt level = case level of
  Log.CRITICAL -> "CRITICAL"
  Log.FATAL    -> "FATAL"
  Log.WARNING  -> "WARNING"
  Log.INFO     -> "INFO"
  Log.DEBUG    -> "DEBUG"
  TRACE        -> "TRACE"
  Log.NOTSET   -> "NOTSET"
  level'       -> "LEVEL" >> B.int level'

newtype Level = Level {unLevel :: Log.Level} deriving (Eq)

instance Show Level where
  show (Level Log.CRITICAL) = "critical"
  show (Level Log.FATAL)    = "fatal"
  show (Level Log.WARNING)  = "warning"
  show (Level Log.INFO)     = "info"
  show (Level Log.DEBUG)    = "debug"
  show (Level TRACE)        = "trace"
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
        Read.Ident "trace"    -> TRACE
        x -> errorWithoutStackTrace $ "cannot parse log level" <> show x

t :: HasCallStack => Builder () -> IO ()
t = Log.withDefaultLogger . trace

d :: HasCallStack => Builder () -> IO ()
d = Log.withDefaultLogger . Log.debug

i :: HasCallStack => Builder () -> IO ()
i = Log.withDefaultLogger . Log.info

w :: HasCallStack => Builder () -> IO ()
w = Log.withDefaultLogger . Log.warning

e :: HasCallStack => Builder () -> IO ()
e = Log.withDefaultLogger . Log.fatal

foreign import ccall unsafe "rts_getThreadId" getThreadId :: ThreadId# -> CInt
