{- Mostly Copy from https://hackage.haskell.org/package/fast-logger and modified.
   The original license is BSD-3-Clause.
-}
{-# LANGUAGE BangPatterns         #-}
{-# LANGUAGE CPP                  #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE TypeSynonymInstances #-}

{-
This is needed to make ghci work.

Relatead ghc issues:
* https://gitlab.haskell.org/ghc/ghc/-/issues/19733
* https://gitlab.haskell.org/ghc/ghc/-/issues/15454
-}
{-# OPTIONS_GHC -fobject-code #-}

module HStream.Logger
  ( -- * Logger
    Logger
  , setDefaultLogger
  , setDefaultLoggerLevel
  , withDefaultLogger

  -- * Log function
  , trace
  , debug
  , debug1
  , info
  , warning
  , fatal
  , warnSlow
  , i

  -- * Builder
  , build
  , buildString
  , buildString'

    -- * Log Config
  , LoggerConfig (..)
  , defaultLoggerConfig
    -- ** LogType
  , LogType
  , LogType' (..)
  , Log.FileLogSpec (..)
    -- ** Formatter
  , LogFormatter
    -- ** Log Level
  , Level (..)
  , pattern CRITICAL
  , pattern FATAL
  , pattern WARNING
  , pattern INFO
  , pattern DEBUG
  , pattern TRACE
  , pattern NOTSET
  ) where

import           Control.Concurrent             (threadDelay)
import qualified Control.Concurrent.Async       as Async
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Data.IORef
import           Foreign.C.Types
import           GHC.Conc.Sync                  (ThreadId (..), myThreadId)
import           GHC.Exts                       (ThreadId#)
import           GHC.Stack
import           System.EasyFile                (getFileSize)
import           System.IO.Unsafe               (unsafePerformIO)
import qualified System.Log.FastLogger          as Log
import qualified System.Log.FastLogger.Internal as Log
import           System.Posix.IO                (stdError)
import           System.Posix.Terminal          (queryTerminal)
import qualified Text.Read                      as Read

import           HStream.Base.Ansi              (AnsiColor (..), color)

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

-- | Logging Levels
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
newtype Level = Level {unLevel :: Int}
  deriving (Eq, Ord)

pattern CRITICAL :: Level
pattern CRITICAL = Level 50

pattern FATAL :: Level
pattern FATAL = Level 40

pattern WARNING :: Level
pattern WARNING = Level 30

pattern INFO :: Level
pattern INFO = Level 20

pattern DEBUG1 :: Level
pattern DEBUG1 = Level 11

pattern DEBUG :: Level
pattern DEBUG = Level 10

pattern TRACE :: Level
pattern TRACE = Level 5

pattern NOTSET :: Level
pattern NOTSET = Level 0

instance Show Level where
  show CRITICAL = "critical"
  show FATAL    = "fatal"
  show WARNING  = "warning"
  show INFO     = "info"
  show DEBUG1   = "debug1"
  show DEBUG    = "debug"
  show TRACE    = "trace"
  show NOTSET   = "notset"
  show _        = "unknown log level"

instance Read Level where
  readPrec = do
    l <- Read.lexP
    return $
      case l of
        Read.Ident "critical" -> CRITICAL
        Read.Ident "fatal"    -> FATAL
        Read.Ident "warning"  -> WARNING
        Read.Ident "info"     -> INFO
        Read.Ident "debug1"   -> DEBUG1
        Read.Ident "debug"    -> DEBUG
        Read.Ident "trace"    -> TRACE
        x -> errorWithoutStackTrace $ "cannot parse log level" <> show x

-------------------------------------------------------------------------------

build :: Log.ToLogStr a => a -> Log.LogStr
build = Log.toLogStr

buildString :: String -> Log.LogStr
buildString = Log.toLogStr
{-# INLINE buildString #-}

buildString' :: Show a => a -> Log.LogStr
buildString' = Log.toLogStr . show
{-# INLINE buildString' #-}

-------------------------------------------------------------------------------

-- | Log Formatter.
type LogFormatter
   = Log.LogStr           -- ^ data\/time string(second precision)
  -> Level                -- ^ log level
  -> Log.LogStr           -- ^ log content
  -> CallStack            -- ^ call stack trace
  -> ThreadId             -- ^ logging thread id
  -> Log.LogStr

-- | A default log formatter
--
-- @[FATAL][2021-02-01T15:03:30+0800][<interactive>:31:1][thread#669]...@
defaultFmt :: LogFormatter
defaultFmt time level content cstack (ThreadId tid#) =
    square (defaultFmtLevel level)
 <> square time
 <> square (defaultFmtCallStack cstack)
 <> square ("thread#" <> Log.toLogStr @Int (fromIntegral $ getThreadId tid#))
 <> content
 <> "\n"

-- | A default colored log formatter
--
-- DEBUG level is 'Cyan'
-- WARNING level is 'Yellow'
-- FATAL and CRITICAL level are 'Red'
defaultColoredFmt :: LogFormatter
defaultColoredFmt time level content cstack (ThreadId tid#) =
  let Log.LogStr _ b = defaultFmtLevel level
      coloredLevel =
        case level of
          -- TODO: color for trace
          DEBUG    -> color Cyan b
          DEBUG1   -> color Cyan b
          INFO     -> color Magenta b
          WARNING  -> color Yellow b
          FATAL    -> color Red b
          CRITICAL -> color Red b
          _        -> b
   in square (Log.toLogStr coloredLevel)
   <> square time
   <> square (defaultFmtCallStack cstack)
   <> square ("thread#" <> Log.toLogStr @Int (fromIntegral $ getThreadId tid#))
   <> content
   <> "\n"

-- | Format log levels
--
-- Level other than built-in ones, are formatted in decimal numeric format.
defaultFmtLevel :: Level -> Log.LogStr
defaultFmtLevel level = case level of
  CRITICAL -> "CRITICAL"
  FATAL    -> "FATAL"
  WARNING  -> "WARNING"
  INFO     -> "INFO"
  DEBUG    -> "DEBUG"
  DEBUG1   -> "DEBUG1"
  TRACE    -> "TRACE"
  NOTSET   -> "NOTSET"
  level'   -> "LEVEL" <> Log.toLogStr (unLevel level')
{-# INLINE defaultFmtLevel #-}

-- | Default stack formatter which fetch the logging source and location.
defaultFmtCallStack :: CallStack -> Log.LogStr
defaultFmtCallStack cs =
 case reverse $ getCallStack cs of
   [] -> "<no call stack found>"
   (_, loc):_ ->
      Log.toLogStr (srcLocFile loc)
      <> ":"
      <> Log.toLogStr (srcLocStartLine loc)
      <> ":"
      <> Log.toLogStr (srcLocStartCol loc)
{-# INLINABLE defaultFmtCallStack #-}

square :: Log.LogStr -> Log.LogStr
square s = "[" <> s <> "]"
{-# INLINE square #-}

-------------------------------------------------------------------------------

type LogType = LogType' Log.LogStr

-- Variant of Log.LogType'
--
-- | Logger Type.
data LogType' a where
  LogNone :: LogType' Log.LogStr
  LogStdout :: LogType' Log.LogStr
  LogStderr :: LogType' Log.LogStr
  LogFile :: FilePath -> LogType' Log.LogStr
  LogFileRotate :: Log.FileLogSpec -> LogType' Log.LogStr

-- | Logger config type used in this module.
data LoggerConfig = LoggerConfig
  { loggerBufSize          :: {-# UNPACK #-} !Int
    -- ^ Buffer size of each core
  , loggerLevel            :: {-# UNPACK #-} !Level
    -- ^ Config log's filter level
  , loggerFormatter        :: !LogFormatter
    -- ^ Log formatter
  , loggerType             :: !LogType
  , loggerFlushImmediately :: {-# UNPACK #-} !Bool
    -- ^ Flush immediately after logging
  }

defaultLoggerConfig :: LoggerConfig
defaultLoggerConfig = LoggerConfig 4096 NOTSET defaultFmt LogStderr False
{-# INLINABLE defaultLoggerConfig #-}

-- FIXME: 'Log.newTimeCache' updates every 1 second, so it doesn't provide
-- microsecond precision
defaultTimeCache :: IO Log.FormattedTime
defaultTimeCache = unsafePerformIO $ Log.newTimeCache iso8061DateFormat
  where
    iso8061DateFormat = "%Y-%m-%dT%H:%M:%S%z"
{-# NOINLINE defaultTimeCache #-}

-------------------------------------------------------------------------------

data Logger = Logger
  (Level -> Bool -> CallStack -> Log.LogStr -> IO ())   -- ^ logging function
  (IO ()) -- ^ clean up action
  (IO ()) -- ^ manually flush function

-- 'Log.newTimedFastLogger' doesn't export the flush function
newLogger :: LoggerConfig -> IO Logger
newLogger !LoggerConfig{..} =
  case loggerType of
    LogNone    -> return $ Logger (\_ _ _ _ -> pure ()) (pure ()) (pure ())
    LogStdout  -> Log.newStdoutLoggerSet loggerBufSize >>= loggerInit
    LogStderr  -> Log.newStderrLoggerSet loggerBufSize >>= loggerInit
    LogFile fp -> Log.newFileLoggerSet loggerBufSize fp >>= loggerInit
    LogFileRotate fspec -> rotateLoggerInit fspec loggerBufSize
  where
    loggerInit lgrset = return $ Logger
      (\level shouldFlush cstack s ->
        when (level >= loggerLevel) $ do
          tid <- myThreadId
          time <- defaultTimeCache
          Log.pushLogStr lgrset $
            loggerFormatter (Log.toLogStr time) level s cstack tid
          when (loggerFlushImmediately || shouldFlush) $ loggerFlush lgrset
      )
      (Log.rmLoggerSet lgrset)
      (loggerFlush lgrset)

    -- Rotate file logger
    rotateLoggerInit fspec bsize = do
      lgrset <- Log.newFileLoggerSet bsize $ Log.log_file fspec
      ref <- newIORef (0 :: Int)
      mvar <- newMVar ()
      return $ Logger
        (\level shouldFlush cstack s ->
          when (level >= loggerLevel) $ do
            cnt <- decrease ref
            tid <- myThreadId
            time <- defaultTimeCache
            Log.pushLogStr lgrset $
              loggerFormatter (Log.toLogStr time) level s cstack tid
            when (loggerFlushImmediately || shouldFlush) $ loggerFlush lgrset
            when (cnt <= 0) $ tryRotate lgrset fspec ref mvar
        )
        (Log.rmLoggerSet lgrset)
        (loggerFlush lgrset)

    loggerFlush lgrset = Log.flushLogStr lgrset

globalLogger :: IORef Logger
globalLogger = unsafePerformIO $ do
  istty <- queryTerminal stdError
  let fmt = if istty then defaultColoredFmt else defaultFmt
  newIORef =<< newLogger defaultLoggerConfig{loggerFormatter = fmt}
{-# NOINLINE globalLogger #-}

-- | Change the global logger.
setGlobalLogger :: Logger -> IO ()
setGlobalLogger !logger = atomicWriteIORef globalLogger logger
{-# INLINABLE setGlobalLogger #-}

getGlobalLogger :: IO Logger
getGlobalLogger = readIORef globalLogger
{-# INLINABLE getGlobalLogger #-}

-- | Set the global logger by config.
setDefaultLogger :: Level -> Bool -> LogType -> Bool -> IO ()
setDefaultLogger level withColor typ flushImdt = do
  let config = defaultLoggerConfig
        { loggerLevel = level
        , loggerFormatter = if withColor then defaultColoredFmt else defaultFmt
        , loggerType = typ
        , loggerFlushImmediately = flushImdt
        }
  setGlobalLogger =<< newLogger config

-- | Set the global logger by level.
setDefaultLoggerLevel :: Level -> IO ()
setDefaultLoggerLevel lvl =
  setGlobalLogger =<< newLogger defaultLoggerConfig{loggerLevel = lvl}

-- NOTE: do **NOT** use this with nesting calls like:
--
-- @
-- Log.withDefaultLogger $ do
--   Log.withDefaultLogger $ do
--     Log.debug $ "..."
--   Log.debug $ "..."
-- @
--
-- because the inner call will call the release function to release the global
-- LoggerSet.
withDefaultLogger :: IO () -> IO ()
withDefaultLogger = (`finally` clean)
  where
    clean = getGlobalLogger >>= \(Logger _ c _) -> c
{-# INLINABLE withDefaultLogger #-}

-------------------------------------------------------------------------------

trace :: HasCallStack => Log.LogStr -> IO ()
trace = logBylevel False TRACE callStack

debug :: HasCallStack => Log.LogStr -> IO ()
debug = logBylevel False DEBUG callStack

debug1 :: HasCallStack => Log.LogStr -> IO ()
debug1 = logBylevel False DEBUG1 callStack

info :: HasCallStack => Log.LogStr -> IO ()
info = logBylevel False INFO callStack

i :: HasCallStack => Log.LogStr -> IO ()
i = logBylevel True INFO callStack

warning :: HasCallStack => Log.LogStr -> IO ()
warning = logBylevel True WARNING callStack

fatal :: HasCallStack => Log.LogStr -> IO ()
fatal = logBylevel True FATAL callStack

warnSlow :: Int -> Int -> Log.LogStr -> IO a -> IO a
warnSlow starter duration msg f = Async.withAsync f $ \a1 ->
  Async.withAsync h $ \_a2 -> Async.wait a1
  where
    h = do threadDelay starter
           forever $ do warning msg
                        threadDelay duration

logBylevel :: Bool -> Level -> CallStack -> Log.LogStr -> IO ()
logBylevel shouldFlush level cstack s = do
  Logger f _cleanFun _flushFun <- getGlobalLogger
  f level shouldFlush cstack s
{-# INLINABLE logBylevel #-}

-------------------------------------------------------------------------------

-- this cannot be capi, as GHC panics.
#if __GLASGOW_HASKELL__ >= 904
-- https://gitlab.haskell.org/ghc/ghc/-/merge_requests/6163
foreign import ccall unsafe "rts_getThreadId" getThreadId :: ThreadId# -> CULLong
#elif __GLASGOW_HASKELL__ >= 900
-- https://gitlab.haskell.org/ghc/ghc/-/merge_requests/1254
foreign import ccall unsafe "rts_getThreadId" getThreadId :: ThreadId# -> CLong
#else
foreign import ccall unsafe "rts_getThreadId" getThreadId :: ThreadId# -> CInt
#endif

decrease :: IORef Int -> IO Int
decrease ref = atomicModifyIORef' ref (\x -> (x - 1, x - 1))

tryRotate :: Log.LoggerSet -> Log.FileLogSpec -> IORef Int -> MVar () -> IO ()
tryRotate lgrset spec ref mvar = bracket lock unlock rotateFiles
  where
    lock           = tryTakeMVar mvar
    unlock Nothing = return ()
    unlock _       = putMVar mvar ()
    rotateFiles Nothing = return ()
    rotateFiles _       = do
      msiz <- getSize
      case msiz of
        -- A file is not available.
        -- So, let's set a big value to the counter so that
        -- this function is not called frequently.
        Nothing -> writeIORef ref 1000000
        Just siz
          | siz > limit -> do
              Log.rotate spec
              Log.renewLoggerSet lgrset
              writeIORef ref $ estimate limit
          | otherwise -> writeIORef ref $ estimate (limit - siz)
    file = Log.log_file spec
    limit = Log.log_file_size spec
    getSize = handle (\(SomeException _) -> return Nothing) $
      -- The log file is locked by GHC.
      -- We need to get its file size by the way not using locks.
      Just . fromIntegral <$> getFileSize file
    -- 200 is an ad-hoc value for the length of log line.
    estimate x = fromInteger (x `div` 200)
