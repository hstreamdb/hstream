{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}

{-
This is needed to make ghci work.

Relatead ghc issues:
* https://gitlab.haskell.org/ghc/ghc/-/issues/19733
* https://gitlab.haskell.org/ghc/ghc/-/issues/15454
-}
{-# OPTIONS_GHC -fobject-code #-}

module HStream.LoggerOld
  ( Log_.Logger
  , Log_.withDefaultLogger

  , trace
  , Log_.debug
  , Log_.info
  , Log_.warning
  , Log_.fatal
  , t, d, i, w, e
  , warnSlow

  , Log_.LoggerConfig (..)
  , setLogConfig
  , Log_.defaultLoggerConfig

  , build
  , buildString
  , buildString'

  -- * Log Level
  , L.Level (..)
  , pattern L.CRITICAL
  , pattern L.FATAL
  , pattern L.WARNING
  , pattern L.INFO
  , pattern L.DEBUG
  , pattern L.TRACE
  , pattern L.NOTSET

  , Log_.flushDefaultLogger
  , Log_.setDefaultLogger
  , Log_.getDefaultLogger
  ) where

import           Control.Concurrent       (threadDelay)
import qualified Control.Concurrent.Async as Async
import           Control.Monad            (forever)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString.Char8    as BSC
import qualified Data.Text                as Text
import qualified Data.Text.Lazy           as TL
import           Foreign.C.Types          (CInt (..))
import           GHC.Conc.Sync            (ThreadId (..))
import           GHC.Exts                 (ThreadId#)
import           GHC.Stack

import qualified Z.Data.Builder           as B
import           Z.Data.Builder           (Builder)
import qualified Z.IO.Logger              as Log_
import qualified Z.IO.StdStream.Ansi      as Z

import qualified HStream.Logger           as L

trace :: HasCallStack => Builder () -> IO ()
trace = Log_.otherLevel (L.unLevel L.TRACE) False{- flush immediately ? -}
{-# INLINE trace #-}

t :: HasCallStack => Builder () -> IO ()
t = Log_.withDefaultLogger . trace

d :: HasCallStack => Builder () -> IO ()
d = Log_.withDefaultLogger . Log_.debug

i :: HasCallStack => Builder () -> IO ()
i = Log_.withDefaultLogger . Log_.info

w :: HasCallStack => Builder () -> IO ()
w = Log_.withDefaultLogger . Log_.warning

e :: HasCallStack => Builder () -> IO ()
e = Log_.withDefaultLogger . Log_.fatal

setLogConfig :: L.Level -> Bool -> IO ()
setLogConfig level withColor = do
  let config = Log_.defaultLoggerConfig
        { Log_.loggerLevel = L.unLevel level
        , Log_.loggerFormatter = if withColor
                                then defaultColoredFmt_
                                else defaultFmt_
        }
  Log_.setDefaultLogger =<< Log_.newStdLogger config


-- | A default log formatter
--
-- @[FATAL][2021-02-01T15:03:30+0800][<interactive>:31:1][thread#669]...@
defaultFmt_ :: Log_.LogFormatter
defaultFmt_ ts level content cstack (ThreadId tid#) = do
    B.square (defaultLevelFmt_ $ L.Level level)
    B.square ts
    B.square $ Log_.defaultFmtCallStack cstack
    B.square $ "thread#" >> B.int (getThreadId tid#)
    content
    B.char8 '\n'

-- | A default colored log formatter
--
-- DEBUG level is 'Cyan', WARNING level is 'Yellow', FATAL and CRITICAL level are 'Red'.
defaultColoredFmt_ :: Log_.LogFormatter
defaultColoredFmt_ ts level content cstack (ThreadId tid#) = do
  let blevel = defaultLevelFmt_ (L.Level level)
  B.square (case level of
    Log_.DEBUG    -> Z.color Z.Cyan blevel
    Log_.INFO     -> Z.color Z.Magenta blevel
    Log_.WARNING  -> Z.color Z.Yellow blevel
    Log_.FATAL    -> Z.color Z.Red blevel
    Log_.CRITICAL -> Z.color Z.Red blevel
    _             -> blevel)
  B.square ts
  B.square $ Log_.defaultFmtCallStack cstack
  B.square $ "thread#" >> B.int (getThreadId tid#)
  content
  B.char8 '\n'

-- | Format 'DEBUG' to 'CRITICAL', etc.
--
-- Level other than built-in ones, are formatted in decimal numeric format, i.e.
-- @defaultLevelFmt 60 == "LEVEL60"@
defaultLevelFmt_ :: L.Level -> B.Builder ()
{-# INLINE defaultLevelFmt_ #-}
defaultLevelFmt_ level = case level of
  L.CRITICAL -> "CRITICAL"
  L.FATAL    -> "FATAL"
  L.WARNING  -> "WARNING"
  L.INFO     -> "INFO"
  L.DEBUG    -> "DEBUG"
  L.TRACE    -> "TRACE"
  L.NOTSET   -> "NOTSET"
  level'     -> "LEVEL" >> B.int (L.unLevel level')

foreign import ccall unsafe "rts_getThreadId" getThreadId :: ThreadId# -> CInt

warnSlow :: Int -> Int -> Builder () -> IO a -> IO a
warnSlow starter duration msg f = Async.withAsync f $ \a1 ->
  Async.withAsync h $ \_a2 -> Async.wait a1
  where
    h = do threadDelay starter
           forever $ do Log_.warning msg
                        threadDelay duration

-------------------------------------------------------------------------------

class ToLogStr msg where
  toLogStr :: msg -> Builder ()

instance (Integral a, Bounded a) => ToLogStr a where
  {-# INLINE toLogStr #-}
  toLogStr = B.int

instance ToLogStr Text.Text where
  {-# INLINE toLogStr #-}
  toLogStr = B.stringUTF8 . Text.unpack

instance ToLogStr TL.Text where
  {-# INLINE toLogStr #-}
  toLogStr = B.stringUTF8 . Text.unpack . TL.toStrict

instance ToLogStr ByteString where
  {-# INLINE toLogStr #-}
  toLogStr = B.stringUTF8 . BSC.unpack

build :: ToLogStr a => a -> Builder ()
build = toLogStr

buildString :: String -> Builder ()
buildString = B.stringUTF8
{-# INLINE buildString #-}

buildString' :: Show a => a -> Builder ()
buildString' = B.stringUTF8 . show
{-# INLINE buildString' #-}
