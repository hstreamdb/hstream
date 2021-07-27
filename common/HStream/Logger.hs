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

  , module Z.Data.Builder
  ) where

import           Z.Data.Builder
import qualified Z.IO.Logger    as Log

d :: Builder () -> IO ()
d = Log.withDefaultLogger . Log.debug

i :: Builder () -> IO ()
i = Log.withDefaultLogger . Log.info

w :: Builder () -> IO ()
w = Log.withDefaultLogger . Log.warning

e :: Builder () -> IO ()
e = Log.withDefaultLogger . Log.fatal
