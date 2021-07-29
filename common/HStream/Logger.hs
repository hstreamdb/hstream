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
  , module Z.Data.Builder
  , fromText
  , fromLazyText
  ) where

import qualified Data.Text      as Text
import qualified Data.Text.Lazy as TL
import           GHC.Stack      (HasCallStack)
import           Z.Data.Builder
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

fromText :: Text.Text -> Builder ()
fromText = U.textToZBuilder
{-# INLINE fromText #-}

fromLazyText :: TL.Text -> Builder ()
fromLazyText = U.lazyTextToZBuilder
{-# INLINE fromLazyText #-}
