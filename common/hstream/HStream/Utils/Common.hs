module HStream.Utils.Common
  ( maybeToEither
  , setupFatalSignalHandler
  , newRandomText
  ) where

import           Data.List     (stripPrefix)
import           Data.Maybe    (fromMaybe)
import           Data.Text     (Text)
import qualified Data.Text     as Text
import           System.Random

maybeToEither :: b -> Maybe a -> Either b a
maybeToEither errmsg = maybe (Left errmsg) Right

foreign import ccall unsafe "hs_common.h setup_fatal_signal_handler"
  setupFatalSignalHandler :: IO ()

newRandomText :: Int -> IO Text
newRandomText n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen
