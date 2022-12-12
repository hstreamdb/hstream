module HStream.Utils.Common
  ( maybeToEither
  , withoutPrefix
  , setupSigsegvHandler
  , newRandomText
  ) where

import           Data.List     (stripPrefix)
import           Data.Maybe    (fromMaybe)
import           Data.Text     (Text)
import qualified Data.Text     as Text
import           System.Random

maybeToEither :: b -> Maybe a -> Either b a
maybeToEither errmsg = maybe (Left errmsg) Right

withoutPrefix :: Eq a => [a] -> [a] -> [a]
withoutPrefix prefix ele = fromMaybe ele $ stripPrefix prefix ele

foreign import ccall unsafe "hs_common.h setup_sigsegv_handler"
  setupSigsegvHandler :: IO ()

newRandomText :: Int -> IO Text
newRandomText n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen
