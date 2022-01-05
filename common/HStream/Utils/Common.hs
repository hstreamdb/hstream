module HStream.Utils.Common
  ( maybeToEither
  , withoutPrefix
  , setupSigsegvHandler
  ) where

import           Data.List  (stripPrefix)
import           Data.Maybe (fromMaybe)

maybeToEither :: b -> Maybe a -> Either b a
maybeToEither errmsg = maybe (Left errmsg) Right

withoutPrefix :: Eq a => [a] -> [a] -> [a]
withoutPrefix prefix ele = fromMaybe ele $ stripPrefix prefix ele

foreign import ccall unsafe "hs_common.h setup_sigsegv_handler"
  setupSigsegvHandler :: IO ()
