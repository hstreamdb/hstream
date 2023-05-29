module HStream.Utils.Common
  ( maybeToEither
  , newRandomText
  ) where

import           Data.Text     (Text)
import qualified Data.Text     as Text
import           System.Random

maybeToEither :: b -> Maybe a -> Either b a
maybeToEither errmsg = maybe (Left errmsg) Right

newRandomText :: Int -> IO Text
newRandomText n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen
