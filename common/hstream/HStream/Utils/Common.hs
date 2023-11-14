module HStream.Utils.Common
  ( maybeToEither
  , newRandomText
  , limitedMapConcurrently
  , splitOn
  ) where

import           Control.Concurrent.Async (mapConcurrently)
import           Control.Concurrent.QSem  (QSem, newQSem, signalQSem, waitQSem)
import           Control.Exception        (bracket_)
import qualified Data.ByteString          as BS
import           Data.Text                (Text)
import qualified Data.Text                as Text
import           System.Random

maybeToEither :: b -> Maybe a -> Either b a
maybeToEither errmsg = maybe (Left errmsg) Right

newRandomText :: Int -> IO Text
newRandomText n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen

limitedMapConcurrently :: Int -> (a -> IO b) -> [a] -> IO [b]
limitedMapConcurrently maxConcurrency f inputs = do
  sem <- newQSem maxConcurrency
  mapConcurrently (limited sem . f) inputs
 where
   limited :: QSem -> IO c -> IO c
   limited sem = bracket_ (waitQSem sem) (signalQSem sem)

  -- Break a ByteString into pieces separated by the first ByteString argument, consuming the delimiter
splitOn :: BS.ByteString -> BS.ByteString -> [BS.ByteString]
splitOn ""        = error "delimiter shouldn't be empty."
splitOn delimiter = go
  where
    go s = let (pre, post) = BS.breakSubstring delimiter s
            in pre : if BS.null post then [] else go (BS.drop (BS.length delimiter) post)
