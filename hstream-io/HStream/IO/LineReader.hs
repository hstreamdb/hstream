module HStream.IO.LineReader where

import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.Text.Lazy as TL
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BSL

-- TODO:
-- + optimize sequential read(cache latest line offset)
-- + optimize random read(cache line offsets)
newtype LineReader = LineReader
  { filePath :: String
  -- lineOffsets
  }

newLineReader :: String -> IO LineReader
newLineReader filePath = do
  return $ LineReader{..}

readLines :: LineReader -> Int -> Int -> IO T.Text
readLines LineReader{..} begin count = do
  TL.toStrict . TL.unlines . take count . drop (begin - 1) . TL.lines . TL.decodeUtf8
    <$> BSL.readFile filePath
