{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.IO.LineReader where

import qualified Control.Concurrent         as C
import qualified Control.Exception          as E
import           Data.Int                   (Int64)
import qualified Data.Text                  as T
import qualified Data.Text.Internal.Builder as T
import qualified Data.Text.IO               as T
import qualified Data.Text.Lazy             as TL
import qualified System.IO                  as IO

-- TODO:
-- + [FINISHED] optimize sequential read(cache latest line offset)
-- + optimize random read(cache line offsets)
data LineReader = LineReader
  { filePath         :: String
  , latestLineOffset :: C.MVar (Int, Int64)
  }

newLineReader :: String -> IO LineReader
newLineReader filePath = do
  latestLineOffset <- C.newMVar (1, 0)
  return $ LineReader{..}

readLines :: LineReader -> Int -> Int -> IO T.Text
readLines lr@LineReader{..} begin count = do
  IO.withFile filePath IO.ReadMode $ \hdl -> do
    seekToLineOffset lr hdl 0 (begin - 1) >>= \case
      False -> return ""
      True -> do
        (result, gotCount) <- getLines hdl mempty 0 count
        currentOffset <- IO.hTell hdl
        _ <- C.swapMVar latestLineOffset (begin + gotCount, fromIntegral currentOffset)
        return . TL.toStrict $ T.toLazyText result

seekToLineOffset :: LineReader -> IO.Handle -> Int -> Int -> IO Bool
seekToLineOffset LineReader{..} hdl begin count = do
  (lineNum, fileOffset) <- C.readMVar latestLineOffset
  if lineNum == begin then do
    IO.hSeek hdl IO.AbsoluteSeek (fromIntegral fileOffset)
    return True
  else
    dropLines hdl begin count

dropLines :: IO.Handle -> Int -> Int -> IO Bool
dropLines hdl count total = do
  if count < total then
    E.try (T.hGetLine hdl) >>= \case
      Left (_ :: E.SomeException) -> return False
      Right _ -> dropLines hdl (count + 1) total
  else
    return True

getLines :: IO.Handle -> T.Builder -> Int -> Int -> IO (T.Builder, Int)
getLines hdl builder count total = do
  if count < total then
    E.try (T.hGetLine hdl) >>= \case
      Left (_ :: E.SomeException) -> return (builder, count)
      Right line -> do
        let newBuilder = builder <> "\n" <> T.fromText line
        getLines hdl newBuilder (count + 1) total
  else
    return (builder, total)
