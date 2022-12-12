{-# LANGUAGE AllowAmbiguousTypes #-}

module Util where

import           Control.Monad
import qualified Data.ByteString           as BS
import           Data.ByteString.Internal  (c2w)
import qualified Data.ByteString.Lazy      as BSL
import           Data.Vector               (Vector)
import qualified Data.Vector               as V
import           HStream.Utils             (Encoder, compress,
                                            serializeHStreamRecords)
import           System.Random

import           HStream.Server.HStreamApi
import           Proto3.Suite

genHStreamRecords :: Int -> IO (Vector HStreamRecord)
genHStreamRecords n = V.replicateM n (HStreamRecord Nothing <$> newRandomByteString 100)

genEncodedHStreamRecords :: forall a. Encoder a => Int -> IO BSL.ByteString
genEncodedHStreamRecords n = serializeHStreamRecords @a <$> genHStreamRecords n

genCompressedRecords :: forall a. Encoder a => Enumerated CompressionType -> Int -> IO BS.ByteString
genCompressedRecords compType size = do
  serializedRecords <- genEncodedHStreamRecords @a size
  return $ compress compType serializedRecords

newRandomByteString :: Int -> IO BS.ByteString
newRandomByteString n = BS.pack <$> replicateM n (c2w <$> randomRIO ('a', 'z'))

testPair :: [(Int, String)]
testPair = [ (1, "100 Byte")
           , (10, "1K")
           , (100, "10K")
           , (1000, "100K")
           , (10000, "1M")
           ]
