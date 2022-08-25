{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}

module CompresstionBench where

import           Control.Monad             (forM, forM_)
import           Criterion                 (Benchmark, bench, bgroup, env, nf)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BSL
import           Data.Word                 (Word64)
import qualified HStream.Logger            as Log
import           HStream.Server.HStreamApi
import           HStream.Utils             (Encoder, EncoderType (..), compress,
                                            decompress)
import           Numeric                   (showFFloat)
import           Proto3.Suite
import           Util

gzip :: Enumerated CompressionType
gzip = Enumerated $ Right CompressionTypeGzip

benchCompresstion :: [Benchmark]
benchCompresstion = compressBench <> decompressBench

compressBench :: [Benchmark]
compressBench = [
  bgroup "compress with origin encode" $
    map (\(size, describe) ->
          env (genEncodedHStreamRecords @'OriginEncoder size) $ \records ->
              bench describe $ nf (compress gzip) records) testPair,

  bgroup "compress with Proto encode" $
    map (\(size, describe) ->
          env (genEncodedHStreamRecords @'ProtoEncoder size) $ \records ->
              bench describe $ nf (compress gzip) records) testPair
 ]

decompressBench :: [Benchmark]
decompressBench = [
  bgroup "decompress with origin encode" $
    map (\(size, describe) ->
          env (genCompressedRecords @'OriginEncoder gzip size) $ \records ->
              bench describe $ nf (decompress gzip) records) testPair,

  bgroup "decompress with Proto encode" $
    map (\(size, describe) ->
          env (genCompressedRecords @'ProtoEncoder gzip size) $ \records ->
              bench describe $ nf (decompress gzip) records) testPair
 ]

getCompressSize
  :: forall a. Encoder a
  => Enumerated CompressionType
  -> Int                        -- payload size of HStreamRecord
  -> IO (Word64, Word64)        -- (size of SerializedHStreamRecord, size of CompressedRecord)
getCompressSize compType size = do
  serializedRecord <- genEncodedHStreamRecords @a size
  let compressedRecord = compress compType serializedRecord
  return (fromIntegral $ BSL.length serializedRecord, fromIntegral $ BS.length compressedRecord)

testCompressionSize :: IO ()
testCompressionSize = do
  res1 <- forM testPair $ \(size, _) -> getCompressSize @'OriginEncoder gzip size
  res2 <- forM testPair $ \(size, _) -> getCompressSize @'ProtoEncoder gzip size
  let resultPairs = zipWith (\(origin1, c1) (origin2, c2) -> ((origin1, c1), (origin2, c2))) res1 res2
  forM_ resultPairs $ \((o1, c1), (o2, c2)) -> do
    Log.info $ "[origin encoded]: origin size " <> Log.buildInt o1
            <> ", compressed size " <> Log.buildInt c1
            <> ", compression rate " <> Log.buildString' (showFFloat @Double (Just 3) (fromIntegral c1 / fromIntegral o1 * 100) "%")
    Log.info $ "[proto encoded]: origin size " <> Log.buildInt o2
            <> ", compressed size " <> Log.buildInt c2
            <> ", compression rate " <> Log.buildString' (showFFloat @Double (Just 3) (fromIntegral c2 / fromIntegral o2 * 100) "%")

