{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module CodecBench where

import           Control.Monad        (forM)
import           Criterion            (Benchmark, bench, bgroup, env, nf)
import qualified Data.ByteString.Lazy as BSL
import           Data.Word            (Word64)
import           HStream.Utils        (Encoder, EncoderType (..),
                                       deserializeHStreamRecords,
                                       serializeHStreamRecords)
import           Util

benchCodec :: [Benchmark]
benchCodec = encodeBench <> decodeBench

encodeBench :: [Benchmark]
encodeBench = [
  bgroup "origin encode" $
    map (\(size, describe) ->
          env (genHStreamRecords size) $ \records ->
            bench describe $ nf (serializeHStreamRecords @'OriginEncoder) records) testPair,

  bgroup "proto encode" $
    map (\(size, describe) ->
          env (genHStreamRecords size) $ \records ->
            bench describe $ nf (serializeHStreamRecords @'ProtoEncoder) records) testPair
 ]

decodeBench :: [Benchmark]
decodeBench = [
  bgroup "origin decode" $
    map (\(size, describe) ->
          env (genEncodedHStreamRecords @'OriginEncoder size) $ \records ->
            bench describe $ nf (deserializeHStreamRecords @'OriginEncoder) records) testPair,

  bgroup "proto decode" $
    map (\(size, describe) ->
          env (genEncodedHStreamRecords @'ProtoEncoder size) $ \records ->
            bench describe $ nf (deserializeHStreamRecords @'ProtoEncoder) records) testPair
 ]

getSize :: forall a. Encoder a => Int -> IO Word64
getSize size = fromIntegral . BSL.length <$> genEncodedHStreamRecords @a size

testCodecSize :: IO [(Word64, Word64)]
testCodecSize = do
  res1 <- forM testPair $ \(size, _) -> getSize @'OriginEncoder size
  res2 <- forM testPair $ \(size, _) -> getSize @'ProtoEncoder size
  return $ zip res1 res2

