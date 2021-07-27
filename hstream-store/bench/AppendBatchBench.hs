{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Monad          (void)
import qualified Criterion.Main         as C
import qualified Criterion.Main.Options as C
import qualified Data.ByteString        as B
import qualified Options.Applicative    as O
import           Z.Data.CBytes          (CBytes)
import           Z.Data.Vector          (Bytes)
import qualified Z.Data.Vector          as V

import qualified Data.ByteUnits         as BU
import qualified HStream.Store          as S
import qualified HStream.Store.Logger   as S

-- | To run the benchmark:
--
-- > cabal run -- hstore-bench-append-batch --config /data/store/logdevice.conf --logid 1 --output bench.html --regress allocated:iters +RTS -T
main :: IO ()
main = do
  AppendBatchBenchOpts{..} <- O.execParser $ C.describeWith appendBenchParser
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  client <- S.newLDClient configFile
  runAppendBatchBench mode client logid

data AppendBatchBenchOpts = AppendBatchBenchOpts
  { configFile :: CBytes
  , logid      :: S.C_LogID
  , mode       :: C.Mode
  }

appendBenchParser :: O.Parser AppendBatchBenchOpts
appendBenchParser = AppendBatchBenchOpts
  <$> O.strOption ( O.long "config"
               <> O.metavar "STRING"
               <> O.help "path to the client config file")
  <*> O.option O.auto ( O.long "logid"
                     <> O.metavar "INT"
                     <> O.help "the logid to append to")
  <*> C.parseWith C.defaultConfig

runAppendBatchBench :: C.Mode -> S.LDClient -> S.C_LogID -> IO ()
runAppendBatchBench mode client logid = do
  C.runMode mode
    [ let (size, num) = (1024, 100)
      in C.bgroup (message size num) $ benches size num
    , let (size, num) = (1024, 1000)
      in C.bgroup (message size num) $ benches size num
    , let (size, num) = (1024 * 1024, 100)
      in C.bgroup (message size num) $ benches size num
    ]
    where
      bytesInBatch :: [Bytes] -> IO ()
      bytesInBatch bytes = void $ S.appendBatch client logid bytes S.CompressionLZ4 Nothing

      byteStringInBatch :: [B.ByteString] -> IO ()
      byteStringInBatch bs = void $ S.appendBatchBS client logid bs S.CompressionLZ4 Nothing

      message :: Int -> Int -> String
      message size num = "append " <> show num <> " * size " <> prettySize size <> " messages in batch"

      prettySize :: Int -> String
      prettySize size = BU.getShortHand (BU.getAppropriateUnits (BU.ByteValue (fromIntegral size) BU.Bytes))

      benches :: Int -> Int -> [C.Benchmark]
      benches size num =
        let !bytes = V.replicate size 97
            !bs    = B.replicate size 97
            !bytesBatch = replicate num bytes
            !bsBatch    = replicate num bs
        in
          [ C.bench "append bytes in batch" $ C.nfIO (bytesInBatch bytesBatch)
          , C.bench "append bytestring in batch" $ C.nfIO (byteStringInBatch bsBatch)
          ]
