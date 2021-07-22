{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Monad          (void)
import qualified Criterion.Main         as C
import qualified Criterion.Main.Options as C
import qualified Data.ByteString        as B
import qualified Data.ByteString.Lazy   as LB
import qualified Options.Applicative    as O
import           Z.Data.CBytes          (CBytes)
import           Z.Data.Vector          (Bytes)
import qualified Z.Data.Vector          as V
import qualified Z.Foreign              as ZF

import qualified HStream.Store          as S
import qualified HStream.Store.Logger   as S

-- | To run the benchmark:
--
-- > cabal run -- hstore-bench-append --config /data/store/logdevice.conf --logid 1 --output bench.html --regress allocated:iters +RTS -T
main :: IO ()
main = do
  AppendBenchOpts{..} <- O.execParser $ C.describeWith appendBenchParser
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  client <- S.newLDClient configFile
  runAppendBench mode client logid

data AppendBenchOpts = AppendBenchOpts
  { configFile :: CBytes
  , logid      :: S.C_LogID
  , mode       :: C.Mode
  }

appendBenchParser :: O.Parser AppendBenchOpts
appendBenchParser = AppendBenchOpts
  <$> O.strOption ( O.long "config"
               <> O.metavar "STRING"
               <> O.help "path to the client config file")
  <*> O.option O.auto ( O.long "logid"
                     <> O.metavar "INT"
                     <> O.help "the logid to append to")
  <*> C.parseWith C.defaultConfig

runAppendBench :: C.Mode -> S.LDClient -> S.C_LogID -> IO ()
runAppendBench mode client logid = do
  C.runMode mode
    [ C.bgroup "5 bytes message" $
      let !bytes = "hello" :: Bytes
          !bs    = "Hello" :: B.ByteString
          !lbs   = "Hello" :: LB.ByteString
      in
        [ C.bench "vanilla Bytes" $ C.nfIO (vanillaBytes bytes)
        , C.bench "vanilla ByteString" $ C.nfIO (vanillaByteString bs)
        , C.bench "ByteString to Bytes" $ C.nfIO (byteStringToBytes bs)
        , C.bench "Bytes to ByteString" $ C.nfIO (bytesToByteString bytes)
        , C.bench "LazyByteString to Bytes" $ C.nfIO (lazyByteStringToBytes lbs)
        , C.bench "LazyByteString to ByteString" $ C.nfIO (lazyByteStringToByteString lbs)
        ]
    , C.bgroup "1 kb message" $
      let !bytes = V.replicate 1024 97
          !bs    = B.replicate 1024 97
          !lbs   = LB.replicate 1024 97
      in
        [ C.bench "vanilla Bytes" $ C.nfIO (vanillaBytes bytes)
        , C.bench "vanilla ByteString" $ C.nfIO (vanillaByteString bs)
        , C.bench "ByteString to Bytes" $ C.nfIO (byteStringToBytes bs)
        , C.bench "Bytes to ByteString" $ C.nfIO (bytesToByteString bytes)
        , C.bench "LazyByteString to Bytes" $ C.nfIO (lazyByteStringToBytes lbs)
        , C.bench "LazyByteString to ByteString" $ C.nfIO (lazyByteStringToByteString lbs)
        ]
    , C.bgroup "1 mb message" $
      let !bytes = V.replicate (1024 * 1024) 97
          !bs    = B.replicate (1024 * 1024) 97
          !lbs   = LB.replicate (1024 * 1024) 97
      in
        [ C.bench "vanilla Bytes" $ C.nfIO (vanillaBytes bytes)
        , C.bench "vanilla ByteString" $ C.nfIO (vanillaByteString bs)
        , C.bench "ByteString to Bytes" $ C.nfIO (byteStringToBytes bs)
        , C.bench "Bytes to ByteString" $ C.nfIO (bytesToByteString bytes)
        , C.bench "LazyByteString to Bytes" $ C.nfIO (lazyByteStringToBytes lbs)
        , C.bench "LazyByteString to ByteString" $ C.nfIO (lazyByteStringToByteString lbs)
        ]
    ]
    where
      vanillaBytes :: Bytes -> IO ()
      vanillaBytes bytes = void $ S.append client logid bytes Nothing

      vanillaByteString :: B.ByteString -> IO ()
      vanillaByteString bs = void $ S.appendBS client logid bs Nothing

      byteStringToBytes :: B.ByteString -> IO ()
      byteStringToBytes = vanillaBytes . ZF.fromByteString

      bytesToByteString :: Bytes -> IO ()
      bytesToByteString = vanillaByteString . ZF.toByteString

      lazyByteStringToBytes :: LB.ByteString -> IO ()
      lazyByteStringToBytes = vanillaBytes . ZF.fromByteString . LB.toStrict

      lazyByteStringToByteString :: LB.ByteString -> IO ()
      lazyByteStringToByteString = vanillaByteString . LB.toStrict
