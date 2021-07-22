{-# LANGUAGE BangPatterns #-}

module HStream.AppendBench where

import           Criterion.Main
import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as LB
import           System.IO.Unsafe     (unsafePerformIO)
import           Z.Data.Vector        (Bytes)
import qualified Z.Data.Vector        as V

import           Control.Monad        (void)
import qualified HStream.Store        as S
import qualified HStream.Store.Logger as S
import qualified Z.Foreign            as ZF

client :: S.LDClient
client = unsafePerformIO $ do
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  S.newLDClient "/data/store/logdevice.conf"
{-# NOINLINE client #-}

logid :: S.C_LogID
logid = 1

vanillaAppend :: Bytes -> IO ()
vanillaAppend bytes = void $ S.append client logid bytes Nothing

byteStringAppend :: B.ByteString -> IO ()
byteStringAppend = vanillaAppend . ZF.fromByteString

lazyByteStringAppend :: LB.ByteString -> IO ()
lazyByteStringAppend = vanillaAppend . ZF.fromByteString . LB.toStrict

appendBench :: [Benchmark]
appendBench =
  [ bgroup "5 bytes message"
    [ bench "vanilla" $ nfIO (vanillaAppend "hello")
    , bench "bytestring" $ nfIO (byteStringAppend "hello")
    , bench "lazy bytestring" $ nfIO (lazyByteStringAppend "hello")
    ]
  , bgroup "1 kb message" $
    let !bytes = V.replicate 1024 97
        !bs    = B.replicate 1024 97
        !lbs   = LB.replicate 1024 97
    in
      [ bench "vanilla" $ nfIO (vanillaAppend bytes)
      , bench "bytestring" $ nfIO (byteStringAppend bs)
      , bench "lazy bytestring" $ nfIO (lazyByteStringAppend lbs)
      ]
  , bgroup "1 mb message" $
    let !bytes = V.replicate (1024 * 1024) 97
        !bs    = B.replicate (1024 * 1024) 97
        !lbs   = LB.replicate (1024 * 1024) 97
    in
      [ bench "vanilla" $ nfIO (vanillaAppend bytes)
      , bench "bytestring" $ nfIO (byteStringAppend bs)
      , bench "lazy bytestring" $ nfIO (lazyByteStringAppend lbs)
      ]

  ]
