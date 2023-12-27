{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

module Main where

import           Criterion.Main
import qualified Data.ByteString         as BS
import           Data.Vector             (Vector)
import qualified Data.Vector             as V
import qualified Kafka.Protocol.Encoding as K

main :: IO ()
main = do
  -- Use ./gen_data.py to generate the data file first
  !batchBs1K1 <- BS.readFile "/tmp/records_1k_1.data"
  !batchBs1K100 <- BS.readFile "/tmp/records_1k_100.data"
  !batchBs1K1000 <- BS.readFile "/tmp/records_1k_1000.data"

  defaultMain
    [ bgroup "vector"
        [ bench "pure empty" $ nfIO @(Vector Int) (pure V.empty)
        , bench "replicateM 0" $ nfIO @(Vector Int) (V.replicateM 0 (pure 0))
        ]
    , bgroup "decode records"
        [ bench "1K*1" $ nfIO $ K.decodeBatchRecords' True batchBs1K1
        , bench "1K*100" $ nfIO $ K.decodeBatchRecords' True batchBs1K100
        , bench "1K*1000" $ nfIO $ K.decodeBatchRecords' True batchBs1K1000
        ]
    ]
