{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Criterion.Main
import qualified Data.ByteString as BS
import           Data.Char       (ord)
import qualified Data.Vector     as V
import qualified Network.HESP    as HESP

main :: IO ()
main =
  defaultMain
    [ bgroup "mkArray"
        [ bench "10B*10000" $ whnf (mkArray 10) 10000
        , bench "10000B*10" $ whnf (mkArray 10000) 10
        ]
    , bgroup "encodeArray"
        [ bench "10B*10000" $ whnf (encodeArray 10) 10000
        , bench "10000B*10" $ whnf (encodeArray 10000) 10
        ]
    , bgroup "decodeArray"
        [ bench "10B*10000" $ whnf decodeArray (encodeArray 10 10000)
        , bench "10000B*10" $ whnf decodeArray (encodeArray 10000 10)
        ]
    ]

mkArray :: Int -> Int -> HESP.Message
mkArray x y =
  let k = fromIntegral $ ord 'x'
   in HESP.mkArray $! V.replicate y $! HESP.mkBulkString $! BS.replicate x k

encodeArray :: Int -> Int -> BS.ByteString
encodeArray x y = HESP.serialize $! mkArray x y

decodeArray :: BS.ByteString -> HESP.Message
decodeArray bs = head $ do
  e_m <- (V.! 0) <$> HESP.deserializeWith ([bs]) ""
  case e_m of
    Left errmsg -> error errmsg
    Right msg   -> return msg
