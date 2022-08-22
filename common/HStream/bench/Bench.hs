import           Criterion.Main

import           CodecBench
import           CompresstionBench

main :: IO ()
main = defaultMain benchmarks
 where
   benchmarks = benchCodec <> benchCompresstion
