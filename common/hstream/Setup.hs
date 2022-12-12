{-# LANGUAGE CPP #-}

import           Distribution.Simple
import           System.Environment

main = do
  args <- getArgs
  if head args == "configure"
     then defaultMainArgs $ [ "--with-gcc=c++"
#if __GLASGOW_HASKELL__ >= 810
                            , "--ghc-options", "-optcxx-std=c++17"
#endif
                            ] ++ args
     else defaultMain
