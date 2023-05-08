module HStream.Base.Concurrent
  ( runConc
  ) where

import           Control.Concurrent
import           Control.Monad

runConc :: Int -> IO () -> IO ()
runConc n f = do
  xs <- replicateM n newEmptyMVar
  forM_ [0..n-1] $ \i -> forkIO $ f >> putMVar (xs!!i) ()
  forM_ xs takeMVar
