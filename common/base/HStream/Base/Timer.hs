{-# LANGUAGE BangPatterns #-}

module HStream.Base.Timer
  ( CompactedWorker
  , startCompactedWorker
  , triggerCompactedWorker
  , stopCompactedWorker
  ) where

import           Control.Concurrent
import           Control.Exception  (SomeException, catch, throwIO)
import           Control.Monad      (forever, void)
import           Data.IORef

data CompactedWorker = CompactedWorker
  { compactedWorkerFlag   :: {-# UNPACK #-} !(MVar ())
  , compactedWorkerThread :: {-# UNPACK #-} !(IORef (Maybe ThreadId))
  }

-- Run "task" for each "delay" microseconds(10^-6 sec) at most once.
startCompactedWorker :: Int -> IO a -> IO CompactedWorker
startCompactedWorker delay task = do
  flag <- newEmptyMVar
  thread <- newIORef Nothing
  tid <- forkIO $ forever $ do
    catch (do threadDelay delay
              _ <- takeMVar flag
              !_ <- task
              pure ())
          (\e -> do atomicWriteIORef thread Nothing
                    throwIO (e :: SomeException))  -- let the forked thread end
  atomicWriteIORef thread $ Just tid
  pure CompactedWorker{ compactedWorkerFlag = flag
                      , compactedWorkerThread = thread
                      }

triggerCompactedWorker :: CompactedWorker -> IO ()
triggerCompactedWorker CompactedWorker{..} = void $ tryPutMVar compactedWorkerFlag ()
{-# INLINABLE triggerCompactedWorker #-}

stopCompactedWorker :: CompactedWorker -> IO ()
stopCompactedWorker CompactedWorker{..} = do
  tid <- readIORef compactedWorkerThread
  maybe (pure ()) killThread tid
