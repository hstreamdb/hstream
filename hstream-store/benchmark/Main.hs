{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (void, when)
import Control.Monad.Loops (whileM)
import qualified Control.Concurrent.MVar as MVar
import qualified Control.Concurrent.QSemN as QSemN 
import qualified Data.Map as Map
import qualified Data.Time.Clock as Clock
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import Data.Word (Word64)
import GHC.List (foldl')
import qualified HStream.Store as HStore
import qualified System.Timeout as Timeout
import Z.Data.CBytes (CBytes)
import Z.Data.Vector.Base (Bytes)

-- logdevice::Status is Errcode which is Word16

nanosSinceEpoch :: Clock.UTCTime -> Word64
nanosSinceEpoch =
  floor . (1e9 *) . Clock.nominalDiffTimeToSeconds . utcTimeToPOSIXSeconds

configPath :: CBytes
{-# INLINEABLE configPath #-}
configPath = "/data/store/logdevice.conf"

testDuration :: Int
{-# INLINE testDuration #-}
testDuration = 10000

isBuffered :: Bool
{-# INLINEABLE isBuffered #-}
isBuffered = False

payloadBuilder1k :: Bytes
{-# INLINEABLE payloadBuilder1k #-}
payloadBuilder1k = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK\n"

testRead :: HStore.LDClient -> HStore.C_LogID -> IO Bool -> IO ()
testRead !client !ld_log !timeoutIO = do
  putStrLn "This is reading test"
  start_time_fomatted <- Clock.getCurrentTime
  let start_time = nanosSinceEpoch start_time_fomatted
  let until_time = HStore.LSN_MAX
  putStrLn ("start lsn: " ++ show start_time)
  new_reader <- HStore.newLDReader client 1 Nothing
  putStrLn "Reader Created!"
  HStore.readerStartReading new_reader ld_log start_time until_time
  HStore.readerSetWaitOnlyWhenNoData new_reader
  
  receive_and_delay_list <- --TODO : count delay.
    whileM timeoutIO $ do
      records <- HStore.readerRead new_reader 100000
      let receive_and_delay = [1 :: Integer | _ <- records]
      return $ sum receive_and_delay

  let total_receive = sum receive_and_delay_list
  putStrLn $ "Total received is: " ++ show total_receive
  putStrLn "Average received latency is: TODO"

testWrite :: HStore.LDClient -> HStore.C_LogID -> IO Bool -> IO ()
testWrite _ 0 _ = return ()
testWrite !client !log !timeoutIO = do
  putStrLn "This is writing test"
  counterMVar <- MVar.newMVar (1000 :: Int)
  sem <- QSemN.newQSemN 1000
  lst <- whileM timeoutIO $ do
    QSemN.waitQSemN sem 1
    MVar.modifyMVar_ counterMVar \x->return $ x - 1
    HStore.AppendCallBackData {HStore.appendCbRetCode = st} <-
      HStore.append client log payloadBuilder1k Nothing
    let tup@(_, critical, _) :: (Int, Int, Int) =
          case st of
            1 -> (1, 1, 1)
            0 -> (0, 0, 1)
            _ -> (1, 0, 1)
    when (critical == 1) $ putStrLn "Critical Error"
    MVar.modifyMVar_ counterMVar \x->return $ x + 1
    counter <- MVar.readMVar counterMVar
    QSemN.signalQSemN sem counter
    return tup
  let folded = foldl' (
        \(!x1, !x2, !x3) (!y1, !y2, !y3) 
        -> (x1 + y1, x2 + y2, x3 + y3)
       ) (0, 0, 0) lst
  let (failures, criticals, total_commits) = folded
  putStrLn $ "TIMEOUT! total commit number is: " ++ show total_commits
  putStrLn $ "Total critical number is" ++ show criticals

main :: IO ()
main = do
  putStrLn "This is HStream-Store benchmark"
  client <- HStore.newLDClient configPath
  putStrLn "client created"
  let hs_attr = HStore.HsLogAttrs 2 Map.empty
  let attr = HStore.LogAttrs hs_attr
  putStrLn ("Log Attribute created, with replication factor of " ++ "2")
  let lo :: HStore.C_LogID = 1000
  let hi :: HStore.C_LogID = 1010
  putStrLn "log id set up"
  void $ HStore.makeLogGroupSync client "/log1" lo hi attr False
  putStrLn "ld_group created with path /log1"

  let timeout :: IO Bool =
        fmap
          ( \case
              Nothing -> False
              Just _ -> True
          )
          (Timeout.timeout testDuration $! return True)

  (void . forkIO) $ testRead client lo timeout

  (void . forkIO) $ testWrite client lo timeout

  threadDelay 5000