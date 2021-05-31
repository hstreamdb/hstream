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
testDuration = 1000000

isBuffered :: Bool
{-# INLINEABLE isBuffered #-}
isBuffered = False

payloadBuilder1k :: Bytes
{-# INLINEABLE payloadBuilder1k #-}
payloadBuilder1k = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK\n"

testRead :: HStore.LDClient -> HStore.C_LogID -> IO ()
testRead !client !ld_log = do
  putStrLn "This is reading test"
  start_time_fomatted <- Clock.getCurrentTime
  let start_time = nanosSinceEpoch start_time_fomatted
  let until_time = HStore.LSN_MAX
  putStrLn ("start lsn: " ++ show start_time)
  new_reader <- HStore.newLDReader client 1 Nothing
  putStrLn "Reader Created!"
  HStore.readerStartReading new_reader ld_log start_time until_time
  HStore.readerSetWaitOnlyWhenNoData new_reader

  receiveMVar <- MVar.newMVar (0 :: Int)
                                                  --TODO : count delay.
  Timeout.timeout testDuration $ whileM (return True) $ do
    putStrLn "what happened"
    records <- HStore.readerRead new_reader 100000
    putStrLn "add one "
    MVar.modifyMVar_ receiveMVar \x->return $ x + 1
  
  receives <- MVar.readMVar receiveMVar

  putStrLn $ "Total received is: " ++ show receives
  putStrLn "Average received latency is: TODO"

testWrite :: HStore.LDClient -> HStore.C_LogID -> IO ()
testWrite _ 0 = return ()
testWrite !client !log = do
  putStrLn "This is writing test"
  counterMVar <- MVar.newMVar (1000 :: Int)
  putStrLn "counter created"
  sem <- QSemN.newQSemN 1000
  putStrLn "semaphore created"
  failures <-  MVar.newMVar (0 :: Int)
  criticals <- MVar.newMVar (0 :: Int)
  total_commits <- MVar.newMVar (0 :: Int)
  Timeout.timeout testDuration $ whileM (return True) $ do
    QSemN.waitQSemN sem 1
    MVar.modifyMVar_ counterMVar \x->return $ x - 1
    HStore.AppendCallBackData {HStore.appendCbRetCode = st} <-
      HStore.append client log payloadBuilder1k Nothing
    case st of
      1 -> do 
        MVar.modifyMVar_ failures \x->return $ x + 1
        MVar.modifyMVar_ criticals \x->return $ x + 1
        MVar.modifyMVar_ total_commits \x->return $ x + 1
      0 -> do 
        MVar.modifyMVar_ total_commits \x->return $ x + 1
      _ -> do 
        MVar.modifyMVar_ failures \x->return $ x + 1
        MVar.modifyMVar_ total_commits \x->return $ x + 1
    return ()
    getCritical <- MVar.readMVar criticals
    when (getCritical == 1) $ putStrLn "Critical Error"
    MVar.modifyMVar_ counterMVar \x->return $ x + 1
    counter <- MVar.readMVar counterMVar
    QSemN.signalQSemN sem counter
  fails <- MVar.readMVar failures
  critics <- MVar.readMVar criticals
  commits <- MVar.readMVar total_commits
  putStrLn $ "TIMEOUT! total commit number is: " ++ show commits
  putStrLn $ "Total critical number is " ++ show critics

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
  --void $ HStore.makeLogGroupSync client "/log1" lo hi attr False
  putStrLn "ld_group created with path /log1"

  let timeout :: IO Bool =
        fmap
          ( \case
              Nothing -> True
              Just _ -> False
          )
          (Timeout.timeout testDuration $ (threadDelay 200))

  (void . forkIO) $ testWrite client lo
  (void . forkIO) $ testRead client lo
  --let run =  void $ whileM (return True) $ putStrLn "I am alive
  --(void . forkIO) $ do 
--Timeout.timeout testDuration run
--putStrLn "done!"

  threadDelay 2000000