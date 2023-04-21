{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}

import           Control.Concurrent       (MVar, modifyMVar_, newMVar, readMVar,
                                           threadDelay)
import           Control.Concurrent.Async (async, cancel, forConcurrently_)
import           Control.Monad            (forM_, forever)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import           Data.Int                 (Int64)
import           GHC.Stack                (HasCallStack)
import qualified HStream.Logger           as Log
import           HStream.Store            (C_LogID, LDReader, newLDClient,
                                           newLDReader, readerReadAllowGap,
                                           readerStartReading)
import qualified HStream.Store            as HS
import qualified HStream.Store.Logger     as Log
import           HStream.Utils            (getPOSIXTime)
import           Options.Applicative
import           Z.Data.CBytes            (CBytes)

data SuccessReads = SuccessReads
  { readSize :: Int64
  , msgCount :: Int64
  } deriving (Show)

mkSuccessReads :: SuccessReads
mkSuccessReads = SuccessReads {readSize=0, msgCount=0}

recordSuccessRead :: SuccessReads -> Int64 -> Int64 -> SuccessReads
recordSuccessRead sc@SuccessReads{..} msgCnt size = sc {readSize = readSize + size, msgCount = msgCount + msgCnt}

readBench :: HasCallStack => ReadConfig -> IO ()
readBench cfg@ReadConfig{..} = do
  Log.info $ "read bench config: " <> Log.build (show cfg)
  let finalThreads = min threadCount readerCount
      logs = [from..to]
      chunkSize = length logs `div` finalThreads
      logsPerThreads = chunk chunkSize [from..to]
  successReads <- newMVar mkSuccessReads

  Log.info "------ perf start ------"
  printProc <- async $ printStats successReads (reportInterval * 1000000)
  ldClient <- newLDClient configPath
  forConcurrently_ logsPerThreads $ \logIds -> do
    reader <- newLDReader ldClient (fromIntegral . length $ logIds) (Just readBufferSize)
    doRead successReads reader logIds maxLog

  cancel printProc

doRead :: HasCallStack => MVar SuccessReads -> LDReader-> [C_LogID] -> Int -> IO ()
doRead suc reader logs maxLogs = do
  Log.info $ "reader begin to read logs: [" <> Log.build (show logs) <> "]"
  forM_ logs $ \log -> readerStartReading reader log HS.LSN_MIN HS.LSN_MAX

  forever $ do
    res <- readerReadAllowGap @ByteString reader maxLogs
    readSuccessRecords suc res
 where
   readSuccessRecords :: MVar SuccessReads -> Either HS.GapRecord [HS.DataRecord ByteString] -> IO ()
   readSuccessRecords _ (Left _gap) = do
     -- Log.info $ "reader meet gap: " <> Log.buildString (show gap)
     return ()
   readSuccessRecords sc (Right dataRecords) = do
     let size = sum $ map (BS.length . HS.recordPayload) dataRecords
         msgCnt = length dataRecords
     -- Log.info $ "reader read " <> Log.build msgCnt <> " records, total size: " <> Log.build size
     modifyMVar_ sc $ \sc' -> return $ recordSuccessRead sc' (fromIntegral msgCnt) (fromIntegral size)

printStats :: MVar SuccessReads -> Int -> IO ()
printStats readst interval = do
  Log.info "start printStats thread."
  curr <- getPOSIXTime
  printStats' 0 0 curr
 where
   printStats' lastCnt lastRead lastTmp = do
      threadDelay interval
      curr <- getPOSIXTime
      SuccessReads{..} <- readMVar readst
      let elapsed    = floor . (* 1e3) $ (curr - lastTmp)
      let messages   = (fromIntegral (msgCount - lastCnt) :: Double) * 1000 / fromIntegral elapsed
          throughput = (fromIntegral (readSize - lastRead) :: Double) * 1000 / 1024 / 1024 / fromIntegral elapsed
      Log.info $ "[Read]: " <> Log.build messages <> " record/s"
              <> ", throughput: " <> Log.build throughput <> " MB/s"
              <> ", messages: " <> Log.build (msgCount - lastCnt)
              <> ", elapsed: " <> Log.buildString' elapsed
      printStats' msgCount readSize curr

chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk n xs = go xs
 where
   go []  = []
   go !ys = let (subLs, rest) = splitAt n ys
             in subLs : go rest

data ReadConfig = ReadConfig
  { configPath     :: CBytes
  , from           :: C_LogID
  , to             :: C_LogID
  , threadCount    :: Int
  , readerCount    :: Int
  , readBufferSize :: Int64
  , maxLog         :: Int
  , reportInterval :: Int
  } deriving (Show)

parseConfig :: Parser ReadConfig
parseConfig = ReadConfig
  <$> strOption ( long "path"
               <> metavar "PATH"
               <> showDefault
               <> value "/data/store/logdevice.conf"
               <> help "Specify the path of LogDevice configuration file."
                )
  <*> option auto ( long "from"
                 <> metavar "INT"
                 <> help "Start logId."
                  )
  <*> option auto ( long "to"
                 <> metavar "INT"
                 <> help "End logId"
                  )
  <*> option auto ( long "thread-count"
                 <> metavar "INT"
                 <> showDefault
                 <> value 1
                 <> help "Number of threads to run readers."
                  )
  <*> option auto ( long "reader-count"
                 <> metavar "INT"
                 <> showDefault
                 <> value 1
                 <> help "Number of readers to subscribe stream."
                  )
  <*> option auto ( long "read-buffer-size"
                 <> metavar "INT"
                 <> showDefault
                 <> value 10
                 <> help "reader read buffer size."
                  )
  <*> option auto ( long "max-log"
                 <> metavar "INT"
                 <> showDefault
                 <> value 100
                 <> help "reader read buffer size."
                  )
  <*> option auto ( long "interval"
                 <> metavar "INT"
                 <> showDefault
                 <> value 3
                 <> help "Display period of statistical information in seconds."
                  )

newtype RBenchCmd = ReadBench ReadConfig

commandParser :: Parser RBenchCmd
commandParser = hsubparser
  ( command "readBench" (info (ReadBench <$> parseConfig) (progDesc "Read bench command.")) )

runCommand :: RBenchCmd -> IO()
runCommand (ReadBench opts) = readBench opts

main :: IO ()
main = do
  Log.setLogDeviceDbgLevel Log.C_DBG_WARNING
  runCommand =<< customExecParser (prefs showHelpOnEmpty) opts
 where
   opts = info (helper <*> commandParser) (fullDesc <> progDesc "HStore-Read-Bench-Tool")
