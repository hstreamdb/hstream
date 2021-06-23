{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}

import           Control.Monad
import           Data.Int            (Int64)
import           Data.List           (sort)
import           GHC.Stack           (HasCallStack)
import           Options.Applicative
import           System.Random
import           Text.Printf
import           Z.Data.ASCII        (c2w)
import           Z.Data.CBytes       (CBytes)
import qualified Z.Data.Vector       as ZV
import           Z.IO.Time           (SystemTime (..), getSystemTime')

import qualified HStream.Store       as HS

type Timestamp = Int64

-- | Use CountWindow to count the results in a specified time window
data CountWindow = CountWindow
  { windowElapse       :: Timestamp
  , windowCount        :: Int64
  , windowMaxLatency   :: Int
  , windowTotalLatency :: Int64
  , windowBytes        :: Int64
  } deriving (Show)

newCountWindow :: IO CountWindow
newCountWindow = do
  let window = CountWindow
       { windowElapse = 0
       , windowCount = 0
       , windowMaxLatency = 0
       , windowTotalLatency = 0
       , windowBytes = 0
       }
  return window

updateCountWindow :: CountWindow -> Int -> Int64 -> CountWindow
updateCountWindow win@CountWindow{..} latency totalBytes
  = win { windowElapse = windowElapse + fromIntegral latency
        , windowCount = windowCount + 1
        , windowMaxLatency = max windowMaxLatency latency
        , windowTotalLatency = windowTotalLatency + fromIntegral latency
        , windowBytes = windowBytes + totalBytes
        }

data WriteStats = WriteStats
  { writeElapse            :: !Timestamp
  , writeLatencies         :: ![Int]
  , writeSampling          :: !Int
  , writeIteration         :: !Int
  , writeSampleIndex       :: !Int
  , writeCount             :: !Int64
  , writeBytes             :: !Int64
  , writeMaxLatency        :: !Int
  , writeTotalLatency      :: !Int64
  , writeCountWindow       :: !CountWindow
  , writeReportingInterval :: !Timestamp
  } deriving (Show)

initWriteStats :: Timestamp -> Int64 -> IO WriteStats
initWriteStats reportInterval numRecords = do
  window <- newCountWindow
  return $ WriteStats
    { writeElapse = 0
    , writeIteration = 0
    , writeSampling = fromIntegral (numRecords `div` min numRecords 500000)
    , writeLatencies = []
    , writeSampleIndex = 0
    , writeCount = 0
    , writeBytes = 0
    , writeMaxLatency = 0
    , writeTotalLatency = 0
    , writeCountWindow = window
    , writeReportingInterval = reportInterval
    }

perfWrite :: HasCallStack => AppendOpts -> IO ()
perfWrite (AppendOpts CommonConfig{..}) = do
  ldClient <- HS.newLDClient configPath
  oldStats <- initWriteStats reportInterval numRecords
  putStrLn "-----PERF START----"
  payload <- ZV.replicateMVec recordSize (c2w <$> randomRIO ('a', 'z'))
  loop ldClient targetLogID oldStats payload numRecords
  where
    loop :: HS.LDClient -> HS.C_LogID -> WriteStats -> ZV.Bytes -> Int64 -> IO ()
    loop client logId stats@WriteStats{..} payload n
      | n <= 0 = putStrLn "Total: " >> printTotal stats
      | otherwise = do
         startStamp <- getCurrentTimestamp
         void $ HS.append client logId payload Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         newStats <- record stats writeIteration (fromIntegral latency) (fromIntegral recordSize)
         loop client logId newStats payload $! (n - 1)

perfBenchWrite :: HasCallStack => BatchOpts -> IO ()
perfBenchWrite (BatchOpts CommonConfig{..} batchSize compressionOn)  = do
  ldClient <- HS.newLDClient configPath
  oldStats <- initWriteStats reportInterval numRecords
  putStrLn "-----PERF START----"
  payloads <- replicateM batchSize $ ZV.replicateMVec recordSize (c2w <$> randomRIO ('a', 'z'))
  loop ldClient targetLogID oldStats payloads numRecords
  where
    compression = if compressionOn then HS.CompressionLZ4 else HS.CompressionNone
    loop :: HS.LDClient -> HS.C_LogID -> WriteStats -> [ZV.Bytes] -> Int64 -> IO ()
    loop client logId stats@WriteStats{..} payloads n
      | n <= 0 = putStrLn "Total: " >> printTotal stats
      | otherwise = do
         startStamp <- getCurrentTimestamp
         void $ HS.appendBatch client logId payloads compression Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         newStats <- record stats writeIteration (fromIntegral latency) (fromIntegral (recordSize * batchSize))
         loop client logId newStats payloads $! (n - 1)

record :: WriteStats
       -> Int -> Int -> Int64
       -> IO WriteStats
record stats@WriteStats{..} iter latency newBytes = do
  let newWindow = updateCountWindow writeCountWindow latency newBytes
  let newStats = stats
       { writeElapse = writeElapse + fromIntegral latency
       , writeLatencies = if (iter `rem` writeSampling) == 0 then writeLatencies ++ [latency] else writeLatencies
       , writeSampleIndex = if (iter `rem` writeSampling) == 0 then writeSampleIndex + 1 else writeSampleIndex
       , writeIteration = writeIteration + 1
       , writeCount = writeCount + 1
       , writeBytes = writeBytes + newBytes
       , writeMaxLatency = max writeMaxLatency latency
       , writeTotalLatency = writeTotalLatency + fromIntegral latency
       , writeCountWindow = newWindow
       }
  if windowElapse newWindow >= writeReportingInterval
     then do
       printWindow newWindow
       win <- newCountWindow
       return $ newStats { writeCountWindow = win }
     else do
       return newStats

printWindow :: CountWindow -> IO ()
printWindow CountWindow{..} = do
  putStrLn $ "\nelapsed = " ++ show windowElapse ++ " windowCount = " ++ show windowCount
  let recsPerSec = 1000 * (fromIntegral windowCount :: Double) / fromIntegral windowElapse
  let mbPerSec = 1000 * (fromIntegral windowBytes :: Double) / fromIntegral windowElapse / (1024 * 1024)
  printf "%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1d ms max latency \n"
    windowCount recsPerSec mbPerSec ((fromIntegral windowTotalLatency :: Double) / fromIntegral windowCount) windowMaxLatency

printTotal :: WriteStats -> IO ()
printTotal WriteStats{..} = do
  putStrLn $ "elapsed = " ++ show writeElapse ++ " count = " ++ show writeCount
  let recsPerSec = 1000 * (fromIntegral writeCount :: Double) / fromIntegral writeElapse
  let mbPerSec = 1000 * (fromIntegral writeBytes :: Double) / fromIntegral writeElapse / (1024 * 1024)
  let percs = getPercentiles writeLatencies [0.5, 0.95, 0.99, 0.999]
  printf "%d records sent, %.2f records/sec (%.2f MB/sec), %.2f ms avg latency. \n %.2d ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th \n"
    writeCount recsPerSec mbPerSec ((fromIntegral writeTotalLatency :: Double) / fromIntegral writeCount) writeMaxLatency (percs !! 0) (percs !! 1) (percs !! 2) (percs !! 3)

getPercentiles :: [Int] -> [Double] -> [Int]
getPercentiles latencies percentiles =
  let sLatencies = sort latencies
      size = length sLatencies
  in [sLatencies !! floor (p * fromIntegral size) | p <- percentiles ]

getCurrentTimestamp :: IO Timestamp
getCurrentTimestamp = do
  MkSystemTime sec nano <- getSystemTime'
  let !ts = floor @Double $ (fromIntegral sec * 1e3) + (fromIntegral nano / 1e6)
  return ts

data CommonConfig = CommonConfig
  { configPath     :: CBytes
  , targetLogID    :: HS.C_LogID
  , numRecords     :: Int64
  , recordSize     :: Int
  , reportInterval :: Int64
  } deriving (Show)

commonConfigParser :: Parser CommonConfig
commonConfigParser = CommonConfig
  <$> strOption ( long "path"
               <> metavar "PATH"
               <> showDefault
               <> value "/data/store/logdevice.conf"
               <> help "Specify the path of LogDevice configuration file."
                )
  <*> option auto ( long "log"
                 <> metavar "LOGID"
                 <> help "Specify the write target by logID"
                  )
  <*> option auto ( long "num-records"
                 <> metavar "INT"
                 <> help "Number of messages to produce"
                  )
  <*> option auto ( long "record-size"
                 <> metavar "INT"
                 <> help "Message size in bytes."
                  )
  <*> option auto ( long "interval"
                 <> metavar "INT"
                 <> showDefault
                 <> value 1000
                 <> help "Display period of statistical information in milliseconds."
                  )

newtype AppendOpts = AppendOpts { appendOpts :: CommonConfig }

appendOptsParser :: Parser AppendOpts
appendOptsParser = AppendOpts <$> commonConfigParser

data BatchOpts = BatchOpts
 { commonConfig  :: CommonConfig
 , batchSize     :: Int
 , compressionOn :: Bool
 }

batchOptsParser :: Parser BatchOpts
batchOptsParser = BatchOpts
  <$> commonConfigParser
  <*> option auto ( long "batch-size"
                 <> metavar "INT"
                 <> showDefault
                 <> value 1024
                 <> help "Number of records gather in a batch write."
                  )
  <*> switch ( long "compression"
            <> help "Turn on the compression."
             )

data WBenchCmd
  = AppendBench AppendOpts
  | AppendBatchBench BatchOpts

commandParser :: Parser WBenchCmd
commandParser = hsubparser
  ( command "append" (info (AppendBench <$> appendOptsParser) (progDesc "Basic write append bench command."))
 <> command "batch-append" (info (AppendBatchBench <$> batchOptsParser) (progDesc "Batch write append bench command."))
  )

runCommand :: WBenchCmd -> IO ()
runCommand (AppendBench opts)      = perfWrite opts
runCommand (AppendBatchBench opts) = perfBenchWrite opts

main :: IO ()
main = do
  HS.setLogDeviceDbgLevel HS.C_DBG_ERROR
  runCommand =<< customExecParser (prefs showHelpOnEmpty) opts
  where
    opts = info (helper <*> commandParser) (fullDesc <> progDesc "HStore-Write-Bench-Tool")
