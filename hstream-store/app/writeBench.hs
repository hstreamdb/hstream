{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}

import           Control.Concurrent
import           Control.Concurrent.Async (forConcurrently_)
import           Control.Monad
import           Data.Int                 (Int64)
import           Data.List                (sort)
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import           Data.Maybe               (fromJust)
import           GHC.Stack                (HasCallStack)
import           Options.Applicative
import           System.Random
import           Text.Printf
import           Z.Data.ASCII             (c2w)
import           Z.Data.CBytes            (CBytes)
import qualified Z.Data.Parser            as P
import qualified Z.Data.Vector            as ZV
import           Z.IO.Time                (SystemTime (..), getSystemTime')

import qualified HStream.Store            as HS

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

mergeStats :: WriteStats -> WriteStats -> WriteStats
mergeStats stats acc = acc
  { writeElapse = writeElapse acc + writeElapse stats
  , writeBytes = writeBytes acc + writeBytes stats
  , writeCount = writeCount acc + writeCount stats
  , writeTotalLatency = writeTotalLatency acc + writeTotalLatency stats
  , writeLatencies = writeLatencies acc ++ writeLatencies stats
  , writeMaxLatency = max (writeMaxLatency acc) (writeMaxLatency stats)
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
    loop client logId stats payload n
      | n <= 0 = putStrLn ("\nWrite to " ++ show logId ++ " total: ") >> printTotal stats
      | otherwise = do
         startStamp <- getCurrentTimestamp
         void $ HS.append client logId payload Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         newStats <- record stats logId (fromIntegral latency) (fromIntegral recordSize)
         loop client logId newStats payload $! (n - 1)

perfBenchWrite :: HasCallStack => BatchOpts -> IO ()
perfBenchWrite (BatchOpts CommonConfig{..} BatchConfig{..})  = do
  ldClient <- HS.newLDClient configPath
  oldStats <- initWriteStats reportInterval numRecords
  putStrLn "-----PERF START----"
  payloads <- replicateM batchSize $ ZV.replicateMVec recordSize (c2w <$> randomRIO ('a', 'z'))
  loop ldClient targetLogID oldStats payloads numRecords
  where
    compression = if compressionOn then HS.CompressionLZ4 else HS.CompressionNone
    loop :: HS.LDClient -> HS.C_LogID -> WriteStats -> [ZV.Bytes] -> Int64 -> IO ()
    loop client logId stats payloads n
      | n <= 0 = putStrLn ("\nWrite to " ++ show logId ++ " total: ") >> printTotal stats
      | otherwise = do
         startStamp <- getCurrentTimestamp
         void $ HS.appendBatch client logId payloads compression Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         newStats <- record stats logId (fromIntegral latency) (fromIntegral (recordSize * batchSize))
         loop client logId newStats payloads $! (n - 1)

type StatsMap = Map HS.C_LogID (MVar WriteStats)

parallelBench :: HasCallStack => ParallelOpts -> IO ()
parallelBench ParallelOpts{..} = do
  ldClient <- HS.newLDClient pConfigPath
  oldStats <- initWriteStats pReportInterval pNumRecords
  payloads <- replicateM pbatchSize $ ZV.replicateMVec pRecordSize (c2w <$> randomRIO ('a', 'z'))
  ls <- forM logs $ \(logId, _) -> do
    s <- newMVar oldStats
    return (logId, s)
  let mp = Map.fromList ls
  putStrLn "-----PERF START----"
  forConcurrently_ targets $ \logId -> do
    process ldClient mp payloads pNumRecords logId
  printSingleLogStats mp
  printTotalLogStats mp
  where
    targets = concatMap (\(logId, n) -> replicate n logId) logs
    BatchConfig pbatchSize pcompression = pBatchConfig
    compression = if pcompression then HS.CompressionLZ4 else HS.CompressionNone

    process :: HS.LDClient -> StatsMap -> [ZV.Bytes] -> Int64 -> HS.C_LogID -> IO ()
    process client mp payloads n logId
      | n <= 0 = return ()
      | otherwise = do
         startStamp <- getCurrentTimestamp
         void $ HS.appendBatch client logId payloads compression Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         updateStats mp logId latency
         process client mp payloads (n - 1) logId

    updateStats :: StatsMap -> HS.C_LogID -> Int64 -> IO ()
    updateStats mp logId latency = do
      let mvar = fromJust $ Map.lookup logId mp
      stats <- takeMVar mvar
      newStats <- record stats logId (fromIntegral latency) (fromIntegral (pRecordSize * pbatchSize))
      putMVar mvar newStats

    printSingleLogStats :: StatsMap -> IO ()
    printSingleLogStats mp = do
      forM_ (Map.toList mp) $ \(logId, s) -> do
        stats <- readMVar s
        putStrLn ("\n----------Write to " ++ show logId ++ " total: ") >> printTotal stats >> putStrLn "----------"

    printTotalLogStats :: StatsMap -> IO ()
    printTotalLogStats mp = do
      basicStats <- initWriteStats pReportInterval pNumRecords
      sStats <- forM (Map.elems mp) $ \s -> do stats <- readMVar s
                                               return stats
      let res = foldr mergeStats basicStats sStats
      let totalStats = res { writeElapse = writeElapse res `div` fromIntegral (length sStats) }
      putStrLn "\n============================\nWrite complete: " >>
        printTotal totalStats >> putStrLn "============================"

record :: WriteStats
       -> HS.C_LogID -> Int -> Int64
       -> IO WriteStats
record stats@WriteStats{..} logId latency newBytes = do
  let newWindow = updateCountWindow writeCountWindow latency newBytes
  let newStats = stats
       { writeElapse = writeElapse + fromIntegral latency
       , writeLatencies = if (writeIteration `rem` writeSampling) == 0 then writeLatencies ++ [latency] else writeLatencies
       , writeSampleIndex = if (writeIteration `rem` writeSampling) == 0 then writeSampleIndex + 1 else writeSampleIndex
       , writeIteration = writeIteration + 1
       , writeCount = writeCount + 1
       , writeBytes = writeBytes + newBytes
       , writeMaxLatency = max writeMaxLatency latency
       , writeTotalLatency = writeTotalLatency + fromIntegral latency
       , writeCountWindow = newWindow
       }
  if windowElapse newWindow >= writeReportingInterval
     then do
       printWindow newWindow logId
       win <- newCountWindow
       return $ newStats { writeCountWindow = win }
     else do
       return newStats

printWindow :: CountWindow -> HS.C_LogID -> IO ()
printWindow CountWindow{..} logId = do
  putStrLn $ "\nwrite to " ++ show logId ++ ", elapsed = " ++ show windowElapse ++ " windowCount = " ++ show windowCount
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
  printf "total write = %.2f MB\n" $ (fromIntegral writeBytes :: Double) / (1024 * 1024)
  printf ("%d records sent, %.2f records/sec (%.2f MB/sec), %.2f ms avg latency. \n %.2d ms max latency,"
       <> "%d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th \n")
    writeCount recsPerSec mbPerSec ((fromIntegral writeTotalLatency :: Double) / fromIntegral writeCount)
    writeMaxLatency (percs !! 0) (percs !! 1) (percs !! 2) (percs !! 3)

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

newtype AppendOpts = AppendOpts CommonConfig

appendOptsParser :: Parser AppendOpts
appendOptsParser = AppendOpts <$> commonConfigParser

data BatchConfig = BatchConfig
  { batchSize     :: Int
  , compressionOn :: Bool
  } deriving (Show)

batchConfigParser :: Parser BatchConfig
batchConfigParser = BatchConfig
  <$> option auto ( long "batch-size"
                 <> metavar "INT"
                 <> showDefault
                 <> value 1024
                 <> help "Number of records gather in a batch write."
                  )
  <*> switch ( long "compression"
            <> help "Turn on the compression."
             )

data BatchOpts = BatchOpts
 { commonConfig :: CommonConfig
 , batchConfig  :: BatchConfig
 } deriving (Show)

batchOptsParser :: Parser BatchOpts
batchOptsParser = BatchOpts
  <$> commonConfigParser
  <*> batchConfigParser

type WriteUnit = (HS.C_LogID, Int)

data ParallelOpts = ParallelOpts
  { pConfigPath     :: CBytes
  , logs            :: [WriteUnit]
  , pNumRecords     :: Int64
  , pRecordSize     :: Int
  , pReportInterval :: Int64
  , pBatchConfig    :: BatchConfig
  } deriving (Show)

parseWriteUnit :: ReadM (HS.C_LogID, Int)
parseWriteUnit = eitherReader $ parse . ZV.packASCII
  where
    parse :: ZV.Bytes -> Either String (HS.C_LogID, Int)
    parse bs =
      case P.parse' parser bs of
        Left er -> Left $ "cannot parse value: " <> show er
        Right i -> Right i
    parser = do
      P.skipSpaces
      logId <- P.int
      res <- P.peekMaybe
      case res of
        Nothing -> return (logId, 1)
        Just _ -> do
          P.char8 ':'
          num <- P.int
          P.skipSpaces
          return (logId, num)

parallelOptsParser :: Parser ParallelOpts
parallelOptsParser = ParallelOpts
  <$> strOption ( long "path"
               <> metavar "PATH"
               <> showDefault
               <> value "/data/store/logdevice.conf"
               <> help "Specify the path of LogDevice configuration file."
                )
  <*> many (option parseWriteUnit ( long "logs"
                                 <> metavar "LOGID:INT"
                                 <> help ( "Write messages to the specified stream, followed by a number to specify the "
                                        <> "number of threads using to write concurrently.")
                                  ))
  <*> option auto ( long "num-records"
                 <> metavar "INT"
                 <> help "Number of messages write to each log."
                  )
  <*> option auto ( long "record-size"
                 <> metavar "INT"
                 <> help "Single message size in bytes."
                  )
  <*> option auto ( long "interval"
                 <> metavar "INT"
                 <> showDefault
                 <> value 1000
                 <> help "Display period of statistical information in milliseconds."
                  )
  <*> batchConfigParser

data WBenchCmd
  = AppendBench AppendOpts
  | AppendBatchBench BatchOpts
  | ParallelBench ParallelOpts

commandParser :: Parser WBenchCmd
commandParser = hsubparser
  ( command "append" (info (AppendBench <$> appendOptsParser) (progDesc "Basic write append bench command."))
 <> command "batch-append" (info (AppendBatchBench <$> batchOptsParser) (progDesc "Batch write append bench command."))
 <> command "parallel-append" (info (ParallelBench <$> parallelOptsParser) (progDesc "Bench write to multi-logs."))
  )

runCommand :: WBenchCmd -> IO ()
runCommand (AppendBench opts)      = perfWrite opts
runCommand (AppendBatchBench opts) = perfBenchWrite opts
runCommand (ParallelBench opts)    = parallelBench opts

main :: IO ()
main = do
  HS.setLogDeviceDbgLevel HS.C_DBG_ERROR
  runCommand =<< customExecParser (prefs showHelpOnEmpty) opts
  where
    opts = info (helper <*> commandParser) (fullDesc <> progDesc "HStore-Write-Bench-Tool")
