{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Control.Exception      (SomeException, try)
import           Control.Monad
import           Control.Monad.IO.Class (liftIO)
import           Data.Int               (Int64)
import           Data.List
import qualified Data.Map.Strict        as Map
import           Data.String
import           GHC.Stack              (HasCallStack)
import           Options.Applicative
import           System.Random
import           Text.Printf
import           Z.Data.CBytes          (CBytes, pack)
import           Z.IO.Time              (SystemTime (..), getSystemTime')

import           HStream.Store          hiding (info)

logDeviceConfigPath :: CBytes
logDeviceConfigPath = "/data/store/logdevice.conf"

type Timestamp = Int64

reportInterval :: Timestamp
reportInterval = 5000

data CountWindow = CountWindow
  { windowStart        :: Timestamp
  , windowCount        :: Int64
  , windowMaxLatency   :: Int
  , windowTotalLatency :: Int64
  , windowBytes        :: Int64
  } deriving (Show)

newWindow :: IO CountWindow
newWindow = do
  start <- getCurrentTimestamp
  let window = CountWindow
       { windowStart = start
       , windowCount = 0
       , windowMaxLatency = 0
       , windowTotalLatency = 0
       , windowBytes = 0
       }
  return window

updateWindow :: CountWindow -> Int -> Int64 -> CountWindow
updateWindow win latency totalBytes
  = win { windowCount = windowCount win + 1
        , windowMaxLatency = max (windowMaxLatency win) latency
        , windowTotalLatency = windowTotalLatency win + (fromIntegral latency)
        , windowBytes = windowBytes win + totalBytes
        }

data Stats = Stats
  { start             :: Timestamp
  , latencies         :: [Int]
  , sampling          :: Int
  , iteration         :: Int
  , sampleIndex       :: Int
  , count             :: Int64
  , bytes             :: Int64
  , maxLatency        :: Int
  , totalLatency      :: Int64
  , countWindow       :: CountWindow
  , reportingInterval :: Timestamp
  } deriving (Show)

initStats :: Int64 -> IO Stats
initStats numRecords = do
  msTimeStamp <- getCurrentTimestamp
  window <- newWindow
  return $ Stats
    { start = msTimeStamp
    , iteration = 0
    , sampling = fromIntegral (numRecords `div` (min numRecords 500000))
    , latencies = []
    , sampleIndex = 0
    , count = 0
    , bytes = 0
    , maxLatency = 0
    , totalLatency = 0
    , countWindow = window
    , reportingInterval = reportInterval
    }

perfWrite :: HasCallStack => ParseArgument -> IO ()
perfWrite ParseArgument{..} = do
  ldClient <- liftIO $ newLDClient logDeviceConfigPath
  oldStats <- initStats numRecords
  let name = mkStreamName $ pack streamName
  isExist <- liftIO $ doesStreamExists ldClient name
  when isExist $ do
    putStrLn $ "-----remove exist stream----"
    removeStream ldClient name
  createStream ldClient name (LogAttrs $ HsLogAttrs 3 Map.empty)
  logId <- liftIO $ getCLogIDByStreamName ldClient name
  loop ldClient logId oldStats numRecords
  where
    stats = initStats numRecords
    loop :: LDClient -> C_LogID -> Stats -> Int64 -> IO ()
    loop client logId stats@Stats{..} n
      | n <= 0 = do
        putStrLn $ "Total: "
        printTotal stats
      | otherwise = do
         payload <- fromString <$> replicateM recordSize (randomRIO ('a', 'z'))
         startStamp <- getCurrentTimestamp
         void $ append client logId payload Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         newStats <- record stats iteration (fromIntegral latency) (fromIntegral recordSize) endStamp
         loop client logId newStats (n - 1)

record :: Stats
       -> Int -> Int -> Int64 -> Int64
       -> IO Stats
record stats iter latency newBytes time = do
  let newStats = stats
       { latencies = if (iter `rem` sample) == 0 then oldLatencies ++ [latency] else oldLatencies
       , sampleIndex = if (iter `rem` sample) == 0 then oldSampleIndex + 1 else oldSampleIndex
       , iteration = iteration stats + 1
       , count = count stats + 1
       , bytes = bytes stats + newBytes
       , maxLatency = max (maxLatency stats) latency
       , totalLatency = totalLatency stats + (fromIntegral latency)
       , countWindow = updateWindow (countWindow stats) latency newBytes
       }
  if time - (windowStart $ countWindow newStats) >= reportingInterval newStats
     then do
       printWindow $ countWindow newStats
       win <- newWindow
       let finalStats = newStats { countWindow = win }
       return finalStats
     else do
       return newStats
  where
    oldLatencies = latencies stats
    oldSampleIndex = sampleIndex stats
    sample = sampling stats

printWindow :: CountWindow -> IO ()
printWindow CountWindow{..} = do
  current <- getCurrentTimestamp
  let elapsed = current - windowStart
  putStrLn $ "\nelapsed = " ++ show elapsed ++ " windowCount = " ++ show windowCount
  let recsPerSec = 1000 * (fromIntegral windowCount :: Double) / (fromIntegral elapsed)
  let mbPerSec = 1000 * (fromIntegral windowBytes :: Double) / (fromIntegral elapsed) / (1024 * 1024)
  printf "%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1d ms max latency \n"
    windowCount recsPerSec mbPerSec ((fromIntegral windowTotalLatency :: Double) / (fromIntegral windowCount)) windowMaxLatency

printTotal :: Stats -> IO ()
printTotal Stats{..} = do
  current <- getCurrentTimestamp
  let elapsed = current - start
  putStrLn $ "elapsed = " ++ show elapsed ++ " count = " ++ show count
  let recsPerSec = 1000 * (fromIntegral count :: Double) / (fromIntegral elapsed)
  let mbPerSec = 1000 * (fromIntegral bytes :: Double) / (fromIntegral elapsed) / (1024 * 1024)
  let percs = getPercentiles latencies [0.5, 0.95, 0.99, 0.999]
  printf "%d records sent, %.2f records/sec (%.2f MB/sec), %.2f ms avg latency. \n %.2d ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th \n"
    count recsPerSec mbPerSec ((fromIntegral totalLatency :: Double) / (fromIntegral count)) maxLatency (percs !! 0) (percs !! 1) (percs !! 2) (percs !! 3)

getPercentiles :: [Int] -> [Double] -> [Int]
getPercentiles latencies percentiles =
  let sLatencies = sort latencies
      size = length sLatencies
  in [sLatencies !! (floor (p * (fromIntegral size))) | p <- percentiles ]

getCurrentTimestamp :: IO Timestamp
getCurrentTimestamp = do
  MkSystemTime sec nano <- getSystemTime'
  return $ floor (fromIntegral (sec * 10^3) + (fromIntegral nano / 10^6))

data ParseArgument = ParseArgument
  { streamName :: String
  , numRecords :: Int64
  , recordSize :: Int
  } deriving (Show)

parseConfig :: Parser ParseArgument
parseConfig =
  ParseArgument
    <$> strOption   (long "stream"      <> metavar "STRING" <> help "Produce message to this stream")
    <*> option auto (long "num-records" <> metavar "INT"    <> help "Number of messages to produce")
    <*> option auto (long "record-size" <> metavar "INT"    <> help "Message size in bytes.")

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStore-Write-Bench-Tool")
  putStrLn "HStore-Write-Bench-Tool"
  perfWrite config
