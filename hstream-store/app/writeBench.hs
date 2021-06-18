{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Control.Monad
import           Data.Int            (Int64)
import           Data.List
import qualified Data.Map.Strict     as Map
import           Data.String
import           GHC.Stack           (HasCallStack)
import           Options.Applicative
import           System.Random
import           Text.Printf
import           Z.Data.CBytes       (CBytes, pack)
import           Z.IO.Time           (SystemTime (..), getSystemTime')

import qualified HStream.Store       as HS

type Timestamp = Int64

-- | Use CountWindow to count the results in a specified time window
data CountWindow = CountWindow
  { windowStart        :: Timestamp
  , windowCount        :: Int64
  , windowMaxLatency   :: Int
  , windowTotalLatency :: Int64
  , windowBytes        :: Int64
  } deriving (Show)

newCountWindow :: IO CountWindow
newCountWindow = do
  start <- getCurrentTimestamp
  let window = CountWindow
       { windowStart = start
       , windowCount = 0
       , windowMaxLatency = 0
       , windowTotalLatency = 0
       , windowBytes = 0
       }
  return window

updateCountWindow :: CountWindow -> Int -> Int64 -> CountWindow
updateCountWindow win@CountWindow{..} latency totalBytes
  = win { windowCount = windowCount + 1
        , windowMaxLatency = max windowMaxLatency latency
        , windowTotalLatency = windowTotalLatency + fromIntegral latency
        , windowBytes = windowBytes + totalBytes
        }

data WriteStats = WriteStats
  { writeStart             :: !Timestamp
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
  msTimeStamp <- getCurrentTimestamp
  window <- newCountWindow
  return $ WriteStats
    { writeStart = msTimeStamp
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

perfWrite :: HasCallStack => ParseArgument -> IO ()
perfWrite ParseArgument{..} = do
  ldClient <- HS.newLDClient configPath
  HS.setLogDeviceDbgLevel HS.C_DBG_ERROR

  oldStats <- initWriteStats reportInterval numRecords
  let name = HS.mkStreamName streamName
  isExist <- HS.doesStreamExists ldClient name
  when isExist $ do
    putStrLn "-----remove exist stream----"
    HS.removeStream ldClient name
  HS.createStream ldClient name (HS.LogAttrs $ HS.HsLogAttrs 3 Map.empty)
  logId <- HS.getCLogIDByStreamName ldClient name
  putStrLn "-----PERF START----"
  loop ldClient logId oldStats numRecords
  where
    loop :: HS.LDClient -> HS.C_LogID -> WriteStats -> Int64 -> IO ()
    loop client logId stats@WriteStats{..} n
      | n <= 0 = do
        putStrLn "Total: "
        printTotal stats
      | otherwise = do
         payload <- fromString <$> replicateM recordSize (randomRIO ('a', 'z'))
         startStamp <- getCurrentTimestamp
         void $ HS.append client logId payload Nothing
         endStamp <- getCurrentTimestamp
         let latency = endStamp - startStamp
         newStats <- record stats writeIteration (fromIntegral latency) (fromIntegral recordSize) endStamp
         loop client logId newStats (n - 1)

record :: WriteStats
       -> Int -> Int -> Int64 -> Int64
       -> IO WriteStats
record stats@WriteStats{..} iter latency newBytes time = do
  let newWindow = updateCountWindow writeCountWindow latency newBytes
  let newStats = stats
       { writeLatencies = if (iter `rem` writeSampling) == 0 then writeLatencies ++ [latency] else writeLatencies
       , writeSampleIndex = if (iter `rem` writeSampling) == 0 then writeSampleIndex + 1 else writeSampleIndex
       , writeIteration = writeIteration + 1
       , writeCount = writeCount + 1
       , writeBytes = writeBytes + newBytes
       , writeMaxLatency = max writeMaxLatency latency
       , writeTotalLatency = writeTotalLatency + fromIntegral latency
       , writeCountWindow = newWindow
       }
  if time - windowStart writeCountWindow >= writeReportingInterval
     then do
       printWindow newWindow
       win <- newCountWindow
       return $ newStats { writeCountWindow = win }
     else do
       return newStats

printWindow :: CountWindow -> IO ()
printWindow CountWindow{..} = do
  current <- getCurrentTimestamp
  let elapsed = current - windowStart
  putStrLn $ "\nelapsed = " ++ show elapsed ++ " windowCount = " ++ show windowCount
  let recsPerSec = 1000 * (fromIntegral windowCount :: Double) / (fromIntegral elapsed)
  let mbPerSec = 1000 * (fromIntegral windowBytes :: Double) / (fromIntegral elapsed) / (1024 * 1024)
  printf "%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1d ms max latency \n"
    windowCount recsPerSec mbPerSec ((fromIntegral windowTotalLatency :: Double) / (fromIntegral windowCount)) windowMaxLatency

printTotal :: WriteStats -> IO ()
printTotal WriteStats{..} = do
  current <- getCurrentTimestamp
  let elapsed = current - writeStart
  putStrLn $ "elapsed = " ++ show elapsed ++ " count = " ++ show writeCount
  let recsPerSec = 1000 * (fromIntegral writeCount :: Double) / fromIntegral elapsed
  let mbPerSec = 1000 * (fromIntegral writeBytes :: Double) / fromIntegral elapsed / (1024 * 1024)
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
  return $ floor (fromIntegral (sec * 10^3) + (fromIntegral nano / 10^6))

data ParseArgument = ParseArgument
  { configPath     :: CBytes
  , streamName     :: CBytes
  , numRecords     :: Int64
  , recordSize     :: Int
  , reportInterval :: Int64
  } deriving (Show)

parseConfig :: Parser ParseArgument
parseConfig = ParseArgument
  <$> strOption ( long "path"
               <> metavar "PATH"
               <> showDefault
               <> value "/data/store/logdevice.conf"
               <> help "Specify the path of LogDevice configuration file."
                )
  <*> strOption ( long "stream"
               <> metavar "STRING"
               <> help "Produce message to this stream"
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
                 <> value 5000
                 <> help "Display period of statistical information in milliseconds."
                  )

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStore-Write-Bench-Tool")
  putStrLn "HStore-Write-Bench-Tool"
  perfWrite config
