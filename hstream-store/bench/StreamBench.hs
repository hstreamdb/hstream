{-# LANGUAGE RecordWildCards #-}

module Main where

import           Control.Concurrent               (MVar, modifyMVar_, newMVar,
                                                   readMVar)
import           Control.Monad                    (replicateM_, unless)
import qualified Criterion.Main                   as C
import qualified Criterion.Main.Options           as C
import           Data.HashMap.Strict              (HashMap)
import qualified Data.HashMap.Strict              as HMap
import qualified Options.Applicative              as O
import           System.IO.Unsafe                 (unsafePerformIO)
import           Z.Data.CBytes                    (CBytes)

import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Logger             as S

-- | To run the benchmark:
--
-- > cabal run -- hstore-bench-stream --config /data/store/logdevice.conf --stream test --output bench.html --regress allocated:iters +RTS -T
main :: IO ()
main = do
  BenchOpts{..} <- O.execParser $ C.describeWith benchParser
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  client <- S.newLDClient configFile
  let streamid = S.mkStreamId S.StreamTypeStream streamName
  logid <- getLogId client streamid
  runBench mode logid client streamid

data BenchOpts = BenchOpts
  { configFile :: CBytes
  , streamName :: CBytes
  , mode       :: C.Mode
  }

benchParser :: O.Parser BenchOpts
benchParser = BenchOpts
  <$> O.strOption ( O.long "config"
                 <> O.metavar "STRING"
                 <> O.help "path to the client config file"
                  )
  <*> O.strOption ( O.long "stream"
                 <> O.metavar "STRING"
                 <> O.help "the stream name to test"
                  )
  <*> C.parseWith C.defaultConfig

runBench :: C.Mode -> S.C_LogID -> S.LDClient -> S.StreamId -> IO ()
runBench mode expected client streamid = do
  C.runMode mode
    [ C.bgroup "UnderlyingLogId" $ benches 100
    ]
    where
      benches :: Int -> [C.Benchmark]
      benches n =
          [ C.bench "byLdClient" $ C.nfIO (runGetLogId n expected $ getLogId client streamid)
          , C.bench "byMemCache" $ C.nfIO (runGetLogId n expected $ getLogId' client streamid)
          ]

runGetLogId :: Int -> S.C_LogID -> IO S.C_LogID -> IO ()
runGetLogId n expected f = replicateM_ n $ do
  logid <- f
  unless (logid == expected) $ error "Wrong result!"

getLogId :: S.LDClient -> S.StreamId -> IO S.C_LogID
getLogId client streamid = do
  (log_path, _key) <- S.getStreamLogPath streamid Nothing
  fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client log_path)
{-# INLINABLE getLogId #-}

gloCache :: MVar (HashMap S.StreamId S.C_LogID)
gloCache = unsafePerformIO $ newMVar HMap.empty
{-# NOINLINE gloCache #-}

getLogId' :: S.LDClient -> S.StreamId -> IO S.C_LogID
getLogId' client streamid = do
  (log_path, _key) <- S.getStreamLogPath streamid Nothing
  m_logid <- HMap.lookup streamid <$> (readMVar gloCache)
  case m_logid of
    Nothing -> do
      logid <- fst <$> (LD.logGroupGetRange =<< LD.getLogGroup client log_path)
      modifyMVar_ gloCache $ pure . HMap.insert streamid logid
      pure logid
    Just ld -> pure ld
{-# INLINABLE getLogId' #-}
