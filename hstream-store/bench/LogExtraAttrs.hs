{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Monad
import qualified Data.Map.Strict                  as Map
import           Data.Time.Clock.System
import           Foreign.ForeignPtr               (withForeignPtr)
import qualified Z.Data.CBytes                    as CBytes
import           Z.Data.CBytes                    (CBytes)

import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as LD
import qualified HStream.Store.Logger             as S

timeit :: IO a -> IO a
timeit action = do
  start <- getSystemTime
  !a <- action
  end <- getSystemTime
  let end' = systemSeconds end * 10^9 + fromIntegral (systemNanoseconds end)
      start' = systemSeconds start * 10^9 + fromIntegral (systemNanoseconds start)
  print $ "-> " <> show (fromIntegral (end' - start') / 10^9) <> "s"
  pure a

main :: IO ()
main = do
  let configFile = "./local-data/logdevice/logdevice.conf"
      dir_path = "/tmp/bench_extra_attrs"
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  client <- S.newLDClient configFile

  --LD.syncLogsConfigVersion client =<< LD.removeLogDirectory client dir_path True
  --dir <- LD.makeLogDirectory client dir_path S.def{S.logReplicationFactor = S.defAttr1 3} True
  dir <- LD.getLogDirectory client dir_path
  attrs <- LD.logDirectoryGetAttrsPtr dir

  forM_ [1..10] $ \i -> timeit $ do
    forM_ [1..2000] $ \j -> do
      let k = CBytes.pack $ show i <> "_" <> show j
      let new_attrs = Map.singleton k k
      attrs' <- LD.updateLogAttrsExtrasPtr attrs new_attrs
      ver <- withForeignPtr attrs' $ LD.ldWriteAttributes client dir_path
      --LD.syncLogsConfigVersion client ver
      pure ()

  --LD.getAttrsExtrasFromPtr attrs
