{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.SpecUtils where

import           Control.Applicative              (liftA2)
import           Control.Exception                (bracket)
import           Control.Monad                    (void)
import           Data.Maybe                       (fromMaybe)
import           System.Environment               (lookupEnv)
import           System.IO.Unsafe                 (unsafePerformIO)
import           System.Random                    (newStdGen, randomRs)
import           Test.Hspec
import qualified Z.Data.CBytes                    as CBytes
import           Z.Data.CBytes                    (CBytes)
import           Z.Data.Vector                    (Bytes)

import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as I
import qualified HStream.Store.Logger             as S

client :: S.LDClient
client = unsafePerformIO $ do
  config <- fromMaybe "/data/store/logdevice.conf" <$> lookupEnv "TEST_LD_CONFIG"
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  S.newLDClient $ CBytes.pack config
{-# NOINLINE client #-}

readPayload :: S.C_LogID -> Maybe S.LSN -> IO Bytes
readPayload logid m_lsn = head <$> readPayload' logid m_lsn

readPayload' :: S.C_LogID -> Maybe S.LSN -> IO [Bytes]
readPayload' logid m_lsn = do
  sn <- liftA2 fromMaybe (S.getTailLSN client logid) (pure m_lsn)
  reader <- S.newLDReader client 1 Nothing
  S.readerStartReading reader logid sn sn
  xs <- S.readerRead reader 10
  return $ map S.recordPayload xs

readLSN :: S.DataRecordFormat a => S.C_LogID -> Maybe S.LSN -> IO [a]
readLSN logid m_lsn = do
  sn <- liftA2 fromMaybe (S.getTailLSN client logid) (pure m_lsn)
  reader <- S.newLDReader client 1 Nothing
  S.readerSetTimeout reader 5000
  S.readerStartReading reader logid sn sn
  go reader []
  where
    go reader acc = do
      xs <- S.readerRead reader 10
      case xs of
        []  -> return $ map S.recordPayload acc
        xs' -> go reader $ acc ++ xs'

newRandomName :: Int -> IO CBytes
newRandomName n = CBytes.pack . take n . randomRs ('a', 'z') <$> newStdGen

logdirAround :: I.LogAttributes -> SpecWith CBytes -> Spec
logdirAround attrs = aroundAll $ \runTest -> bracket setup clean runTest
  where
    setup = do
      dirname <- ("/" <>) <$> newRandomName 10
      lddir <- I.makeLogDirectory client dirname attrs False
      void $ I.syncLogsConfigVersion client =<< I.logDirectoryGetVersion lddir
      return dirname
    clean dirname =
      I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True

loggroupAround
  :: S.C_LogID
  -> CBytes
  -> S.LogAttributes
  -> SpecWith (CBytes, S.C_LogID) -> Spec
loggroupAround logid logname attrs =
  aroundAll $ \runTest -> bracket setup clean runTest
  where
    setup = do
      lg <- I.makeLogGroup client logname logid logid attrs False
      void $ I.syncLogsConfigVersion client =<< I.logGroupGetVersion lg
      return (logname, logid)
    clean (logname_, _logid) =
      I.syncLogsConfigVersion client =<< I.removeLogGroup client logname_
