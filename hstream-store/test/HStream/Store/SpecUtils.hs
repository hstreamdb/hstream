{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.SpecUtils where

import           Control.Applicative  (liftA2)
import           Data.Maybe           (fromMaybe)
import           System.IO.Unsafe     (unsafePerformIO)
import           System.Random        (newStdGen, randomRs)
import           Z.Data.CBytes        (CBytes)
import qualified Z.Data.CBytes        as CBytes
import           Z.Data.Vector        (Bytes)

import qualified HStream.Store        as S
import qualified HStream.Store.Logger as S

client :: S.LDClient
client = unsafePerformIO $ do
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  S.newLDClient "/data/store/logdevice.conf"
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

newRandomName :: Int -> IO CBytes
newRandomName n = CBytes.pack . take n . randomRs ('a', 'z') <$> newStdGen
