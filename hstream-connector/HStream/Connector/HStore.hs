{-# LANGUAGE BangPatterns       #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}

module HStream.Connector.HStore
  ( hstoreSourceConnector
  , hstoreSourceConnectorWithoutCkp
  , hstoreSinkConnector
  , transToStreamName
  )
where

import           Control.Monad                (void)
import qualified Data.Map.Strict              as M
import           GHC.Generics                 (Generic)
import qualified Z.Data.Builder               as B
import qualified Z.Data.CBytes                as ZCB
import qualified Z.Data.JSON                  as JSON
import           Z.Data.Text                  (validate)
import qualified Z.IO.Logger                  as Log

import           HStream.Processing.Connector
import           HStream.Processing.Type      as HPT
import qualified HStream.Store                as S
import           HStream.Utils

hstoreSourceConnector :: S.LDClient -> S.LDSyncCkpReader -> SourceConnector
hstoreSourceConnector ldclient reader = SourceConnector {
  subscribeToStream = subscribeToHStoreStream ldclient reader,
  unSubscribeToStream = unSubscribeToHStoreStream ldclient reader,
  readRecords = readRecordsFromHStore ldclient reader 100,
  commitCheckpoint = commitCheckpointToHStore ldclient reader
}

hstoreSourceConnectorWithoutCkp :: S.LDClient -> S.LDReader -> SourceConnectorWithoutCkp
hstoreSourceConnectorWithoutCkp ldclient reader = SourceConnectorWithoutCkp {
  subscribeToStreamWithoutCkp = subscribeToHStoreStream' ldclient reader,
  unSubscribeToStreamWithoutCkp = unSubscribeToHStoreStream' ldclient reader,
  readRecordsWithoutCkp = readRecordsFromHStore' ldclient reader 100
}

hstoreSinkConnector :: S.LDClient -> SinkConnector
hstoreSinkConnector ldclient = SinkConnector {
  writeRecord = writeRecordToHStore ldclient
}

transToStreamName :: HPT.StreamName -> S.StreamName
transToStreamName = S.mkStreamName . textToCBytes

subscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> HPT.StreamName -> Offset -> IO ()
subscribeToHStoreStream ldclient reader stream startOffset = do
  logId <- S.getCLogIDByStreamName ldclient (transToStreamName stream)
  startLSN <-
        case startOffset of
          Earlist    -> return S.LSN_MIN
          Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
          Offset lsn -> return lsn
  S.ckpReaderStartReading reader logId startLSN S.LSN_MAX

subscribeToHStoreStream' :: S.LDClient -> S.LDReader -> HPT.StreamName -> Offset -> IO ()
subscribeToHStoreStream' ldclient reader stream startOffset = do
  logId <- S.getCLogIDByStreamName ldclient (transToStreamName stream)
  startLSN <-
        case startOffset of
          Earlist    -> return S.LSN_MIN
          Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
          Offset lsn -> return lsn
  S.readerStartReading reader logId startLSN S.LSN_MAX

unSubscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream ldclient reader streamName =
  S.stopCkpReader ldclient reader (transToStreamName streamName)

unSubscribeToHStoreStream' :: S.LDClient -> S.LDReader -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream' ldclient reader streamName =
  S.stopReader ldclient reader (transToStreamName streamName)

dataRecordToSourceRecord :: S.LDClient -> S.DataRecord -> IO SourceRecord
dataRecordToSourceRecord ldclient S.DataRecord {..} = do
  logGroup <- S.getLogGroupByID ldclient recordLogID
  groupName <- S.logGroupGetName logGroup
  case JSON.decode' recordPayload of
    Left _ -> error "payload decode error!"
    Right Payload {..} ->
      return
        SourceRecord {
          srcStream = cbytesToText groupName,
          srcKey = fmap cbytesToLazyByteString pKey,
          srcValue = cbytesToLazyByteString pValue,
          srcTimestamp = pTimestamp,
          srcOffset = recordLSN
        }

readRecordsFromHStore :: S.LDClient -> S.LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  void $ S.ckpReaderSetTimeout reader 1000
  dataRecords <- S.ckpReaderRead reader maxlen
  mapM (dataRecordToSourceRecord ldclient) dataRecords

readRecordsFromHStore' :: S.LDClient -> S.LDReader -> Int -> IO [SourceRecord]
readRecordsFromHStore' ldclient reader maxlen = do
  void $ S.readerSetTimeout reader 1000
  dataRecords <- S.readerRead reader maxlen
  mapM (dataRecordToSourceRecord ldclient) dataRecords

commitCheckpointToHStore :: S.LDClient -> S.LDSyncCkpReader -> HPT.StreamName -> Offset -> IO ()
commitCheckpointToHStore ldclient reader streamName offset = do
  logId <- S.getCLogIDByStreamName ldclient (transToStreamName streamName)
  case offset of
    Earlist    -> error "expect normal offset, but get Earlist"
    Latest     -> error "expect normal offset, but get Latest"
    Offset lsn -> S.writeCheckpoints reader (M.singleton logId lsn)

writeRecordToHStore :: S.LDClient -> SinkRecord -> IO ()
writeRecordToHStore ldclient SinkRecord{..} = do
  putStrLn "Start writeRecordToHStore..."
  logId <- S.getCLogIDByStreamName ldclient (transToStreamName snkStream)
  let payload =
        Payload {
          pTimestamp = snkTimestamp,
          pKey = fmap lazyByteStringToCBytes snkKey,
          pValue = lazyByteStringToCBytes snkValue
        }
  -- FIXME: for some unknown reasons, github action will exit failure without
  -- any information out if we evaluate the payload. So we here always print the
  -- payload.
  putStrLn $ "DEBUG: payload " <> show payload
  let !_testText = validate "hello, world"
  putStrLn "validate done"
  let bin_payload = JSON.encode payload
  Log.withDefaultLogger . Log.debug $ "bin payload: " <> B.bytes bin_payload
  _ <- S.append ldclient logId bin_payload Nothing
  return ()

data Payload = Payload {
  pTimestamp :: Timestamp,
  pKey       :: Maybe ZCB.CBytes,
  pValue     :: ZCB.CBytes
} deriving (Show, Generic, JSON.JSON)
