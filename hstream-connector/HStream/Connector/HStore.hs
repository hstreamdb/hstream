{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}

module HStream.Connector.HStore
  ( hstoreSourceConnector
  , hstoreSourceConnectorWithoutCkp
  , hstoreTempSourceConnector
  , hstoreSinkConnector
  , hstoreTempSinkConnector
  , transToStreamName
  , transToTempStreamName
  , transToViewStreamName
  )
where

import           Control.Monad                (void)
import qualified Data.ByteString.Lazy         as BL
import           Data.Int                     (Int64)
import qualified Data.Map.Strict              as M
import qualified Data.Map.Strict              as Map
import           Data.Maybe                   (fromJust, isJust)
import qualified Data.Text.Lazy               as TL
import           Z.Data.Vector                (Bytes)
import           Z.Foreign                    (toByteString)
import qualified Z.IO.Logger                  as Log

import           HStream.Processing.Connector
import           HStream.Processing.Type      as HPT
import qualified HStream.Store                as S
import           HStream.Utils

hstoreSourceConnector :: S.LDClient -> S.LDSyncCkpReader -> SourceConnector
hstoreSourceConnector ldclient reader = SourceConnector {
  subscribeToStream = subscribeToHStoreStream False ldclient reader,
  unSubscribeToStream = unSubscribeToHStoreStream False ldclient reader,
  readRecords = readRecordsFromHStore ldclient reader 100,
  commitCheckpoint = commitCheckpointToHStore False ldclient reader
}

hstoreSourceConnectorWithoutCkp :: S.LDClient -> S.LDReader -> SourceConnectorWithoutCkp
hstoreSourceConnectorWithoutCkp ldclient reader = SourceConnectorWithoutCkp {
  subscribeToStreamWithoutCkp = subscribeToHStoreStream' ldclient reader,
  unSubscribeToStreamWithoutCkp = unSubscribeToHStoreStream' ldclient reader,
  readRecordsWithoutCkp = readRecordsFromHStore' ldclient reader 100
}

hstoreTempSourceConnector :: S.LDClient -> S.LDSyncCkpReader -> SourceConnector
hstoreTempSourceConnector ldclient reader = SourceConnector {
  subscribeToStream = subscribeToHStoreStream True ldclient reader,
  unSubscribeToStream = unSubscribeToHStoreStream True ldclient reader,
  readRecords = readRecordsFromHStore ldclient reader 100,
  commitCheckpoint = commitCheckpointToHStore True ldclient reader
}

hstoreSinkConnector :: S.LDClient -> SinkConnector
hstoreSinkConnector ldclient = SinkConnector {
  writeRecord = writeRecordToHStore False ldclient
}

hstoreTempSinkConnector :: S.LDClient -> SinkConnector
hstoreTempSinkConnector ldclient = SinkConnector {
  writeRecord = writeRecordToHStore True ldclient
}

--------------------------------------------------------------------------------

transToStreamName :: HPT.StreamName -> S.StreamId
transToStreamName = S.mkStreamId S.StreamTypeStream . textToCBytes

transToTempStreamName :: HPT.StreamName -> S.StreamId
transToTempStreamName = S.mkStreamId S.StreamTypeTemp . textToCBytes

transToViewStreamName :: HPT.StreamName -> S.StreamId
transToViewStreamName = S.mkStreamId S.StreamTypeView . textToCBytes

--------------------------------------------------------------------------------

subscribeToHStoreStream :: Bool -> S.LDClient -> S.LDSyncCkpReader -> HPT.StreamName -> Offset -> IO ()
subscribeToHStoreStream isTemp ldclient reader stream startOffset = do
  logId <- case isTemp of
    True  -> S.getUnderlyingLogId ldclient (transToTempStreamName stream)
    False -> S.getUnderlyingLogId ldclient (transToStreamName stream)
  startLSN <-
        case startOffset of
          Earlist    -> return S.LSN_MIN
          Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
          Offset lsn -> return lsn
  S.ckpReaderStartReading reader logId startLSN S.LSN_MAX

subscribeToHStoreStream' :: S.LDClient -> S.LDReader -> HPT.StreamName -> Offset -> IO ()
subscribeToHStoreStream' ldclient reader stream startOffset = do
  logId <- S.getUnderlyingLogId ldclient (transToStreamName stream)
  startLSN <-
        case startOffset of
          Earlist    -> return S.LSN_MIN
          Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
          Offset lsn -> return lsn
  S.readerStartReading reader logId startLSN S.LSN_MAX

unSubscribeToHStoreStream :: Bool -> S.LDClient -> S.LDSyncCkpReader -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream isTemp ldclient reader streamName = do
  logId <- case isTemp of
    True  -> S.getUnderlyingLogId ldclient (transToTempStreamName streamName)
    False -> S.getUnderlyingLogId ldclient (transToStreamName streamName)
  S.ckpReaderStopReading reader logId

unSubscribeToHStoreStream' :: S.LDClient -> S.LDReader -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream' ldclient reader streamName = do
  logId <- S.getUnderlyingLogId ldclient (transToStreamName streamName)
  S.readerStopReading reader logId

dataRecordToSourceRecord :: S.LDClient -> Payload -> IO SourceRecord
dataRecordToSourceRecord ldclient Payload {..} = do
  logGroup <- S.getLogGroupByID ldclient pLogID
  groupName <- S.logGroupGetName logGroup
  return SourceRecord
    { srcStream = cBytesToText groupName
    , srcKey = Just "{}"
    -- A dummy key typed Aeson.Object, for avoiding errors while processing queries with JOIN clause only.
    -- It is not used and will be removed in the future.
    , srcValue = BL.fromStrict . toByteString $ pValue
    , srcTimestamp = pTimeStamp
    , srcOffset = pLSN
    }

readRecordsFromHStore :: S.LDClient -> S.LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  void $ S.ckpReaderSetTimeout reader 1000
  dataRecords <- S.ckpReaderRead reader maxlen
  let payloads = filter (isJust) $ map getJsonFormatRecord dataRecords
  mapM (dataRecordToSourceRecord ldclient . fromJust) payloads

readRecordsFromHStore' :: S.LDClient -> S.LDReader -> Int -> IO [SourceRecord]
readRecordsFromHStore' ldclient reader maxlen = do
  void $ S.readerSetTimeout reader 1000
  dataRecords <- S.readerRead reader maxlen
  let payloads = filter (isJust) $ map getJsonFormatRecord dataRecords
  mapM (dataRecordToSourceRecord ldclient . fromJust) payloads

getJsonFormatRecord :: S.DataRecord Bytes -> Maybe Payload
getJsonFormatRecord dataRecord
   | flag == jsonPayloadFlag = Just $ Payload logid payload lsn timestamp
   | otherwise               = Nothing
  where
    record    = decodeRecord $ S.recordPayload dataRecord
    flag      = getPayloadFlag record
    payload   = getPayload record
    logid     = S.recordLogID dataRecord
    lsn       = S.recordLSN dataRecord
    timestamp = getTimeStamp record

commitCheckpointToHStore :: Bool -> S.LDClient -> S.LDSyncCkpReader -> HPT.StreamName -> Offset -> IO ()
commitCheckpointToHStore isTemp ldclient reader streamName offset = do
  logId <- case isTemp of
    True  -> S.getUnderlyingLogId ldclient (transToTempStreamName streamName)
    False -> S.getUnderlyingLogId ldclient (transToStreamName streamName)
  case offset of
    Earlist    -> error "expect normal offset, but get Earlist"
    Latest     -> error "expect normal offset, but get Latest"
    Offset lsn -> S.writeCheckpoints reader (M.singleton logId lsn)

writeRecordToHStore :: Bool -> S.LDClient -> SinkRecord -> IO ()
writeRecordToHStore isTemp ldclient SinkRecord{..} = do
  Log.withDefaultLogger . Log.debug $ "Start writeRecordToHStore..."
  logId <- case isTemp of
    True  -> S.getUnderlyingLogId ldclient (transToTempStreamName snkStream)
    False -> S.getUnderlyingLogId ldclient (transToStreamName snkStream)
  timestamp <- getProtoTimestamp
  let header = buildRecordHeader jsonPayloadFlag Map.empty timestamp TL.empty
  let payload = encodeRecord $ buildRecord header snkValue
  _ <- S.append ldclient logId payload Nothing
  return ()

data Payload = Payload
  { pLogID     :: S.C_LogID
  , pValue     :: Bytes
  , pLSN       :: S.LSN
  , pTimeStamp :: Int64
  } deriving (Show)
