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
  , transToTempStreamName
  , transToViewStreamName
  )
where

import qualified Data.Aeson                   as Aeson
import qualified Data.ByteString.Lazy         as BL
import           Data.Int                     (Int64)
import qualified Data.Map.Strict              as M
import qualified Data.Map.Strict              as Map
import           Data.Maybe                   (fromJust)
import qualified Data.Text                    as T
import qualified Data.Vector                  as V
import           Proto3.Suite                 (Enumerated (..))
import qualified Proto3.Suite                 as PB
import           Z.Data.Vector                (Bytes)
import           Z.Foreign                    (toByteString)
import qualified Z.IO.Logger                  as Log

import           HStream.Processing.Connector
import           HStream.Processing.Type      as HPT
import           HStream.Server.HStreamApi    (HStreamRecordBatch (..),
                                               HStreamRecordHeader_Flag (..))
import qualified HStream.Store                as S
import           HStream.Utils

hstoreSourceConnector :: S.LDClient -> S.LDSyncCkpReader -> S.StreamType -> SourceConnector
hstoreSourceConnector ldclient reader streamType = SourceConnector {
  subscribeToStream   = \streamName ->
      subscribeToHStoreStream ldclient reader
        (S.mkStreamId streamType (textToCBytes streamName)),
  unSubscribeToStream = \streamName ->
      unSubscribeToHStoreStream ldclient reader
        (S.mkStreamId streamType (textToCBytes streamName)),
  readRecords         = readRecordsFromHStore ldclient reader 100,
  commitCheckpoint    = \streamName ->
      commitCheckpointToHStore ldclient reader
      (S.mkStreamId streamType (textToCBytes streamName))
}

hstoreSourceConnectorWithoutCkp :: S.LDClient -> S.LDReader -> SourceConnectorWithoutCkp
hstoreSourceConnectorWithoutCkp ldclient reader = SourceConnectorWithoutCkp {
  subscribeToStreamWithoutCkp = subscribeToHStoreStream' ldclient reader,
  unSubscribeToStreamWithoutCkp = unSubscribeToHStoreStream' ldclient reader,
  readRecordsWithoutCkp = readRecordsFromHStore' ldclient reader 100
}

hstoreSinkConnector :: S.LDClient -> S.StreamType -> SinkConnector
hstoreSinkConnector ldclient streamType = SinkConnector {
  writeRecord = writeRecordToHStore ldclient streamType
}

--------------------------------------------------------------------------------

transToStreamName :: HPT.StreamName -> S.StreamId
transToStreamName = S.mkStreamId S.StreamTypeStream . textToCBytes

transToTempStreamName :: HPT.StreamName -> S.StreamId
transToTempStreamName = S.mkStreamId S.StreamTypeTemp . textToCBytes

transToViewStreamName :: HPT.StreamName -> S.StreamId
transToViewStreamName = S.mkStreamId S.StreamTypeView . textToCBytes

--------------------------------------------------------------------------------

subscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> Offset -> IO ()
subscribeToHStoreStream ldclient reader streamId startOffset = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  startLSN <- case startOffset of
    Earlist    -> return S.LSN_MIN
    Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
    Offset lsn -> return lsn
  S.ckpReaderStartReading reader logId startLSN S.LSN_MAX

subscribeToHStoreStream' :: S.LDClient -> S.LDReader -> HPT.StreamName -> Offset -> IO ()
subscribeToHStoreStream' ldclient reader stream startOffset = do
  logId <- S.getUnderlyingLogId ldclient (transToStreamName stream) Nothing
  startLSN <-
        case startOffset of
          Earlist    -> return S.LSN_MIN
          Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
          Offset lsn -> return lsn
  S.readerStartReading reader logId startLSN S.LSN_MAX

unSubscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> IO ()
unSubscribeToHStoreStream ldclient reader streamId = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  S.ckpReaderStopReading reader logId

unSubscribeToHStoreStream' :: S.LDClient -> S.LDReader -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream' ldclient reader streamName = do
  logId <- S.getUnderlyingLogId ldclient (transToStreamName streamName) Nothing
  S.readerStopReading reader logId

dataRecordToSourceRecord :: S.LDClient -> Payload -> IO SourceRecord
dataRecordToSourceRecord ldclient Payload {..} = do
  streamName <- S.streamName . fst <$> S.getStreamIdFromLogId ldclient pLogID
  return SourceRecord
    { srcStream = cBytesToText streamName
      -- A dummy key typed Aeson.Object, for avoiding errors while processing
      -- queries with JOIN clause only. It is not used and will be removed in
      -- the future.
    , srcKey = Just "{}"
    , srcValue = let Right struct = PB.fromByteString . bytesToByteString $ pValue
                  in Aeson.encode . structToJsonObject $ struct
      --BL.fromStrict . toByteString $ pValue
    , srcTimestamp = pTimeStamp
    , srcOffset = pLSN
    }

readRecordsFromHStore :: S.LDClient -> S.LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  S.ckpReaderSetTimeout reader 1000   -- 1000 milliseconds
  dataRecords <- S.ckpReaderRead reader maxlen
  let payloads = concat $ map getJsonFormatRecords dataRecords
  mapM (dataRecordToSourceRecord ldclient) payloads

readRecordsFromHStore' :: S.LDClient -> S.LDReader -> Int -> IO [SourceRecord]
readRecordsFromHStore' ldclient reader maxlen = do
  S.readerSetTimeout reader 1000    -- 1000 milliseconds
  dataRecords <- S.readerRead reader maxlen
  let payloads = concat $ map getJsonFormatRecords dataRecords
  mapM (dataRecordToSourceRecord ldclient) payloads

-- Note: It actually gets 'Payload'(defined in this file)s of all JSON format DataRecords.
getJsonFormatRecords :: S.DataRecord Bytes -> [Payload]
getJsonFormatRecords dataRecord =
  map (\hsr -> let logid     = S.recordLogID dataRecord
                   payload   = getPayload hsr
                   lsn       = S.recordLSN dataRecord
                   timestamp = getTimeStamp hsr
                in Payload logid payload lsn timestamp)
  (filter (\hsr -> getPayloadFlag hsr == Enumerated (Right HStreamRecordHeader_FlagJSON)) hstreamRecords)
  where
    HStreamRecordBatch bss = decodeBatch $ S.recordPayload dataRecord
    hstreamRecords = (decodeRecord . byteStringToBytes) <$> (V.toList bss)

commitCheckpointToHStore :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> Offset -> IO ()
commitCheckpointToHStore ldclient reader streamId offset = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  case offset of
    Earlist    -> error "expect normal offset, but get Earlist"
    Latest     -> error "expect normal offset, but get Latest"
    Offset lsn -> S.writeCheckpoints reader (M.singleton logId lsn) 10{-retries-}

writeRecordToHStore :: S.LDClient -> S.StreamType -> SinkRecord -> IO ()
writeRecordToHStore ldclient streamType SinkRecord{..} = do
  let streamId = S.mkStreamId streamType (textToCBytes snkStream)
  Log.withDefaultLogger . Log.debug $ "Start writeRecordToHStore..."
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  timestamp <- getProtoTimestamp
  let header  = buildRecordHeader HStreamRecordHeader_FlagJSON Map.empty timestamp clientDefaultKey
      payload = BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust $ Aeson.decode snkValue
  let record = buildRecord header payload

  -- FIXME: do the same thing as 'appendHandler'
  let lowerPayload = encodeBatch . HStreamRecordBatch $
        encodeRecord . updateRecordTimestamp timestamp <$> (V.singleton record)
  _ <- S.appendBS ldclient logId lowerPayload Nothing
  return ()

data Payload = Payload
  { pLogID     :: S.C_LogID
  , pValue     :: Bytes
  , pLSN       :: S.LSN
  , pTimeStamp :: Int64
  } deriving (Show)
