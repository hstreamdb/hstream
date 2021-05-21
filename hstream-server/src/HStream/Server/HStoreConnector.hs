{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}


module HStream.Server.HStoreConnector
  ( hstoreSourceConnector,
    hstoreSinkConnector,
  )
where

import           HStream.Processing.Connector
import           HStream.Processing.Type          as HPT
import           HStream.Server.Utils
import           HStream.Store
import           HStream.Store.Internal.LogDevice
import           RIO
import qualified RIO.Map                          as M
import qualified Z.Data.CBytes                    as ZCB
import qualified Z.Data.JSON                      as JSON

hstoreSourceConnector :: LDClient -> LDSyncCkpReader -> SourceConnector
hstoreSourceConnector ldclient reader = SourceConnector {
  subscribeToStream = subscribeToHStoreStream ldclient reader,
  unSubscribeToStream = unSubscribeToHStoreStream ldclient reader,
  readRecords = readRecordsFromHStore ldclient reader 100,
  commitCheckpoint = commitCheckpointToHStore ldclient reader
}

hstoreSinkConnector :: LDClient -> SinkConnector
hstoreSinkConnector ldclient = SinkConnector {
  writeRecord = writeRecordToHStore ldclient
}

subscribeToHStoreStream :: LDClient -> LDSyncCkpReader -> HPT.StreamName -> Offset -> IO ()
subscribeToHStoreStream ldclient reader stream startOffset = do
  logId <- getCLogIDByStreamName ldclient (textToCBytes stream)
  startLSN <-
        case startOffset of
          Earlist    -> return 0
          Latest     -> getTailLSN ldclient logId
          Offset lsn -> return lsn
  ckpReaderStartReading reader logId startLSN LSN_MAX

unSubscribeToHStoreStream :: LDClient -> LDSyncCkpReader -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream ldclient reader streamName = do
  logId <- getCLogIDByStreamName ldclient (textToCBytes streamName)
  ckpReaderStopReading reader logId

readRecordsFromHStore :: LDClient -> LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  void $ ckpReaderSetTimeout reader 1000
  dataRecords <- ckpReaderRead reader maxlen
  mapM dataRecordToSourceRecord dataRecords
  where
    dataRecordToSourceRecord :: DataRecord -> IO SourceRecord
    dataRecordToSourceRecord DataRecord {..} = do
      logGroup <- getLogGroupByID ldclient recordLogID
      groupName <- logGroupGetName logGroup
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

commitCheckpointToHStore :: LDClient -> LDSyncCkpReader -> HPT.StreamName -> Offset -> IO ()
commitCheckpointToHStore ldclient reader streamName offset = do
  logId <- getCLogIDByStreamName ldclient (textToCBytes streamName)
  case offset of
    Earlist    -> error "expect normal offset, but get Earlist"
    Latest     -> error "expect normal offset, but get Latest"
    Offset lsn -> writeCheckpointsSync reader (M.singleton logId lsn)

writeRecordToHStore :: LDClient -> SinkRecord -> IO ()
writeRecordToHStore ldclient SinkRecord{..} = do
  logId <- getCLogIDByStreamName ldclient (textToCBytes snkStream)
  let payload =
        Payload {
          pTimestamp = snkTimestamp,
          pKey = fmap lazyByteStringToCbytes snkKey,
          pValue = lazyByteStringToCbytes snkValue
        }
  lsn <- appendSync ldclient logId (JSON.encode payload) Nothing
  return ()

data Payload = Payload {
  pTimestamp :: Timestamp,
  pKey       :: Maybe ZCB.CBytes,
  pValue     :: ZCB.CBytes
} deriving (Show, Generic, JSON.JSON)

