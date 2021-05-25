{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}
{-# LANGUAGE TypeApplications   #-}


module HStream.Server.HStoreConnector
  ( hstoreSourceConnector,
    hstoreSinkConnector,
  )
where

import qualified Database.MySQL.Base              as MySQL
import           HStream.Processing.Connector
import           HStream.Processing.Type          as HPT
import           HStream.Server.Utils
import           HStream.Store
import           HStream.Store.Internal.LogDevice
import           RIO
import qualified RIO.Map                          as M
import qualified RIO.ByteString.Lazy               as BL
import qualified Z.Data.CBytes                    as ZCB
import qualified Z.Data.Builder                   as ZB
import qualified Z.Data.Text                      as ZT
import qualified Z.Data.Vector                    as ZV
import qualified Z.Data.JSON                      as JSON
import           Z.IO.Exception
import qualified Z.Foreign                         as ZF

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


-- data DataRecord = DataRecord
--   { recordLogID   :: !C_LogID
--   , recordLSN     :: !LSN
--   , recordPayload :: !Bytes
--   } deriving (Show, Eq)

readRecordsFromHStore_ :: LDClient -> LDSyncCkpReader -> Int -> IO [DataRecord]
readRecordsFromHStore_ ldclient reader maxlen = do
  void $ ckpReaderSetTimeout reader 1000
  ckpReaderRead reader maxlen

readRecordsFromHStore :: LDClient -> LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  mapM dataRecordToSourceRecord =<< (readRecordsFromHStore_ ldclient reader maxlen)
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
              srcKey = fmap zTextToLazyByteString pKey,
              srcValue = zTextToLazyByteString pValue,
              srcTimestamp = pTimestamp,
              srcOffset = recordLSN
            }

-- | Read some record from logdevice and write to MySQL
--
-- It will use the loggroup name as the table name, decode the payload as JSON, write each field as column.
readRecordAndWriteToMySQL :: HasCallStack => LDClient -> LDSyncCkpReader -> Int -> MySQL.MySQLConn -> IO ()
readRecordAndWriteToMySQL ldclient reader maxlen conn = do
    rs <- readRecordsFromHStore_ ldclient reader maxlen
    forM_ rs $ \ DataRecord{..} -> do

        -- TODO, ??? This will be slow, should be included in DataRecord
        logGroup <- getLogGroupByID ldclient recordLogID
        groupName <- logGroupGetName logGroup

        payload <- unwrap "EPARSE"  (JSON.decode' recordPayload)
        value <- unwrap "EPARSE" (JSON.decodeText' @JSON.Value (pValue payload))

        MySQL.execute_ conn . MySQL.Query . BL.fromStrict . ZF.toByteString . ZB.build $ do
            "INSERT INTO "
            ZCB.toBuilder groupName
            " "
            buildFields fst value   -- ( field1, field2,...fieldN )
            " VALUES "
            buildFields snd value   -- ( value1, value2,...valueN )

  where
    buildFields f (JSON.Object obj) =
        ZB.paren $ ZB.intercalateList ZB.comma JSON.encodeJSON (map f (ZV.unpack obj))
    buildFields _ _ = "()"


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
          pKey = fmap lazyByteStringToZText snkKey,
          pValue = lazyByteStringToZText snkValue
        }
  lsn <- appendSync ldclient logId (JSON.encode payload) Nothing
  return ()

data Payload = Payload {
  pTimestamp :: Timestamp,
  pKey       :: Maybe ZT.Text,
  pValue     :: ZT.Text
} deriving (Show, Generic, JSON.JSON)

