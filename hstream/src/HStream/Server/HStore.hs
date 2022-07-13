{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}

module HStream.Server.HStore
  ( hstoreSourceConnector
  , hstoreSourceConnectorWithoutCkp
  , hstoreSinkConnector
  )
where

import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.ByteString.Lazy             as BL
import           Data.Int                         (Int32, Int64)
import qualified Data.Map.Strict                  as M
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Proto3.Suite                     (Enumerated (..))
import qualified Proto3.Suite                     as PB
import           Z.Data.Vector                    (Bytes)
import qualified Z.IO.Logger                      as Log

import           HStream.Processing.Connector
import           HStream.Processing.Type          as HPT
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.Subscription as Core
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Types
import qualified HStream.Store                    as S
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

hstoreSourceConnectorWithoutCkp :: ServerContext -> T.Text -> SourceConnectorWithoutCkp
hstoreSourceConnectorWithoutCkp ctx consumerName = SourceConnectorWithoutCkp {
  subscribeToStreamWithoutCkp = subscribeToHStoreStream' ctx consumerName,
  unSubscribeToStreamWithoutCkp = unSubscribeToHStoreStream' ctx consumerName,
  withReadRecordsWithoutCkp = withReadRecordsFromHStore' ctx consumerName
}

hstoreSinkConnector :: ServerContext -> SinkConnector
hstoreSinkConnector ctx = SinkConnector {
  writeRecord = writeRecordToHStore ctx
}

--------------------------------------------------------------------------------

subscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> Offset -> IO ()
subscribeToHStoreStream ldclient reader streamId startOffset = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  startLSN <- case startOffset of
    Earlist    -> return S.LSN_MIN
    Latest     -> fmap (+1) (S.getTailLSN ldclient logId)
    Offset lsn -> return lsn
  S.ckpReaderStartReading reader logId startLSN S.LSN_MAX

subscribeToHStoreStream' :: ServerContext
                         -> T.Text
                         -> HPT.StreamName
                         -> API.SpecialOffset
                         -> IO ()
subscribeToHStoreStream' ctx consumerName streamName offset = do
  let sub = API.Subscription
            { subscriptionSubscriptionId = hstoreSubscriptionPrefix <> streamName <> "_" <> consumerName
            , subscriptionStreamName = streamName
            , subscriptionAckTimeoutSeconds = hstoreSubscriptionAckTimeoutSeconds
            , subscriptionMaxUnackedRecords = hstoreSubscriptionMaxUnackedRecords
            , subscriptionOffset = Enumerated (Right offset)
            }
  Core.createSubscription ctx sub

unSubscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> IO ()
unSubscribeToHStoreStream ldclient reader streamId = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  S.ckpReaderStopReading reader logId

unSubscribeToHStoreStream' :: ServerContext
                           -> T.Text
                           -> HPT.StreamName
                           -> IO ()
unSubscribeToHStoreStream' ctx consumerName streamName = do
  let req = API.DeleteSubscriptionRequest
            { deleteSubscriptionRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName <> "_" <> consumerName
            , deleteSubscriptionRequestForce = True
            }
  Core.deleteSubscription ctx req

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

receivedRecordToSourceRecord :: HPT.StreamName -> API.ReceivedRecord -> SourceRecord
receivedRecordToSourceRecord streamName API.ReceivedRecord{..} =
  let Just API.RecordId{..} = receivedRecordRecordId
      hsrBytes     = receivedRecordRecord in
  let (Right hsr) = PB.fromByteString hsrBytes in
  let pbPayload = API.hstreamRecordPayload hsr
   in SourceRecord
      { srcStream = streamName
      , srcOffset = recordIdBatchId
      , srcTimestamp = getTimeStamp hsr
      , srcKey = Just "{}"
      , srcValue = let Right struct = PB.fromByteString pbPayload
                    in Aeson.encode . structToJsonObject $ struct
      }

readRecordsFromHStore :: S.LDClient -> S.LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  S.ckpReaderSetTimeout reader 1000   -- 1000 milliseconds
  dataRecords <- S.ckpReaderRead reader maxlen
  let payloads = concat $ map getJsonFormatRecords dataRecords
  mapM (dataRecordToSourceRecord ldclient) payloads

withReadRecordsFromHStore' :: ServerContext
                           -> T.Text
                           -> HPT.StreamName
                           -> ([SourceRecord] -> IO ())
                           -> IO ()
withReadRecordsFromHStore' ctx consumerName streamName action = do
  let req = API.StreamingFetchRequest
            { API.streamingFetchRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName <> "_" <> consumerName
            , API.streamingFetchRequestConsumerName = hstoreConsumerPrefix <> consumerName
            , API.streamingFetchRequestAckIds = V.empty
            }
  Core.streamingFetchCore ctx Core.SFetchCoreDirect req action'
  where
    action' :: [API.ReceivedRecord] -> IO ()
    action' receivedRecords = do
      let sourceRecords = receivedRecordToSourceRecord streamName <$> receivedRecords
      action sourceRecords

-- Note: It actually gets 'Payload'(defined in this file)s of all JSON format DataRecords.
getJsonFormatRecords :: S.DataRecord Bytes -> [Payload]
getJsonFormatRecords dataRecord =
  map (\hsr -> let logid     = S.recordLogID dataRecord
                   payload   = getPayload hsr
                   lsn       = S.recordLSN dataRecord
                   timestamp = getTimeStamp hsr
                in Payload logid payload lsn timestamp)
  (filter (\hsr -> getPayloadFlag hsr == Enumerated (Right API.HStreamRecordHeader_FlagJSON)) hstreamRecords)
  where
    API.HStreamRecordBatch bss = decodeBatch $ S.recordPayload dataRecord
    hstreamRecords = (decodeRecord . byteStringToBytes) <$> (V.toList bss)

commitCheckpointToHStore :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> Offset -> IO ()
commitCheckpointToHStore ldclient reader streamId offset = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  case offset of
    Earlist    -> error "expect normal offset, but get Earlist"
    Latest     -> error "expect normal offset, but get Latest"
    Offset lsn -> S.writeCheckpoints reader (M.singleton logId lsn) 10{-retries-}

writeRecordToHStore :: ServerContext -> SinkRecord -> IO ()
writeRecordToHStore ctx SinkRecord{..} = do
  Log.withDefaultLogger . Log.debug $ "Start writeRecordToHStore..."

  timestamp <- getProtoTimestamp
  let header  = buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp clientDefaultKey
      payload = BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust $ Aeson.decode snkValue
  let record = buildRecord header payload
  let req = API.AppendRequest
            { appendRequestStreamName = snkStream
            , appendRequestRecords = V.singleton record
            }
  void $ Core.appendStream ctx req Nothing

data Payload = Payload
  { pLogID     :: S.C_LogID
  , pValue     :: Bytes
  , pLSN       :: S.LSN
  , pTimeStamp :: Int64
  } deriving (Show)

--------------------------------------------------------------------------------

hstoreSubscriptionPrefix :: T.Text
hstoreSubscriptionPrefix = "__hstore_subscription_"

hstoreConsumerPrefix :: T.Text
hstoreConsumerPrefix = "__hstore_consumer_"

hstoreSubscriptionAckTimeoutSeconds :: Int32
hstoreSubscriptionAckTimeoutSeconds = 600

hstoreSubscriptionMaxUnackedRecords :: Int32
hstoreSubscriptionMaxUnackedRecords = 100
