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

import           Control.Concurrent               (modifyMVar)
import           Control.Exception                (throwIO)
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.ByteString.Lazy             as BL
import           Data.Functor                     ((<&>))
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map.Strict                  as M
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust)
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Proto3.Suite                     (Enumerated (..))
import qualified Proto3.Suite                     as PB
import           Z.Data.Vector                    (Bytes)

import qualified HStream.Logger                   as Log
import           HStream.Server.ConnectorTypes    as HCT
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception         (WrongOffset (..))
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Shard             (Shard (streamId),
                                                   cBytesToKey, hashShardKey,
                                                   shardStartKey)
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
                         -> HCT.StreamName
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
                           -> HCT.StreamName
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

receivedRecordToSourceRecord :: HCT.StreamName -> API.ReceivedRecord -> SourceRecord
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
                           -> HCT.StreamName
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
    Earlist    -> throwIO $ WrongOffset "expect normal offset, but get Earlist"
    Latest     -> throwIO $ WrongOffset "expect normal offset, but get Latest"
    Offset lsn -> S.writeCheckpoints reader (M.singleton logId lsn) 10{-retries-}

writeRecordToHStore :: ServerContext -> SinkRecord -> IO ()
writeRecordToHStore ctx SinkRecord{..} = do
  Log.withDefaultLogger . Log.debug $ "Start writeRecordToHStore..."

  timestamp <- getProtoTimestamp
  shardId <- getShardId ctx snkStream
  let header  = buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp clientDefaultKey
      payload = BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust $ Aeson.decode snkValue
  let record = buildRecord header payload
  let req = API.AppendRequest
            { appendRequestStreamName = snkStream
            , appendRequestShardId = shardId
            , appendRequestRecords = V.singleton record
            }
  void $ Core.appendStream ctx req

data Payload = Payload
  { pLogID     :: S.C_LogID
  , pValue     :: Bytes
  , pLSN       :: S.LSN
  , pTimeStamp :: Int64
  } deriving (Show)

--------------------------------------------------------------------------------

getShardId :: ServerContext -> Text -> IO S.C_LogID
getShardId ServerContext{..} sName = do
  getShardDict <&> snd . fromJust . M.lookupLE shardKey
 where
   shardKey   = hashShardKey clientDefaultKey
   streamID   = S.mkStreamId S.StreamTypeStream streamName
   streamName = textToCBytes sName

   getShardDict = modifyMVar shardTable $ \mp -> do
     case HM.lookup sName mp of
       Just shards -> return (mp, shards)
       Nothing     -> do
         -- loading shard infomation for stream first.
         shards <- M.elems <$> S.listStreamPartitions scLDClient streamID
         shardDict <- foldM insertShardDict M.empty shards
         Log.debug $ "build shardDict for stream " <> Log.buildText sName <> ": " <> Log.buildString' (show shardDict)
         return (HM.insert sName shardDict mp, shardDict)

   insertShardDict dict shardId = do
     attrs <- S.getStreamPartitionExtraAttrs scLDClient shardId
     Log.debug $ "attrs for shard " <> Log.buildInt shardId <> ": " <> Log.buildString' (show attrs)
     startKey <- case M.lookup shardStartKey attrs of
        -- FIXME: Under the new shard model, each partition created should have an extrAttr attribute,
        -- except for the default partition created by default for each stream. After the default
        -- partition is subsequently removed, an error should be returned here.
        Nothing  -> return $ cBytesToKey "0"
        -- Nothing -> throwIO $ ShardKeyNotFound shardId
        Just key -> return $ cBytesToKey key
     return $ M.insert startKey shardId dict

--------------------------------------------------------------------------------

hstoreSubscriptionPrefix :: T.Text
hstoreSubscriptionPrefix = "__hstore_subscription_"

hstoreConsumerPrefix :: T.Text
hstoreConsumerPrefix = "__hstore_consumer_"

hstoreSubscriptionAckTimeoutSeconds :: Int32
hstoreSubscriptionAckTimeoutSeconds = 600

hstoreSubscriptionMaxUnackedRecords :: Int32
hstoreSubscriptionMaxUnackedRecords = 100
