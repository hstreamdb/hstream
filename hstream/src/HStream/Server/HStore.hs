{-# LANGUAGE CPP                #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}

module HStream.Server.HStore
  ( hstoreSourceConnector
  , hstoreSourceConnectorWithoutCkp
  , hstoreSinkConnector
  , memorySinkConnector
  , blackholeSinkConnector
  )
where

import           Control.Concurrent               (modifyMVar)
import           Control.Concurrent.STM           (TVar)
import           Control.Exception                (catch, throwIO)
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.ByteString.Lazy             as BL
import           Data.Functor                     ((<&>))
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import           Data.IORef
import qualified Data.Map.Strict                  as M
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (catMaybes, fromJust,
                                                   mapMaybe)
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Proto3.Suite                     (Enumerated (..))
import qualified Proto3.Suite                     as PB
import           Z.Data.Vector                    (Bytes)

import           HStream.Exception                (StreamNotFound (..),
                                                   WrongOffset (..))
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as Core
import qualified HStream.Server.Core.Subscription as Core
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Shard             (cBytesToKey, hashShardKey,
                                                   shardStartKey)
import           HStream.Server.Types
import qualified HStream.Store                    as S
import           HStream.Utils

#ifdef HStreamUseV2Engine
import           HStream.Server.ConnectorTypes    as HCT
#else
import           HStream.Processing.Connector     as HCT
import           HStream.Processing.Type          as HCT
#endif

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

hstoreSourceConnectorWithoutCkp :: ServerContext -> T.Text -> TVar Bool -> SourceConnectorWithoutCkp
hstoreSourceConnectorWithoutCkp ctx consumerName consumerClosed = SourceConnectorWithoutCkp {
  subscribeToStreamWithoutCkp = subscribeToHStoreStream' ctx consumerName,
  unSubscribeToStreamWithoutCkp = unSubscribeToHStoreStream' ctx consumerName,
  isSubscribedToStreamWithoutCkp = isSubscribedToHStoreStream' ctx consumerName,
  withReadRecordsWithoutCkp = withReadRecordsFromHStore' ctx consumerName,
  connectorClosed = consumerClosed
}

hstoreSinkConnector :: ServerContext -> SinkConnector
hstoreSinkConnector ctx = SinkConnector {
  writeRecord = writeRecordToHStore ctx
}

memorySinkConnector :: IORef [SinkRecord] -> SinkConnector
memorySinkConnector ioRef = SinkConnector {
  writeRecord = writeRecordToMemory ioRef
}

blackholeSinkConnector :: SinkConnector
blackholeSinkConnector = SinkConnector {
  writeRecord = writeRecordToBlackHole
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
            , subscriptionCreationTime = Nothing
            }
  void $ Core.createSubscription ctx sub

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

isSubscribedToHStoreStream' :: ServerContext
                            -> T.Text
                            -> HCT.StreamName
                            -> IO Bool
isSubscribedToHStoreStream' ctx consumerName streamName =
  Core.checkSubscriptionExist ctx (hstoreSubscriptionPrefix <> streamName <> "_" <> consumerName)

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

receivedRecordToSourceRecord :: HCT.StreamName -> Maybe API.ReceivedRecord -> [SourceRecord]
receivedRecordToSourceRecord streamName = maybe [] convert
 where
   convert API.ReceivedRecord{..} =
    case receivedRecordRecord of
      Nothing -> []
      Just batchRecord ->
        let hRecords = decompressBatchedRecord batchRecord
            timestamp = getTimeStamp batchRecord
            -- Note: this contains two types of messages:
            -- I. the flag of the HStreamRecord is JSON, then decode the payload (as Struct) and convert it to SourceRecord
            -- II. the flag of the HStreamRecord is RAW, then try decoding the payload (as Aeson) first and convert it to
            --     SourceRecord if succeeded. Otherwise omit the message.
            -- Warning: ByteStrings start with character 'p' and 'x' will be successfully parsed to an empty Struct!
         in catMaybes . V.toList $ V.zipWith (mkSourceRecord timestamp) receivedRecordRecordIds hRecords

   mkSourceRecord :: Int64 -> API.RecordId -> API.HStreamRecord -> Maybe SourceRecord
   mkSourceRecord timestamp API.RecordId{..} hsr@API.HStreamRecord{..} =
     if getPayloadFlag hsr == Enumerated (Right API.HStreamRecordHeader_FlagJSON) then
       case PB.fromByteString hstreamRecordPayload of
         Left _       -> Nothing
         Right struct ->
           Just $ SourceRecord
           { srcStream = streamName
           , srcOffset = recordIdBatchId
           , srcTimestamp = timestamp
           , srcKey = Just "{}"
           , srcValue = Aeson.encode . structToJsonObject $ struct
           }
     else
       let lazyPayload = BL.fromStrict hstreamRecordPayload
        in case Aeson.decode lazyPayload of
             Nothing                  -> Nothing
             Just (_ :: Aeson.Object) ->
               Just $ SourceRecord
               { srcStream = streamName
               , srcOffset = recordIdBatchId
               , srcTimestamp = timestamp
               , srcKey = Just "{}"
               , srcValue = lazyPayload
               }

readRecordsFromHStore :: S.LDClient -> S.LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  S.ckpReaderSetTimeout reader 1000   -- 1000 milliseconds
  dataRecords <- S.ckpReaderRead reader maxlen
  let payloads = concatMap getJsonFormatRecords dataRecords
  mapM (dataRecordToSourceRecord ldclient) payloads

withReadRecordsFromHStore' :: ServerContext
                           -> T.Text
                           -> HCT.StreamName
                           -> (BL.ByteString -> Maybe BL.ByteString)
                           -> (BL.ByteString -> Maybe BL.ByteString)
                           -> TVar Bool
                           -> ([SourceRecord] -> IO (IO (), IO ()))
                           -> IO ()
withReadRecordsFromHStore' ctx consumerName streamName transK transV consumerClosed actionGen = do
  let req = API.StreamingFetchRequest
            { API.streamingFetchRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName <> "_" <> consumerName
            , API.streamingFetchRequestConsumerName = hstoreConsumerPrefix <> consumerName
            , API.streamingFetchRequestAckIds = V.empty
            }
  Core.streamingFetchCore ctx Core.SFetchCoreDirect req consumerClosed actionGen'
  where
    actionGen' :: Maybe API.ReceivedRecord -> IO (IO (), IO ())
    actionGen' receivedRecords = do
      let sourceRecords = receivedRecordToSourceRecord streamName receivedRecords
      let sourceRecords' = mapMaybe (\r@SourceRecord{..} ->
                                       case transV srcValue of
                                         Nothing -> Nothing
                                         Just v  -> Just $ r{ srcKey   = srcKey >>= transK
                                                            , srcValue = v
                                                            }
                                    ) sourceRecords
      actionGen sourceRecords'

-- Note: It actually gets 'Payload'(defined in this file)s of all JSON format DataRecords.
getJsonFormatRecords :: S.DataRecord Bytes -> [Payload]
getJsonFormatRecords dataRecord =
  map (\hsr -> let logid     = S.recordLogID dataRecord
                   payload   = getPayload hsr
                   lsn       = S.recordLSN dataRecord
                in Payload logid payload lsn timestamp)
  (filter (\hsr -> getPayloadFlag hsr == Enumerated (Right API.HStreamRecordHeader_FlagJSON)) hstreamRecords)
  where
    batchRecords = decodeBatchRecord $ S.recordPayload dataRecord
    timestamp = getTimeStamp batchRecords
    hstreamRecords = V.toList . decompressBatchedRecord $ batchRecords

commitCheckpointToHStore :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> Offset -> IO ()
commitCheckpointToHStore ldclient reader streamId offset = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  case offset of
    Earlist    -> throwIO $ WrongOffset "expect normal offset, but get Earlist"
    Latest     -> throwIO $ WrongOffset "expect normal offset, but get Latest"
    Offset lsn -> S.writeCheckpoints reader (M.singleton logId lsn) 10{-retries-}

writeRecordToHStore :: ServerContext
                    -> (BL.ByteString -> Maybe BL.ByteString)
                    -> (BL.ByteString -> Maybe BL.ByteString)
                    -> SinkRecord
                    -> IO ()
writeRecordToHStore ctx transK transV SinkRecord{..} = do
  Log.debug $ "Start writeRecordToHStore..."

  case transV snkValue of
    Nothing -> return () -- FIXME: error message
    Just v  -> do
      timestamp <- getProtoTimestamp
      shardId <- getShardId ctx snkStream
      let header  = buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty clientDefaultKey
          payload = BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust $ Aeson.decode v
          hsRecord = mkHStreamRecord header payload
          record = mkBatchedRecord (PB.Enumerated (Right API.CompressionTypeNone)) (Just timestamp) 1 (V.singleton hsRecord)
      void $ Core.appendStream ctx snkStream shardId record `catch` \(_:: S.NOTFOUND) -> throwIO $ StreamNotFound snkStream

writeRecordToMemory :: IORef [SinkRecord]
                    -> (BL.ByteString -> Maybe BL.ByteString)
                    -> (BL.ByteString -> Maybe BL.ByteString)
                    -> SinkRecord
                    -> IO ()
writeRecordToMemory ioRef transK transV sinkRecord = do
  atomicModifyIORef' ioRef (\xs -> (xs ++ [sinkRecord], ()))

data Payload = Payload
  { pLogID     :: S.C_LogID
  , pValue     :: Bytes
  , pLSN       :: S.LSN
  , pTimeStamp :: Int64
  } deriving (Show)

writeRecordToBlackHole :: (BL.ByteString -> Maybe BL.ByteString)
                       -> (BL.ByteString -> Maybe BL.ByteString)
                       -> SinkRecord
                       -> IO ()
writeRecordToBlackHole _ _ _ = return ()

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
         Log.debug $ "build shardDict for stream " <> Log.build sName <> ": " <> Log.buildString' (show shardDict)
         return (HM.insert sName shardDict mp, shardDict)

   insertShardDict dict shardId = do
     attrs <- S.getStreamPartitionExtraAttrs scLDClient shardId
     Log.debug $ "attrs for shard " <> Log.build shardId <> ": " <> Log.buildString' (show attrs)
     startKey <- case M.lookup shardStartKey attrs of
        -- FIXME: Under the new shard model, each partition created should have an extrAttr attribute,
        -- except for the default partition created by default for each stream. After the default
        -- partition is subsequently removed, an error ShardKeyNotFound should be returned here.
        Nothing  -> return $ cBytesToKey "0"
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
