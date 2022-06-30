{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
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

import           Control.Concurrent
import           Control.Monad
import qualified Data.Aeson                    as Aeson
import qualified Data.ByteString.Lazy          as BL
import           Data.Int                      (Int32, Int64)
import qualified Data.Map.Strict               as M
import qualified Data.Map.Strict               as Map
import           Data.Maybe                    (fromJust, isJust)
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as T
import qualified Data.Vector                   as V
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.LowLevel.Call    (clientCallCancel)
import           Proto3.Suite                  (Enumerated (..))
import qualified Proto3.Suite                  as PB
import qualified Z.Data.Builder.Base           as ZBuilder
import           Z.Data.Vector                 (Bytes)
import           Z.Foreign                     (toByteString)
import qualified Z.IO.Logger                   as Log
import qualified Z.IO.Network                  as ZNet

import           HStream.Processing.Connector
import           HStream.Processing.Type       as HPT
import qualified HStream.Server.HStreamApi     as API
import qualified HStream.Store                 as S
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

hstoreSourceConnectorWithoutCkp :: HStreamClientApi -> T.Text -> SourceConnectorWithoutCkp
hstoreSourceConnectorWithoutCkp api consumerName = SourceConnectorWithoutCkp {
  subscribeToStreamWithoutCkp = subscribeToHStoreStream' api,
  unSubscribeToStreamWithoutCkp = unSubscribeToHStoreStream' api,
  readRecordsWithoutCkp = readRecordsFromHStore' api consumerName 100
}

hstoreSinkConnector :: HStreamClientApi -> SinkConnector
hstoreSinkConnector api = SinkConnector {
  writeRecord = writeRecordToHStore api
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

subscribeToHStoreStream' :: HStreamClientApi -> HPT.StreamName -> IO ()
subscribeToHStoreStream' API.HStreamApi{..} stream = do
  let req = API.Subscription
            { subscriptionSubscriptionId = hstoreSubscriptionPrefix <> stream
            , subscriptionStreamName = stream
            , subscriptionAckTimeoutSeconds = hstoreSubscriptionAckTimeoutSeconds
            , subscriptionMaxUnackedRecords = hstoreSubscriptionMaxUnackedRecords
            }
  void $ hstreamApiCreateSubscription (mkClientNormalRequest 1000 req)

unSubscribeToHStoreStream :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> IO ()
unSubscribeToHStoreStream ldclient reader streamId = do
  logId <- S.getUnderlyingLogId ldclient streamId Nothing
  S.ckpReaderStopReading reader logId

unSubscribeToHStoreStream' :: HStreamClientApi -> HPT.StreamName -> IO ()
unSubscribeToHStoreStream' API.HStreamApi{..} streamName = do
  let req = API.DeleteSubscriptionRequest
            { deleteSubscriptionRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName
            , deleteSubscriptionRequestForce = True
            }
  void $ hstreamApiDeleteSubscription (mkClientNormalRequest 1000 req)

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
      , srcKey = case getRecordKey hsr of
                   Nothing -> Just "{}"
                   Just k  -> Just (BL.fromStrict . T.encodeUtf8 $ k)
      , srcValue = let Right struct = PB.fromByteString pbPayload
                    in Aeson.encode . structToJsonObject $ struct
      }

readRecordsFromHStore :: S.LDClient -> S.LDSyncCkpReader -> Int -> IO [SourceRecord]
readRecordsFromHStore ldclient reader maxlen = do
  S.ckpReaderSetTimeout reader 1000   -- 1000 milliseconds
  dataRecords <- S.ckpReaderRead reader maxlen
  let payloads = concat $ map getJsonFormatRecords dataRecords
  mapM (dataRecordToSourceRecord ldclient) payloads

readRecordsFromHStore' :: HStreamClientApi -> T.Text -> Int -> HPT.StreamName -> IO [SourceRecord]
readRecordsFromHStore' api consumerName maxlen streamName = do
  let lookupSubReq = API.LookupSubscriptionRequest
                     { API.lookupSubscriptionRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName
                     }
  resp <- (API.hstreamApiLookupSubscription api) (mkClientNormalRequest 1000 lookupSubReq)
  case resp of
    ClientErrorResponse clientError -> do
      Log.withDefaultLogger . Log.warning . ZBuilder.stringUTF8 $ show clientError
      return []
    ClientNormalResponse API.LookupSubscriptionResponse{..} _meta1 _meta2 _status _details -> do
      -- FIXME: Nothing
      let (Just node) = lookupSubscriptionResponseServerNode
      runWithAddr (serverNodeToSocketAddr node) $ \API.HStreamApi{..} -> do
        receivedRecords_m <- newMVar []
        void $ hstreamApiStreamingFetch (ClientBiDiRequest 1 mempty (action receivedRecords_m))
        receivedRecords <- readMVar receivedRecords_m
        return $ receivedRecordToSourceRecord streamName <$> receivedRecords
        where
          action recv_m _clientCall _meta streamRecv streamSend _writeDone = do
            let initReq = API.StreamingFetchRequest
                          { API.streamingFetchRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName
                          , API.streamingFetchRequestConsumerName = hstoreConsumerPrefix <> consumerName
                          , API.streamingFetchRequestAckIds = V.empty
                          }
            streamSend initReq
            recving maxlen
              where
                recving leftNums
                  | leftNums <= 0 = clientCallCancel _clientCall
                  | otherwise = do
                      m_recv <- streamRecv
                      case m_recv of
                        Left err -> return ()
                        Right Nothing -> do
                          threadDelay 100000
                          recving leftNums
                        Right (Just resp@API.StreamingFetchResponse{..}) -> do
                          let recIds = V.take leftNums $ V.map fromJust $ V.filter isJust $ API.receivedRecordRecordId <$> streamingFetchResponseReceivedRecords
                          let ackReq = API.StreamingFetchRequest
                                       { API.streamingFetchRequestSubscriptionId = hstoreSubscriptionPrefix <> streamName
                                       , API.streamingFetchRequestConsumerName = hstoreConsumerPrefix <> consumerName
                                       , API.streamingFetchRequestAckIds = recIds
                                       }
                          streamSend ackReq
                          modifyMVar_ recv_m (\xs -> return $ xs ++ (V.toList . V.take leftNums $ streamingFetchResponseReceivedRecords))
                          recving (leftNums - (V.length recIds))

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

writeRecordToHStore :: HStreamClientApi -> SinkRecord -> IO ()
writeRecordToHStore api SinkRecord{..} = do
  Log.withDefaultLogger . Log.debug $ "Start writeRecordToHStore..."
  let lookupReq = API.LookupStreamRequest
                  { lookupStreamRequestStreamName = snkStream
                  , lookupStreamRequestOrderingKey = case snkKey of
                                                       Nothing -> ""
                                                       Just k  -> T.decodeUtf8 . BL.toStrict $ k
                  }
  resp <- (API.hstreamApiLookupStream api) (mkClientNormalRequest 1000 lookupReq)
  case resp of
    ClientErrorResponse clientError -> do
      Log.withDefaultLogger . Log.warning . ZBuilder.stringUTF8 $ show clientError
      return ()
    ClientNormalResponse API.LookupStreamResponse{..} _meta1 _meta2 _status _details -> do
      -- FIXME: Nothing
      let (Just node) = lookupStreamResponseServerNode
      runWithAddr (serverNodeToSocketAddr node) $ \API.HStreamApi{..} -> do
        timestamp <- getProtoTimestamp
        let header  = buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp clientDefaultKey
            payload = BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust $ Aeson.decode snkValue
        let record = buildRecord header payload
        let req = API.AppendRequest
                  { appendRequestStreamName = snkStream
                  , appendRequestRecords = V.singleton record
                  }
        void $ hstreamApiAppend (mkClientNormalRequest 1000 req)

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
