{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Action
  ( Action

  , createStream
  , createStreamBySelect
  , createStreamBySelectWithCustomQueryName
  , deleteStream
  , getStream
  , listStreams
  , listShards
  , readShard
  , readStream
  , insertIntoStream
  , insertIntoStream'

  , createSubscription
  , createSubscription'
  , deleteSubscription
  , getSubscription
  , listSubscriptions

  , createConnector
  , listConnectors
  , pauseConnector
  , resumeConnector

  , listQueries
  , listViews
  , terminateQuery
  , pauseQuery
  , resumeQuery

#ifdef HStreamEnableSchema
  , registerSchema
  , getSchema
  , unregisterSchema
#endif

  , dropAction
  , lookupResource
  , describeCluster

  , executeViewQuery

  , retry
  ) where

import           Control.Concurrent               (threadDelay)
import qualified Data.ByteString                  as BS
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel           (clientCallCancel)
import           Network.GRPC.HighLevel.Generated (ClientError (..),
                                                   ClientRequest (ClientReaderRequest),
                                                   ClientResult (..),
                                                   GRPCIOError (..),
                                                   GRPCMethodType (Normal, ServerStreaming),
                                                   MetadataMap (MetadataMap),
                                                   StatusCode (..))
import qualified Proto3.Suite                     as PT
import           Proto3.Suite.Class               (def)

import           HStream.Client.Types             (Resource (..))
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData.Types    as P
import           HStream.SQL                      (DropObject (..),
                                                   InsertType (..), StreamName)
import qualified HStream.SQL                      as SQL
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils

type Action a = HStreamClientApi -> IO (ClientResult 'Normal a)

createStream :: StreamName -> Int -> Word32
  -> Action API.Stream
createStream sName rFac rDuration API.HStreamApi{..} =
  hstreamApiCreateStream (mkClientNormalRequest' def
    { API.streamStreamName        = sName
    , API.streamReplicationFactor = fromIntegral rFac
    , API.streamBacklogDuration   = rDuration
    , API.streamShardCount        = 1})

listStreams :: Action API.ListStreamsResponse
listStreams    API.HStreamApi{..} = hstreamApiListStreams clientDefaultRequest
listViews   :: Action API.ListViewsResponse
listViews      API.HStreamApi{..} = hstreamApiListViews clientDefaultRequest
listQueries :: Action API.ListQueriesResponse
listQueries    API.HStreamApi{..} = hstreamApiListQueries clientDefaultRequest
listConnectors :: Action API.ListConnectorsResponse
listConnectors API.HStreamApi{..} = hstreamApiListConnectors clientDefaultRequest
listSubscriptions :: Action API.ListSubscriptionsResponse
listSubscriptions API.HStreamApi{..} = hstreamApiListSubscriptions clientDefaultRequest

terminateQuery :: T.Text
  -> HStreamClientApi
  -> IO (ClientResult 'Normal Empty )
terminateQuery qid API.HStreamApi{..} = hstreamApiTerminateQuery
  (mkClientNormalRequest' def {API.terminateQueryRequestQueryId = qid})

dropAction :: Bool -> DropObject -> Action Empty
dropAction ignoreNonExist dropObject API.HStreamApi{..}  = do
  case dropObject of
    DStream    txt -> hstreamApiDeleteStream (mkClientNormalRequest' def
                      { API.deleteStreamRequestStreamName     = txt
                      , API.deleteStreamRequestIgnoreNonExist = ignoreNonExist
                      , API.deleteStreamRequestForce          = False
                      })

    DView      txt -> hstreamApiDeleteView (mkClientNormalRequest' def
                      { API.deleteViewRequestViewId = txt
                      , API.deleteViewRequestIgnoreNonExist = ignoreNonExist
                      })

    DConnector txt -> hstreamApiDeleteConnector (mkClientNormalRequest' def
                      { API.deleteConnectorRequestName = txt
                      -- , API.deleteConnectorRequestIgnoreNonExist = checkIfExist
                      })
    DQuery txt -> hstreamApiDeleteQuery (mkClientNormalRequest' def
                      { API.deleteQueryRequestId = txt
                      })

insertIntoStream
  :: StreamName -> Word64 -> Bool -> BS.ByteString
  -> Action API.AppendResponse
insertIntoStream sName shardId isHRecord payload =
  insertIntoStream' sName shardId isHRecord (pure payload) API.CompressionTypeNone clientDefaultKey

insertIntoStream'
  :: StreamName -> Word64 -> Bool -> V.Vector BS.ByteString -> API.CompressionType -> T.Text
  -> Action API.AppendResponse
insertIntoStream' sName shardId isHRecord payloadVec compressionType partitionKey API.HStreamApi{..} = do
  let header = if isHRecord then buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty partitionKey
                            else buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty partitionKey
      hsRecord = V.map (mkHStreamRecord header) payloadVec
      record = mkBatchedRecord (PT.Enumerated (Right compressionType)) Nothing (fromIntegral $ V.length payloadVec) hsRecord
  hstreamApiAppend (mkClientNormalRequest' def
    { API.appendRequestShardId    = shardId
    , API.appendRequestStreamName = sName
    , API.appendRequestRecords    = Just record
    })

createStreamBySelect :: String -> Action API.Query
createStreamBySelect sql api  = do
  qName <- newRandomText 10
  createStreamBySelectWithCustomQueryName sql ("cli_generated_" <> qName) api

createStreamBySelectWithCustomQueryName :: String -> T.Text -> Action API.Query
createStreamBySelectWithCustomQueryName sql qName API.HStreamApi{..} = do
  hstreamApiCreateQuery (mkClientNormalRequest' def
    { API.createQueryRequestSql = T.pack sql, API.createQueryRequestQueryName = qName })

createConnector :: T.Text -> T.Text -> T.Text -> T.Text -> Action API.Connector
createConnector name typ target cfg API.HStreamApi{..} =
  hstreamApiCreateConnector (mkClientNormalRequest' def
    { API.createConnectorRequestName = name
    , API.createConnectorRequestType = typ
    , API.createConnectorRequestTarget = target
    , API.createConnectorRequestConfig = cfg })

listShards :: T.Text -> Action API.ListShardsResponse
listShards sName API.HStreamApi{..} = do
  hstreamApiListShards $ mkClientNormalRequest' def {
    API.listShardsRequestStreamName = sName
  }

lookupResource :: Resource -> Action API.ServerNode
lookupResource (Resource rType rid) API.HStreamApi{..} = hstreamApiLookupResource $
  mkClientNormalRequest' def
    { API.lookupResourceRequestResId   = rid
    , API.lookupResourceRequestResType = PT.Enumerated $ Right rType
    }

describeCluster :: Action API.DescribeClusterResponse
describeCluster API.HStreamApi{..} = hstreamApiDescribeCluster clientDefaultRequest

pauseConnector :: T.Text -> Action Empty
pauseConnector cid API.HStreamApi{..} = hstreamApiPauseConnector $
  mkClientNormalRequest' def { API.pauseConnectorRequestName = cid }

resumeConnector :: T.Text -> Action Empty
resumeConnector cid API.HStreamApi{..} = hstreamApiResumeConnector $
  mkClientNormalRequest' def { API.resumeConnectorRequestName = cid }

pauseQuery :: T.Text -> Action Empty
pauseQuery qid API.HStreamApi{..} = hstreamApiPauseQuery $
  mkClientNormalRequest' def { API.pauseQueryRequestId = qid }

resumeQuery :: T.Text -> Action Empty
resumeQuery qid API.HStreamApi{..} = hstreamApiResumeQuery $
  mkClientNormalRequest' def { API.resumeQueryRequestId = qid }

#ifdef HStreamEnableSchema
registerSchema :: SQL.Schema -> Action Empty
registerSchema schema API.HStreamApi{..} = hstreamApiRegisterSchema $
  mkClientNormalRequest' (P.hstreamSchemaToSchema schema)

getSchema :: T.Text -> Action API.Schema
getSchema schemaOwner API.HStreamApi{..} = hstreamApiGetSchema $
  mkClientNormalRequest' def { API.getSchemaRequestOwner = schemaOwner }

unregisterSchema :: T.Text -> Action Empty
unregisterSchema schemaOwner API.HStreamApi{..} = hstreamApiUnregisterSchema $
  mkClientNormalRequest' def { API.unregisterSchemaRequestOwner = schemaOwner }
#endif

createSubscription :: T.Text -> T.Text -> Action API.Subscription
createSubscription subId sName = createSubscription' (subscriptionWithDefaultSetting subId sName)

createSubscription' :: API.Subscription -> Action API.Subscription
createSubscription' sub API.HStreamApi{..} = hstreamApiCreateSubscription $ mkClientNormalRequest' sub

deleteSubscription :: T.Text -> Bool -> Action Empty
deleteSubscription subId force API.HStreamApi{..} = hstreamApiDeleteSubscription $
  mkClientNormalRequest' def { API.deleteSubscriptionRequestSubscriptionId = subId
                             , API.deleteSubscriptionRequestForce = force}
deleteStream :: T.Text -> Bool -> Action Empty
deleteStream sName force API.HStreamApi{..} = hstreamApiDeleteStream $
  mkClientNormalRequest' def { API.deleteStreamRequestStreamName = sName
                             , API.deleteStreamRequestForce = force}

getStream :: T.Text -> Action API.GetStreamResponse
getStream sName API.HStreamApi{..} = hstreamApiGetStream $ mkClientNormalRequest' def { API.getStreamRequestName = sName }

getSubscription :: T.Text -> Action API.GetSubscriptionResponse
getSubscription sid API.HStreamApi{..} = hstreamApiGetSubscription $ mkClientNormalRequest' def { API.getSubscriptionRequestId = sid }

executeViewQuery :: String -> Action API.ExecuteViewQueryResponse
executeViewQuery sql API.HStreamApi{..} = hstreamApiExecuteViewQuery $ mkClientNormalRequest' def { API.executeViewQueryRequestSql = T.pack sql }

streamReading :: Format a => IO (Either GRPCIOError (Maybe a)) -> IO ()
streamReading recv = recv >>= \case
   Left (err :: GRPCIOError) -> errorWithoutStackTrace ("error: " <> show err)
   Right Nothing             -> pure ()  -- do `not` call cancel here
   Right (res :: Maybe a) ->
     case res of
       Nothing   -> streamReading recv
       Just res' -> (putStr . formatResult $ res') >> streamReading recv

readShard :: API.ReadShardStreamRequest -> HStreamClientApi -> IO (ClientResult 'ServerStreaming API.ReadShardStreamResponse)
readShard req API.HStreamApi{..} = hstreamApiReadShardStream $
  ClientReaderRequest req requestTimeout (MetadataMap mempty) $ \cancel _meta recv ->
    withInterrupt (clientCallCancel cancel) (streamReading recv)

readStream :: API.ReadStreamRequest -> HStreamClientApi -> IO (ClientResult 'ServerStreaming API.ReadStreamResponse)
readStream req API.HStreamApi{..} = hstreamApiReadStream $
  ClientReaderRequest req requestTimeout (MetadataMap mempty) $ \cancel _meta recv ->
    withInterrupt (clientCallCancel cancel) (streamReading recv)
--------------------------------------------------------------------------------

fakeMap :: (a -> b) -> ClientResult 'Normal a -> ClientResult 'Normal b
fakeMap f (ClientNormalResponse x _meta1 _meta2 _status _details) =
  ClientNormalResponse (f x) _meta1 _meta2 _status _details
fakeMap _ (ClientErrorResponse err) = ClientErrorResponse err

retry :: Word32 -> Word32 -> Action a -> Action a
retry n i action api = do
  res <- action api
  case res of
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusUnavailable details)) -> do
      threadDelay $ fromIntegral (i * 1000 * 1000)
      if n > 0 then retry (n - 1) i action api else return res
    _ -> return res
