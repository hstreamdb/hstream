{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Action
  ( createStream
  , listStreams
  , listViews
  , listQueries
  , listConnectors
  , terminateQueries
  , dropAction
  , insertIntoStream
  , createStreamBySelect
  , runActionWithAddr
  , Action
  ) where

import           Control.Monad                    ((>=>))
import qualified Data.ByteString                  as BS
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientResult (..),
                                                   GRPCMethodType (Normal),
                                                   withGRPCClient)
import           Proto3.Suite.Class               (def)
import           Z.IO.Network                     (SocketAddr)

import           HStream.Client.Utils
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL.Codegen              (DropObject (..),
                                                   InsertType (..), StreamName,
                                                   TerminationSelection (..))
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils

createStream :: StreamName -> Int
  -> Action API.Stream
createStream sName rFac API.HStreamApi{..} =
  hstreamApiCreateStream (mkClientNormalRequest' def
    { API.streamStreamName        = sName
    , API.streamReplicationFactor = fromIntegral rFac
    , API.streamShardCount        = 1})

listStreams :: Action API.ListStreamsResponse
listStreams    API.HStreamApi{..} = hstreamApiListStreams clientDefaultRequest
listViews   :: Action API.ListViewsResponse
listViews      API.HStreamApi{..} = hstreamApiListViews clientDefaultRequest
listQueries :: Action API.ListQueriesResponse
listQueries    API.HStreamApi{..} = hstreamApiListQueries clientDefaultRequest
listConnectors :: Action API.ListConnectorsResponse
listConnectors API.HStreamApi{..} = hstreamApiListConnectors clientDefaultRequest

terminateQueries :: TerminationSelection
  -> HStreamClientApi
  -> IO (ClientResult 'Normal API.TerminateQueriesResponse )
terminateQueries (OneQuery qid) API.HStreamApi{..} =
  hstreamApiTerminateQueries
    (mkClientNormalRequest' def{API.terminateQueriesRequestQueryId = V.singleton $ cBytesToText qid})
terminateQueries AllQueries API.HStreamApi{..} =
  hstreamApiTerminateQueries
    (mkClientNormalRequest' def{API.terminateQueriesRequestAll = True})
terminateQueries (ManyQueries qids) API.HStreamApi{..} =
  hstreamApiTerminateQueries
    (mkClientNormalRequest'
      def {API.terminateQueriesRequestQueryId = V.fromList $ cBytesToText <$> qids})

dropAction :: Bool -> DropObject -> Action Empty
dropAction ignoreNonExist dropObject API.HStreamApi{..}  = do
  case dropObject of
    DStream    txt -> hstreamApiDeleteStream (mkClientNormalRequest' def
                      { API.deleteStreamRequestStreamName     = txt
                      , API.deleteStreamRequestIgnoreNonExist = ignoreNonExist
                      , API.deleteStreamRequestForce          = True
                      })

    DView      txt -> hstreamApiDeleteView (mkClientNormalRequest' def
                      { API.deleteViewRequestViewId = txt
                      , API.deleteViewRequestIgnoreNonExist = ignoreNonExist
                      })

    DConnector txt -> hstreamApiDeleteConnector (mkClientNormalRequest' def
                      { API.deleteConnectorRequestName = txt
                      -- , API.deleteConnectorRequestIgnoreNonExist = checkIfExist
                      })

insertIntoStream
  :: StreamName -> InsertType -> BS.ByteString
  -> Action API.AppendResponse
insertIntoStream sName insertType payload API.HStreamApi{..} = do
  timestamp <- getProtoTimestamp
  let header = case insertType of
        JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp clientDefaultKey
        RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty timestamp clientDefaultKey
      record = buildRecord header payload
  hstreamApiAppend (mkClientNormalRequest' def
    { API.appendRequestStreamName = sName
    , API.appendRequestRecords    = V.singleton record
    })

createStreamBySelect :: T.Text -> Int -> String
  -> Action API.CommandQueryResponse
createStreamBySelect sName rFac sql API.HStreamApi{..} =
  hstreamApiExecuteQuery (mkClientNormalRequest' def
    { API.commandQueryStmtText = T.pack sql})

type Action a = HStreamClientApi -> IO (ClientResult 'Normal a)

runActionWithAddr :: SocketAddr -> Action a -> IO (ClientResult 'Normal a)
runActionWithAddr addr action =
  withGRPCClient (mkGRPCClientConf addr) (hstreamApiClient >=> action)
