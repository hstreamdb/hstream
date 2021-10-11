{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Action where

import qualified Data.ByteString                  as BS
import           Data.Function
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientRequest (ClientNormalRequest),
                                                   ClientResult,
                                                   GRPCMethodType (Normal),
                                                   MetadataMap (MetadataMap))
import           Proto3.Suite.Class               (HasDefault, def)

import           Data.Char                        (toUpper)
import           HStream.SQL.AST                  (RStatsTable (..))
import           HStream.SQL.Codegen              (DropObject (..),
                                                   InsertType (..), StreamName,
                                                   TerminationSelection (..))
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Handler.Stats     (processTable,
                                                   queryAllAppendInBytes,
                                                   queryAllRecordBytes)
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (HStreamClientApi,
                                                   buildRecord,
                                                   buildRecordHeader,
                                                   cBytesToLazyText,
                                                   getProtoTimestamp)

createStream :: HStreamClientApi -> StreamName -> Int -> IO (ClientResult 'Normal API.Stream)
createStream API.HStreamApi{..} sName rFac =
  hstreamApiCreateStream (mkClientNormalRequest def
    { API.streamStreamName        = TL.fromStrict sName
    , API.streamReplicationFactor = fromIntegral rFac})

listStreams :: HStreamClientApi -> IO (ClientResult 'Normal API.ListStreamsResponse)
listStreams API.HStreamApi{..} = hstreamApiListStreams requestDefault
listViews :: HStreamClientApi -> IO (ClientResult 'Normal API.ListViewsResponse)
listViews API.HStreamApi{..} = hstreamApiListViews requestDefault
listQueries :: HStreamClientApi -> IO (ClientResult 'Normal API.ListQueriesResponse)
listQueries API.HStreamApi{..} = hstreamApiListQueries requestDefault
listConnectors :: HStreamClientApi -> IO (ClientResult 'Normal API.ListConnectorsResponse)
listConnectors API.HStreamApi{..} = hstreamApiListConnectors requestDefault

terminateQueries :: HStreamClientApi
  -> TerminationSelection
  -> IO (ClientResult 'Normal API.TerminateQueriesResponse )
terminateQueries API.HStreamApi{..} (OneQuery qid) =
  hstreamApiTerminateQueries
    (mkClientNormalRequest def{API.terminateQueriesRequestQueryId = V.singleton $ cBytesToLazyText qid})
terminateQueries API.HStreamApi{..} AllQueries =
  hstreamApiTerminateQueries
    (mkClientNormalRequest def{API.terminateQueriesRequestAll = True})
terminateQueries API.HStreamApi{..} (ManyQueries qids) =
  hstreamApiTerminateQueries
    (mkClientNormalRequest
      def {API.terminateQueriesRequestQueryId = V.fromList $ cBytesToLazyText <$> qids})

dropAction :: HStreamClientApi -> Bool -> DropObject -> IO (ClientResult 'Normal Empty)
dropAction API.HStreamApi{..} checkIfExist dropObject = do
  case dropObject of
    DStream    txt -> hstreamApiDeleteStream (mkClientNormalRequest def
                      { API.deleteStreamRequestStreamName     = TL.fromStrict txt
                      , API.deleteStreamRequestIgnoreNonExist = checkIfExist
                      })

    DView      txt -> hstreamApiDeleteView (mkClientNormalRequest def
                      { API.deleteViewRequestViewId = TL.fromStrict txt
                      -- , API.deleteViewRequestIgnoreNonExist = checkIfExist
                      })

    DConnector txt -> hstreamApiDeleteConnector (mkClientNormalRequest def
                      { API.deleteConnectorRequestId = TL.fromStrict txt
                      -- , API.deleteConnectorRequestIgnoreNonExist = checkIfExist
                      })

insertIntoStream :: HStreamClientApi
  -> StreamName -> InsertType -> BS.ByteString
  -> IO (ClientResult 'Normal API.AppendResponse)
insertIntoStream API.HStreamApi{..} sName insertType payload = do
  timestamp <- getProtoTimestamp
  let header = case insertType of
        JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp TL.empty
        RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty timestamp TL.empty
      record = buildRecord header payload
  hstreamApiAppend (mkClientNormalRequest def
    { API.appendRequestStreamName = TL.fromStrict sName
    , API.appendRequestRecords    = V.singleton record
    })

createStreamBySelect :: HStreamClientApi
  -> TL.Text -> Int -> [String]
  -> IO (ClientResult 'Normal API.CreateQueryStreamResponse)
createStreamBySelect API.HStreamApi{..} sName rFac sql =
  hstreamApiCreateQueryStream (mkClientNormalRequest def
    { API.createQueryStreamRequestQueryStream
        = Just def
        { API.streamStreamName        = sName
        , API.streamReplicationFactor = fromIntegral rFac}
    , API.createQueryStreamRequestQueryStatements = extractSelect sql})

--------------------------------------------------------------------------------

requestDefault :: HasDefault a => ClientRequest 'Normal a b
requestDefault = mkClientNormalRequest def

requestTimeout :: Int
requestTimeout = 1000

mkClientNormalRequest :: a -> ClientRequest 'Normal a b
mkClientNormalRequest x = ClientNormalRequest x requestTimeout (MetadataMap Map.empty)

extractSelect :: [String] -> TL.Text
extractSelect = TL.pack .
  unwords . reverse . ("CHANGES;" :) .
  dropWhile ((/= "EMIT") . map toUpper) .
  reverse .
  dropWhile ((/= "SELECT") . map toUpper)

--------------------------------------------------------------------------------
sqlStatsAction :: HStreamClientApi -> ([T.Text], RStatsTable, [T.Text]) -> IO ()
sqlStatsAction api (colNames, tableKind, streamNames) = do
  tableRes <- api & case tableKind of
    AppendInBytes -> queryAllAppendInBytes
    RecordBytes   -> queryAllRecordBytes
  putStrLn $ processTable tableRes colNames streamNames
