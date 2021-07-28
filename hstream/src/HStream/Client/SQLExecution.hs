{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.Client.SQLExecution where

import qualified Data.ByteString                  as BS
import           Data.Functor                     ((<&>))
import qualified Data.Map                         as Map
import qualified Data.Text.Lazy                   as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientRequest (ClientNormalRequest),
                                                   GRPCMethodType (Normal),
                                                   MetadataMap (MetadataMap))
import           Proto3.Suite.Class               (HasDefault, def)
import qualified Proto3.Suite.Class               ()
import           System.Console.ANSI              (getTerminalSize)

import qualified HStream.Logger                   as Log
import           HStream.SQL.Codegen              (DropObject (..),
                                                   InsertType (..),
                                                   ShowObject (..), StreamName,
                                                   TerminationSelection (..))
import qualified HStream.Server.HStreamApi        as API
import           HStream.Utils                    (Format (..),
                                                   HStreamClientApi,
                                                   buildRecord,
                                                   buildRecordHeader,
                                                   cBytesToLazyText,
                                                   getProtoTimestamp)
import Data.Char (toUpper)

executeCreatePlan :: HStreamClientApi -> StreamName -> Int -> IO ()
executeCreatePlan API.HStreamApi{..} sName rFac =
  hstreamApiCreateStream (mkClientNormalRequest def
    { API.streamStreamName        = TL.fromStrict sName
    , API.streamReplicationFactor = fromIntegral rFac})
  >>= printResult

executeShowPlan :: HStreamClientApi -> ShowObject -> IO ()
executeShowPlan API.HStreamApi{..} showObject =
  case showObject of
    SStreams    -> hstreamApiListStreams requestDefault >>= printResult
    SViews      -> hstreamApiListViews   requestDefault >>= printResult
    SQueries    -> hstreamApiListQueries requestDefault >>= printResult
    SConnectors -> hstreamApiListConnectors requestDefault >>= printResult

executeTerminatePlan :: HStreamClientApi -> TerminationSelection -> IO ()
executeTerminatePlan API.HStreamApi{..} (OneQuery qid) =
  hstreamApiTerminateQueries
    (mkClientNormalRequest def{API.terminateQueriesRequestQueryId = V.singleton $ cBytesToLazyText qid})
  >> putStr "Done. No results.\n"
executeTerminatePlan API.HStreamApi{..} AllQueries =
  hstreamApiTerminateQueries
    (mkClientNormalRequest def{API.terminateQueriesRequestAll = True})
  >> putStr "Done. No results.\n"
executeTerminatePlan _ (ManyQueries _) = Log.e $ Log.buildText "Not Supported.\n"

executeDropPlan :: HStreamClientApi -> Bool -> DropObject -> IO ()
executeDropPlan API.HStreamApi{..} checkIfExist dropObject = do
  case dropObject of
    DStream    txt -> hstreamApiDeleteStream (mkClientNormalRequest def
                      { API.deleteStreamRequestStreamName     = T.fromStrict txt
                      , API.deleteStreamRequestIgnoreNonExist = checkIfExist
                      })
      >>= printResult
    DView      txt -> hstreamApiDeleteView (mkClientNormalRequest def
                      { API.deleteViewRequestViewId = T.fromStrict txt
                      -- , API.deleteViewRequestIgnoreNonExist = checkIfExist
                      })
      >>= printResult
    DConnector txt -> hstreamApiDeleteConnector (mkClientNormalRequest def
                      { API.deleteConnectorRequestId = T.fromStrict txt
                      -- , API.deleteConnectorRequestIgnoreNonExist = checkIfExist
                      })
      >>= printResult

executeInsertPlan :: HStreamClientApi -> StreamName -> InsertType -> BS.ByteString -> IO ()
executeInsertPlan API.HStreamApi{..} sName insertType payload = do
  timestamp <- getProtoTimestamp
  let header = case insertType of
        JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp TL.empty
        RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty timestamp TL.empty
      record = buildRecord header payload
  hstreamApiAppend (mkClientNormalRequest def
    { API.appendRequestStreamName = TL.fromStrict sName
    , API.appendRequestRecords    = V.singleton record
    })
  >>= printResult

--------------------------------------------------------------------------------

executeCreateBySelect :: HStreamClientApi -> T.Text -> Int -> [String] -> IO ()
executeCreateBySelect API.HStreamApi{..} sName rFac sql =
  hstreamApiCreateQueryStream (mkClientNormalRequest def
    { API.createQueryStreamRequestQueryStream
        = Just def
        { API.streamStreamName        = sName
        , API.streamReplicationFactor = fromIntegral rFac}
    , API.createQueryStreamRequestQueryStatements = extractSelect sql})
  >>= printResult

--------------------------------------------------------------------------------

getWidth :: IO Int
getWidth = getTerminalSize <&> (\case Nothing -> 80; Just (_, w) -> w)

printResult :: Format a => a -> IO ()
printResult resp = getWidth >>= putStr . flip formatResult resp

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
