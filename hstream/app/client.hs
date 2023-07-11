{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# OPTIONS_GHC -Werror=incomplete-patterns #-}

module Main where

import           Control.Concurrent               (threadDelay)
import           Control.Monad                    (when)
import           Data.Aeson                       as Aeson
import qualified Data.Char                        as Char
import qualified Data.List                        as L
import           Data.Maybe                       (mapMaybe, maybeToList)
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientError (..),
                                                   ClientResult (..),
                                                   withGRPCClient)
import qualified Options.Applicative              as O
import           Proto3.Suite                     (def)
import           System.Exit                      (exitFailure)
import           System.Timeout                   (timeout)
import           Text.RawString.QQ                (r)

import qualified HStream.Admin.Server.Command     as Admin
import           HStream.Client.Action            (createSubscription',
                                                   deleteStream,
                                                   deleteSubscription,
                                                   getStream, getSubscription,
                                                   listShards, listStreams,
                                                   listSubscriptions, readShard,
                                                   readStream,  insertIntoStream')
import           HStream.Client.Execute           (executeWithLookupResource_,
                                                   initCliContext,
                                                   simpleExecute)
import           HStream.Client.SQL               (commandExec,
                                                   interactiveSQLApp)
import           HStream.Client.Types             (CliCmd (..), Command (..),
                                                   HStreamCommand (..),
                                                   HStreamInitOpts (..),
                                                   HStreamNodes (..),
                                                   HStreamSqlContext (..),
                                                   HStreamSqlOpts (..),
                                                   ReadShardArgs (..),
                                                   ReadStreamArgs (..),
                                                   RefinedCliConnOpts (..),
                                                   Resource (..),
                                                   StreamCommand (..),
                                                   SubscriptionCommand (..),
                                                   cliCmdParser,
                                                   refineCliConnOpts, AppendArgs (..))
import           HStream.Client.Utils             (mkClientNormalRequest',
                                                   printResult, calculateShardId)
import           HStream.Common.Types             (getHStreamVersion)
import           HStream.Server.HStreamApi        (DescribeClusterResponse (..),
                                                   HStreamApi (..),
                                                   ServerNode (..),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (ResourceType (..),
                                                   fillWithJsonString',
                                                   formatResult,
                                                   handleGRPCIOError,
                                                   newRandomText,
                                                   getServerResp,
                                                   pattern EnumPB, getServerResp)
import qualified HStream.Utils.Aeson              as AesonComp
import HStream.SQL.Codegen.V1 (InsertType(RawFormat))

main :: IO ()
main = runCommand =<<
  O.customExecParser (O.prefs (O.showHelpOnEmpty <> O.showHelpOnError))
                     (O.info (cliCmdParser O.<**> O.helper)
                             (O.fullDesc <> O.header "======= HStream CLI =======")
                     )

runCommand :: CliCmd -> IO ()
runCommand GetVersionCmd = do
  API.HStreamVersion{..} <- getHStreamVersion
  putStrLn $ "version: " <> T.unpack hstreamVersionVersion <> " (" <> T.unpack hstreamVersionCommit <> ")"
runCommand (CliCmd HStreamCommand{..}) = do
  rConnOpts <- refineCliConnOpts cliConnOpts
  case cliCommand of
    HStreamNodes  opts       -> hstreamNodes  rConnOpts opts
    HStreamInit   opts       -> hstreamInit   rConnOpts opts
    HStreamSql    opts       -> hstreamSQL    rConnOpts opts
    HStreamStream opts       -> hstreamStream rConnOpts opts
    HStreamSubscription opts -> hstreamSubscription rConnOpts opts

hstreamSQL :: RefinedCliConnOpts -> HStreamSqlOpts -> IO ()
hstreamSQL connOpt HStreamSqlOpts{_updateInterval = updateInterval,
  _retryInterval = retryInterval, _retryLimit = retryLimit, .. } = do
  hstreamCliContext <- initCliContext connOpt
  case _execute of
    Nothing        -> showHStream *> interactiveSQLApp HStreamSqlContext{..}  _historyFile
    Just statement -> do
      when (Char.isSpace `all` statement) $ do putStrLn "Empty statement" *> exitFailure
      commandExec HStreamSqlContext{..} statement
  where
    showHStream = putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]

hstreamNodes :: RefinedCliConnOpts -> HStreamNodes -> IO ()
hstreamNodes connOpts HStreamNodesList =
  getNodes connOpts >>= putStrLn . formatResult . L.sort . V.toList . API.describeClusterResponseServerNodes
hstreamNodes connOpts (HStreamNodesStatus mid) = do
  nodes <- getNodes connOpts
  let target = case mid of
        Nothing  -> V.toList . API.describeClusterResponseServerNodesStatus $ nodes
        Just sid -> maybeToList . L.find (compareServerId sid) . V.toList . API.describeClusterResponseServerNodesStatus $ nodes
  when (null target) $ errorWithoutStackTrace "Node(s) not found in the cluster"
  putStrLn . formatResult $ target
  where
    compareServerId x API.ServerNodeStatus{..} =
      case serverNodeStatusNode of
        Just API.ServerNode{..} -> serverNodeId == x
        Nothing                 -> False
hstreamNodes connOpts (HStreamNodesCheck nMaybe) = do
  nodes <- describeClusterResponseServerNodesStatus <$> getNodes connOpts
  let n' = length nodes
  case nMaybe of
    Just n -> when (n' < fromIntegral n) $ errorWithoutStackTrace "No enough nodes in the cluster"
    Nothing -> return ()
  let nodesNotRunning = V.filter ((/= EnumPB API.NodeStateRunning) . API.serverNodeStatusState) nodes
  if null nodesNotRunning
    then putStrLn "All nodes in the cluster are running."
    else errorWithoutStackTrace $ "Some Nodes are not running: "
                                <> show (mapMaybe ((API.serverNodeId <$>) . API.serverNodeStatusNode) (V.toList nodesNotRunning))

-- TODO: Init should have it's own rpc call
hstreamInit :: RefinedCliConnOpts -> HStreamInitOpts -> IO ()
hstreamInit RefinedCliConnOpts{..} HStreamInitOpts{..} = do
  ready <- timeout (_timeoutSec * 1000000) $
    withGRPCClient clientConfig $ \client -> do
      api <- hstreamApiClient client
      Admin.sendAdminCommand "server init" api >>= Admin.formatCommandResponse >>= putStrLn
      loop api
  case ready of
    Just s  -> putStrLn s
    Nothing -> putStrLn "Time out waiting for cluster ready" >> exitFailure
  where
    loop api = do
      threadDelay 1000000
      resp <- Admin.sendAdminCommand "server ready" api
      case Aeson.eitherDecodeStrict (T.encodeUtf8 resp) of
        Left errmsg              -> pure $ "Decode json error: " <> errmsg
        Right (Aeson.Object obj) -> do
          let m_type = AesonComp.lookup "type" obj
          case m_type of
            Just (Aeson.String "plain") -> pure $ fillWithJsonString' "content" obj
            _                           -> loop api
        _ -> loop api

hstreamStream :: RefinedCliConnOpts -> StreamCommand -> IO ()
hstreamStream connOpts@RefinedCliConnOpts{..} cmd = do
  case cmd of
    StreamCmdList ->
      simpleExecute clientConfig listStreams >>= printResult
    StreamCmdCreate stream ->
      simpleExecute clientConfig (\HStreamApi{..} -> hstreamApiCreateStream (mkClientNormalRequest' stream)) >>= printResult
    StreamCmdDelete sName force ->
      simpleExecute clientConfig (deleteStream sName force) >>= printResult
    StreamCmdDescribe sName ->
      simpleExecute clientConfig (getStream sName) >>= printResult
    StreamCmdListShard stream -> simpleExecute clientConfig (listShards stream) >>= printResult
    StreamCmdReadShard ReadShardArgs{..} -> do
      suffix <- newRandomText 32
      let req = def { API.readShardStreamRequestReaderId       = "reader_" <> suffix
                    , API.readShardStreamRequestShardId        = shardIdArgs
                    , API.readShardStreamRequestFrom           = startOffset
                    , API.readShardStreamRequestUntil          = endOffset
                    , API.readShardStreamRequestMaxReadBatches = maxReadBatches
                    }
      ctx <- initCliContext connOpts
      executeWithLookupResource_ ctx (Resource ResShardReader (API.readShardStreamRequestReaderId req)) (readShard req)
    StreamCmdReadStream ReadStreamArgs{..} -> do
      suffix <- newRandomText 32
      let req = def { API.readStreamRequestReaderId       = "reader_" <> suffix
                    , API.readStreamRequestStreamName     = readStreamStreamNameArgs
                    , API.readStreamRequestFrom           = readStreamStartOffset
                    , API.readStreamRequestUntil          = readStreamEndOffset
                    , API.readStreamRequestMaxReadBatches = readStreamMaxReadBatches
                    }
      ctx <- initCliContext connOpts
      executeWithLookupResource_ ctx (Resource ResShardReader (API.readStreamRequestReaderId req)) (readStream req)
    StreamCmdAppend AppendArgs{..} -> do
          ctx <- initCliContext connOpts
          shards <- fmap API.listShardsResponseShards . getServerResp =<< simpleExecute clientConfig (listShards appendStream)
          case calculateShardId appendRecordKey (V.toList shards) of
            Just sid -> do
              executeWithLookupResource_ ctx (Resource ResShard (T.pack $ show sid))
                (insertIntoStream' appendStream sid isHRecord (V.fromList appendRecord) appendCompressionType appendRecordKey)
            Nothing  -> errorWithoutStackTrace $ "Failed to calculate shardId with stream: "
                                               <> show appendStream <> ", parition key: " <> show appendRecordKey

hstreamSubscription :: RefinedCliConnOpts -> SubscriptionCommand -> IO ()
hstreamSubscription connOpts@RefinedCliConnOpts{..} = \case
  SubscriptionCmdList -> simpleExecute clientConfig listSubscriptions >>= printResult
  SubscriptionCmdDescribe sid -> do
    ctx <- initCliContext connOpts
    executeWithLookupResource_ ctx (Resource ResSubscription sid) (getSubscription sid) >>= printResult
  SubscriptionCmdCreate subscription -> do
    ctx <- initCliContext connOpts
    executeWithLookupResource_ ctx (Resource ResSubscription (API.subscriptionSubscriptionId subscription)) (createSubscription' subscription) >>= printResult
  SubscriptionCmdDelete sid bool -> do
    ctx <- initCliContext connOpts
    executeWithLookupResource_ ctx (Resource ResSubscription sid) (deleteSubscription sid bool) >>= printResult

getNodes :: RefinedCliConnOpts -> IO DescribeClusterResponse
getNodes RefinedCliConnOpts{..} =
  withGRPCClient clientConfig $ \client -> do
    HStreamApi{..} <- hstreamApiClient client
    res <- hstreamApiDescribeCluster (mkClientNormalRequest' Empty)
    case res of
      ClientNormalResponse resp _ _ _ _ -> return resp
      ClientErrorResponse (ClientIOError e) -> putStrLn (handleGRPCIOError e) >> exitFailure
      ClientErrorResponse err -> error $ "Server Error: " <> show err <> "\n"
