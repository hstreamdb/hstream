{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent               (forkIO, newMVar, readMVar,
                                                   threadDelay)
import           Control.Exception                (finally, handle)
import           Control.Monad                    (forever, void, (>=>))
import           Control.Monad.IO.Class           (liftIO)
import           Data.Char                        (toUpper)
import qualified Data.List                        as L
import qualified Data.Map                         as M
import           Data.Maybe                       (maybeToList)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientError (ClientIOError),
                                                   ClientRequest (..),
                                                   ClientResult (..),
                                                   GRPCIOError (GRPCIOBadStatusCode),
                                                   StatusDetails (unStatusDetails),
                                                   withGRPCClient)
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import qualified Options.Applicative              as O
import qualified System.Console.Haskeline         as H
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)
import           Z.IO.Network.SocketAddr          (ipv4)

import qualified HStream.Admin.Server.Command     as Admin
import           HStream.Client.Action            (createStream,
                                                   createStreamBySelect,
                                                   dropAction, insertIntoStream,
                                                   terminateQueries)
import           HStream.Client.Execute           (execute, executeInsert,
                                                   executeShowPlan)
import           HStream.Client.Gadget            (describeCluster)
import           HStream.Client.Internal          (callDeleteSubscription,
                                                   callDeleteSubscriptionAll,
                                                   callListSubscriptions,
                                                   callStreamingFetch,
                                                   callSubscription)
import           HStream.Client.Types             (CliConnOpts (..),
                                                   Command (..),
                                                   HStreamCommand (..),
                                                   HStreamNodes (..),
                                                   HStreamSqlContext (..),
                                                   HStreamSqlOpts (..),
                                                   commandParser)
import           HStream.Client.Utils             (mkClientNormalRequest')
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (CommandPushQuery (..),
                                                   CommandQuery (..),
                                                   CommandQueryResponse (..),
                                                   DescribeClusterResponse (..),
                                                   HStreamApi (..),
                                                   ServerNode (serverNodeId),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (HStreamPlan (..),
                                                   RCreate (..), RSQL (..),
                                                   RStreamOptions (..),
                                                   hstreamCodegen,
                                                   parseAndRefine)
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (HStreamClientApi,
                                                   formatCommandQueryResponse,
                                                   formatResult,
                                                   mkGRPCClientConf,
                                                   setupSigsegvHandler)

main :: IO ()
main = runCommand =<<
  O.customExecParser (O.prefs (O.showHelpOnEmpty <> O.showHelpOnError))
                     (O.info (commandParser O.<**> O.helper)
                             (O.fullDesc <> O.header "======= HStream CLI =======")
                     )

runCommand :: HStreamCommand -> IO ()
runCommand HStreamCommand {..} =
  case cliCommand of
    HStreamSql opts   -> hstreamSQL cliConnOpts opts
    HStreamNodes opts -> hstreamNodes cliConnOpts opts
    HStreamInit       -> hstreamInit cliConnOpts

hstreamSQL :: CliConnOpts -> HStreamSqlOpts -> IO ()
hstreamSQL CliConnOpts{..} HStreamSqlOpts{ _clientId = clientId, _updateInterval = updateInterval } = do
  putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]
  let addr = ipv4 _serverHost _serverPort
  availableServers <- newMVar []
  currentServer    <- newMVar addr
  let ctx = HStreamSqlContext {..}
  setupSigsegvHandler
  m_desc <- describeCluster ctx addr
  case m_desc of
    Nothing -> Log.e "Connection timed out. Please check the server URI and try again."
    Just _  -> app ctx


hstreamNodes :: CliConnOpts -> HStreamNodes -> IO ()
hstreamNodes connOpts HStreamNodesList =
  getNodes connOpts >>= putStrLn . formatResult . V.toList . API.describeClusterResponseServerNodes
hstreamNodes connOpts (HStreamNodesStatus Nothing) =
  getNodes connOpts >>= putStrLn . formatResult . V.toList . API.describeClusterResponseServerNodesStatus
hstreamNodes connOpts (HStreamNodesStatus (Just sid)) =
  getNodes connOpts
  >>= putStrLn
    . formatResult
    . maybeToList . L.find (compareServerId sid)
    . V.toList . API.describeClusterResponseServerNodesStatus
  where
    compareServerId x API.ServerNodeStatus{..} =
      case serverNodeStatusNode of
        Just API.ServerNode{..} -> serverNodeId == x
        Nothing                 -> False

-- TODO: Init should have it's own rpc call
hstreamInit :: CliConnOpts -> IO ()
hstreamInit CliConnOpts{..} = do
  withGRPCClient (mkGRPCClientConf $ ipv4 _serverHost _serverPort) $ \client -> do
    hstreamApiClient client
    >>= Admin.sendAdminCommand "server init"
    >>= Admin.formatCommandResponse
    >>= putStrLn

getNodes :: CliConnOpts -> IO DescribeClusterResponse
getNodes CliConnOpts{..} =
  withGRPCClient (mkGRPCClientConf $ ipv4 _serverHost _serverPort) $ \client -> do
    HStreamApi{..} <- hstreamApiClient client
    res <- hstreamApiDescribeCluster (mkClientNormalRequest' Empty)
    case res of
      ClientNormalResponse resp _ _ _ _ -> return resp
      ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode x details))
        -> error $ show x <> " Error: "  <> show (unStatusDetails details)
      ClientErrorResponse err -> error $ "Server Error: " <> show err <> "\n"

--------------------------------------------------------------------------------

-- FIXME: Currently, every new command will create a new connection to a server,
-- and this needs to be optimized. This could be done with a grpc client pool.
app :: HStreamSqlContext -> IO ()
app ctx@HStreamSqlContext{..} = do
  putStrLn helpInfo
  void $ forkIO maintainAvailableNodes
  H.runInputT H.defaultSettings loop
  where
    maintainAvailableNodes = forever $ do
      readMVar availableServers >>= \case
        []     -> return ()
        node:_ -> void $ describeCluster ctx node
      threadDelay $ updateInterval * 1000 * 1000

    loop :: H.InputT IO ()
    loop = H.withInterrupt . H.handleInterrupt loop $ do
      H.getInputLine "> " >>= \case
        Nothing -> pure ()
        Just str
          | take 1 (words str) == [":q"] -> pure ()
          | otherwise -> liftIO (commandExec ctx str) >> loop

commandExec :: HStreamSqlContext -> String -> IO ()
commandExec ctx@HStreamSqlContext{..} xs = case words xs of
  [] -> return ()

  -- The Following commands are for testing only
  -- {
  ":sub":subId:stream:_ -> callSubscription ctx (T.pack subId) (T.pack stream)
  ":delSub":subId:_     -> callDeleteSubscription ctx (T.pack subId)
  ":delAllSubs":_       -> callDeleteSubscriptionAll ctx
  ":fetch":subId:_      -> callStreamingFetch ctx V.empty (T.pack subId) (T.pack clientId)
  ":listSubs":_         -> callListSubscriptions ctx
  -- }

  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> case M.lookup (map toUpper x) helpInfos of Just infos -> putStrLn infos; Nothing -> pure ()
  xs'@(_:_)   -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    (parseAndRefine . T.pack) xs >>= \case
      RQSelect{} -> runActionWithGrpc ctx (\api -> sqlStreamAction api (T.pack xs))
      RQCreate (RCreateAs stream _ rOptions) ->
        execute ctx $ createStreamBySelect stream (rRepFactor rOptions) xs'
      rSql' -> hstreamCodegen rSql' >>= \case
        CreatePlan sName rFac
          -> execute ctx $ createStream sName rFac
        ShowPlan showObj
          -> executeShowPlan ctx showObj
        TerminatePlan termSel
          -> execute ctx $ terminateQueries termSel
        DropPlan checkIfExists dropObj
          -> execute ctx $ dropAction checkIfExists dropObj
        InsertPlan sName insertType payload
          -> executeInsert ctx sName $ insertIntoStream sName insertType payload
        _ -> do
          addr <- readMVar currentServer
          withGRPCClient (mkGRPCClientConf addr)
            (hstreamApiClient >=> \api -> sqlAction api (T.pack xs))

sqlStreamAction :: HStreamClientApi -> T.Text -> IO ()
sqlStreamAction HStreamApi{..} sql = do
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000000 mempty action)
  return ()
  where
    action call _meta recv = do
      msg <- withInterrupt (clientCallCancel call) recv
      case msg of
        Left err            -> print err
        Right Nothing       -> putStrLn ("\x1b[32m" <> "Terminated" <> "\x1b[0m")
        Right (Just result) -> do
          putStr $ formatResult result
          action call _meta recv

sqlAction :: HStreamClientApi -> T.Text -> IO ()
sqlAction HStreamApi{..} sql = do
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 mempty)
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      putStr $ formatCommandQueryResponse x
    ClientErrorResponse clientError -> putStrLn $ "Client Error: " <> show clientError

withInterrupt :: IO () -> IO a -> IO a
withInterrupt interruptHandle act = do
  old_handler <- installHandler keyboardSignal (Catch interruptHandle) Nothing
  act `finally` installHandler keyboardSignal old_handler Nothing

helpInfo :: String
helpInfo =
  [r|
Command
  :h                           To show these help info
  :q                           To exit command line interface
  :help [sql_operation]        To show full usage of sql statement

SQL STATEMENTS:
  To create a simplest stream:
    CREATE STREAM stream_name;

  To create a query select all fields from a stream:
    SELECT * FROM stream_name EMIT CHANGES;

  To insert values to a stream:
    INSERT INTO stream_name (field1, field2) VALUES (1, 2);
  |]

helpInfos :: M.Map String String
helpInfos = M.fromList [
  ("CREATE",[r|
  CREATE STREAM <stream_name> [AS <select_query>] [ WITH ( {stream_options} ) ];
  CREATE {SOURCE|SINK} CONNECTOR <connector_name> [IF NOT EXIST] WITH ( {connector_options} ) ;
  CREATE VIEW <stream_name> AS <select_query> ;
  |]),
  ("INSERT",[r|
  INSERT INTO <stream_name> ( {field_name} ) VALUES ( {field_value} );
  INSERT INTO <stream_name> VALUES '<json_value>';
  INSERT INTO <stream_name> VALUES "<binary_value>";
  |]),
  ("SELECT", [r|
  SELECT <* | {expression [ AS field_alias ]}>
  FROM stream_name_1
       [ join_type JOIN stream_name_2
         WITHIN (some_interval)
         ON stream_name_1.field_1 = stream_name_2.field_2 ]
  [ WHERE search_condition ]
  [ GROUP BY field_name [, window_type] ]
  EMIT CHANGES;
  |]),
  ("SHOW", [r|
  SHOW <CONNECTORS|STREAMS|QUERIES|VIEWS>;
  |]),
  ("TERMINATE", [r|
  TERMINATE <QUERY <query_id>|ALL>;
  |]),
  ("DROP", [r|
  DROP <STREAM <stream_name>|VIEW <view_name>> [IF EXISTS];
  |])
  ]

groupedHelpInfo :: String
groupedHelpInfo = ("SQL Statements\n" <> ) . unlines . map (\(x, y) -> x <> "  " <> y) . M.toList $ helpInfos

runActionWithGrpc :: HStreamSqlContext
  -> (HStreamClientApi -> IO b) -> IO b
runActionWithGrpc HStreamSqlContext{..} action= do
  addr <- readMVar currentServer
  withGRPCClient (mkGRPCClientConf addr)
    (hstreamApiClient >=> action)
