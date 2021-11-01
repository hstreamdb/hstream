{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent
import           Control.Exception                (finally, handle)
import           Control.Monad
import           Control.Monad.IO.Class           (liftIO)
import qualified Data.ByteString.Char8            as BSC
import           Data.Char                        (toUpper)
import           Data.Functor                     ((<&>))
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import           Network.Socket                   (PortNumber)
import qualified Options.Applicative              as O
import           System.Console.ANSI              (getTerminalSize)
import qualified System.Console.Haskeline         as H
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)
import qualified Z.Data.CBytes                    as CB
import           Z.IO.Network.SocketAddr          (ipv4)

import           HStream.Client.Action
import           HStream.Client.Gadget
import qualified HStream.Common.Query             as Query
import qualified HStream.Logger                   as Log
import           HStream.SQL
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Store.Logger
import           HStream.Utils                    (Format, HStreamClientApi,
                                                   formatCommandQueryResponse,
                                                   formatResult,
                                                   setupSigsegvHandler)

data UserConfig = UserConfig
  { _serverHost                     :: String
  , _serverPort                     :: PortNumber
  , _clientId                       :: String
  , _availableServersUpdateInterval :: Int
  }

parseConfig :: O.Parser UserConfig
parseConfig =
  UserConfig
    <$> O.strOption (O.long "host" <> O.metavar "HOST" <> O.showDefault <> O.value "127.0.0.1" <> O.help "server host value")
    <*> O.option O.auto (O.long "port" <> O.metavar "INT" <> O.showDefault <> O.value 6570 <> O.short 'p' <> O.help "server port value")
    <*> O.strOption (O.long "client-id" <> O.metavar "ID" <> O.help "unique id for the client")
    <*> O.option O.auto (O.long "update-interval" <> O.metavar "INT" <> O.showDefault <> O.value 30 <> O.help "interval to update available servers in second")

main :: IO ()
main = do
  UserConfig{..} <- O.execParser $ O.info (parseConfig O.<**> O.helper) (O.fullDesc <> O.progDesc "HStream-Client")
  putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]
  let _addr = ipv4 (CB.pack _serverHost) (fromIntegral _serverPort)
  available <- newMVar []
  current <- newMVar _addr
  producers_ <- newMVar mempty
  let ctx = ClientContext { cctxServerHost = _serverHost
                          , cctxServerPort = _serverPort
                          , availableServers = available
                          , currentServer    = current
                          , producers        = producers_
                          , clientId         = _clientId
                          , availableServersUpdateInterval = _availableServersUpdateInterval
                          }
  setupSigsegvHandler
  setLogDeviceDbgLevel C_DBG_ERROR
  m_desc <- describeCluster ctx _addr
  case m_desc of
    Just _  -> app ctx
    Nothing -> do
      Log.e "Connection timed out. Please check the server URI and try again."

app :: ClientContext -> IO ()
app ctx@ClientContext{..} =
  let config = ClientConfig { clientServerHost = Host (BSC.pack cctxServerHost)
                            , clientServerPort = Port (fromIntegral cctxServerPort)
                            , clientArgs       = []
                            , clientSSLConfig  = Nothing
                            , clientAuthority  = Nothing
                            }
   in withGRPCClient config $ \client -> do
        api <- hstreamApiClient client
        query <- Query.newHStreamQuery (CB.pack $ cctxServerHost <> ":" <> show cctxServerPort)
        putStrLn helpInfo
        void $ forkIO maintainAvailableNodes
        H.runInputT H.defaultSettings (loop api query)
  where
    maintainAvailableNodes = forever $ do
      readMVar availableServers >>= \case
        []     -> return ()
        node:_ -> void $ describeCluster ctx node
      threadDelay $ availableServersUpdateInterval * 1000 * 1000
    loop :: HStreamClientApi -> Query.HStreamQuery -> H.InputT IO ()
    loop api query = H.getInputLine "> " >>= \case
      Nothing   -> return ()
      Just str
        | take 1 (words str) == [":q"] -> return ()
        | take 3 (map toUpper <$> words str) == ["USE", "ADMIN", ";"] ||
          take 2 (map toUpper <$> words str) == ["USE", "ADMIN;"]     ->
            loopAdmin api query
        | otherwise -> liftIO (commandExec ctx api str) >> loop api query
    loopAdmin api query = H.getInputLine "ADMIN> " >>= \case
      Nothing   -> return ()
      Just str
        | take 1 (words str) == [":q"] -> return ()
        | take 3 (map toUpper <$> words str) == ["USE", "STREAM", ";"] ||
          take 2 (map toUpper <$> words str) == ["USE", "STREAM;"]     -> loop api query
        | otherwise -> liftIO (mapM_ putStrLn =<< Query.runQuery query (CB.pack str)) >> loopAdmin api query

commandExec :: ClientContext -> HStreamClientApi -> String -> IO ()
commandExec ctx@ClientContext{..} api xs = case words xs of

  ":sub":subId:stream:_ -> callSubscription ctx (T.pack subId) (T.pack stream)
  ":delSub":subId:_     -> callDeleteSubscription ctx (T.pack subId)
  ":fetch":subId:_      -> callStreamingFetch ctx V.empty (T.pack subId) (T.pack clientId)
  ":listSubs":_         -> callListSubscriptions ctx

  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> case M.lookup (map toUpper x) helpInfos of Just infos -> putStrLn infos; Nothing -> pure ()
  xs'@(_:_)   -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    (parseAndRefine . T.pack) xs >>= \case
      RQSelect{} -> sqlStreamAction api (TL.pack xs)
      RQCreate (RCreateAs stream _ rOptions) ->
        createStreamBySelect api (TL.fromStrict stream) (rRepFactor rOptions) xs'
        >>= printResult
      RQSelectStats (RSelectStats colNames tableKind streamNames) ->
        sqlStatsAction api (colNames, tableKind, streamNames)
      rSql' -> hstreamCodegen rSql' >>= \case
        CreatePlan sName rFac
          -> createStream api sName rFac >>= printResult
        ShowPlan showObj
          -> executeShowPlan api showObj
        TerminatePlan termSel
          -> terminateQueries api termSel >>= printResult
        DropPlan checkIfExists dropObj
          -> dropAction api checkIfExists dropObj >>= printResult
        InsertPlan sName insertType payload
          -> insertIntoStream ctx sName insertType payload >>= printResult
        _ -> sqlAction api (TL.pack xs)

  [] -> return ()

sqlStreamAction :: HStreamClientApi -> TL.Text -> IO ()
sqlStreamAction HStreamApi{..} sql = do
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000000 [] action)
  return ()
  where
    action call _meta recv = do
      msg <- withInterrupt (clientCallCancel call) recv
      case msg of
        Left err            -> print err
        Right Nothing       -> putStrLn ("\x1b[32m" <> "Terminated" <> "\x1b[0m")
        Right (Just result) -> do
          width <- getTerminalSize
          putStr $ formatResult (case width of Nothing -> 80; Just (_, w) -> w) result
          action call _meta recv

sqlAction :: HStreamClientApi -> TL.Text -> IO ()
sqlAction HStreamApi{..} sql = do
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 [])
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      width <- getTerminalSize
      putStr $ formatCommandQueryResponse (case width of Nothing -> 80; Just (_, w) -> w) x
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
  CREATE {SOURCE|SINK} CONNECTOR <stream_name> [IF NOT EXIST] WITH ( {connector_options} ) ;
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

executeShowPlan :: HStreamClientApi -> ShowObject -> IO ()
executeShowPlan api showObject =
  case showObject of
    SStreams    -> listStreams    api >>= printResult
    SViews      -> listViews      api >>= printResult
    SQueries    -> listQueries    api >>= printResult
    SConnectors -> listConnectors api >>= printResult

printResult :: Format a => a -> IO ()
printResult resp = getWidth >>= putStr . flip formatResult resp

getWidth :: IO Int
getWidth = getTerminalSize <&> (\case Nothing -> 80; Just (_, w) -> w)
