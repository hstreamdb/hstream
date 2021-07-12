{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Exception                (finally, try)
import           Control.Monad.IO.Class           (liftIO)
import           Data.ByteString                  (ByteString)
import           Data.Char                        (toUpper)
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import qualified Options.Applicative              as O
import           System.Console.ANSI              (getTerminalSize)
import qualified System.Console.Haskeline         as H
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)

import           HStream.SQL
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Store                    (setupSigsegvHandler)
import           HStream.Utils.Format             (formatCommandQueryResponse,
                                                   formatResult)

data UserConfig = UserConfig
  { _serverHost :: ByteString
  , _serverPort :: Int
  }

parseConfig :: O.Parser UserConfig
parseConfig =
  UserConfig
    <$> O.strOption (O.long "host" <> O.metavar "HOST" <> O.showDefault <> O.value "127.0.0.1" <> O.help "server host value")
    <*> O.option O.auto (O.long "port" <> O.metavar "INT" <> O.showDefault <> O.value 6570 <> O.short 'p' <> O.help "server port value")

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
  setupSigsegvHandler
  let clientConfig = ClientConfig { clientServerHost = Host _serverHost
                                  , clientServerPort = Port _serverPort
                                  , clientArgs = []
                                  , clientSSLConfig = Nothing
                                  , clientAuthority = Nothing
                                  }
   in app clientConfig

app :: ClientConfig -> IO ()
app clientConfig = do
  putStrLn helpInfo
  H.runInputT H.defaultSettings  loop
  where
    loop :: H.InputT IO ()
    loop = do
      H.getInputLine "> " >>= \case
        Nothing   -> return ()
        Just str
          | take 1 (words str) == [":q"] -> return ()
          | otherwise  -> liftIO (commandExec clientConfig str) >> loop

commandExec :: ClientConfig -> String -> IO ()
commandExec clientConfig xs = case words xs of
  ":h" : _ -> putStr helpInfo
  [":help"] -> putStr groupedHelpInfo
  ":help" : x : _ -> case M.lookup (map toUpper x) helpInfos of Just infos -> putStrLn infos; Nothing -> pure ()
  val@(_ : _) -> do
    let sql = T.pack (unwords val)
    (liftIO . try . parseAndRefine $ sql) >>= \case
      Left (e :: SomeSQLException) -> liftIO . putStrLn . formatSomeSQLException $ e
      Right rsql                -> case rsql of
        RQSelect _ -> liftIO $ sqlStreamAction clientConfig (TL.fromStrict sql)
        _          -> liftIO $ sqlAction       clientConfig (TL.fromStrict sql)
  [] -> return ()

sqlStreamAction :: ClientConfig -> TL.Text -> IO ()
sqlStreamAction clientConfig sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000000 [] action)
  return ()
  where
    action call _meta recv =
      let go = do
            msg <- withInterrupt (clientCallCancel call) recv
            case msg of
              Left err            -> print err
              Right Nothing       -> putStrLn ("\x1b[32m" <> "Terminated" <> "\x1b[0m")
              Right (Just result) -> do
                width <- getTerminalSize
                putStr $ formatResult (case width of Nothing -> 80; Just (_, w) -> w) result
                go
      in go

sqlAction :: ClientConfig -> TL.Text -> IO ()
sqlAction clientConfig sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 [])
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      width <- getTerminalSize
      putStr $ formatCommandQueryResponse (case width of Nothing -> 80; Just (_, w) -> w) x
    ClientErrorResponse clientError -> putStrLn $ "Client Error: " <> show clientError

withInterrupt :: IO () -> IO a -> IO a
withInterrupt handle act = do
  old_handler <- installHandler keyboardSignal (Catch handle) Nothing
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
  SHOW <CONNECTORS|QUERIES|VIEWS>;
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
