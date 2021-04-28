{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Exception
import           Control.Monad.IO.Class           (liftIO)
import           Data.ByteString                  (ByteString)
import qualified Data.List                        as L
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           HStream.SQL
import           HStream.Server.HStreamApi
import           HStream.Server.Utils             (structToJsonObject)
import           Network.GRPC.HighLevel.Generated
import           Options.Applicative
import           System.Console.Haskeline
import           Text.RawString.QQ

helpInfo :: String
helpInfo =
  unlines
    [ "Command ",
      "  :h                        help command",
      "  :q                        quit cli",
      "  show queries              list all queries",
      "  terminate query <taskid>  terminate query by id",
      "  terminate query all       terminate all queries",
      "  <sql>                     run sql"
    ]

def :: Settings IO
def = setComplete compE defaultSettings

compE :: CompletionFunc IO
compE = completeWord Nothing [] compword

wordTable :: [[String]]
wordTable =
  [ ["show", "queries"],
    ["terminate", "query"],
    ["terminate", "query", "all"],
    [":h"],
    [":q"]
  ]

generalComplete :: [[String]] -> [String] -> [String]
generalComplete t [] = L.nub (map head t)
generalComplete t [x] = case L.nub (filter (L.isPrefixOf x) (map head t)) of
  [w]
    | x == w ->
      map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) [])
  ws -> ws
generalComplete t (x : xs) =
  map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) xs)

specificComplete :: Monad m => [String] -> m [String]
specificComplete _ = return []

compword :: Monad m => String -> m [Completion]
compword s = do
  let gs = generalComplete wordTable (words s)
  cs <- specificComplete (words s)
  return $ map simpleCompletion (gs <> cs)

--------------------------------------------------------------------------------
data UserConfig = UserConfig
  { _serverHost :: ByteString
  , _serverPort :: Int
  }

parseConfig :: Parser UserConfig
parseConfig =
  UserConfig
    <$> strOption (long "host" <> metavar "HOST" <> showDefault <> value "127.0.0.1" <> help "server host value")
    <*> option auto (long "port" <> metavar "INT" <> showDefault <> value 50051 <> short 'p' <> help "server port value")

main :: IO ()
main = do
  UserConfig{..} <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Client")
  putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]
  let clientConfig = ClientConfig { clientServerHost = Host _serverHost
                                  , clientServerPort = Port . fromIntegral $ _serverPort
                                  , clientArgs = []
                                  , clientSSLConfig = Nothing
                                  , clientAuthority = Nothing
                                  }
   in app clientConfig

app :: ClientConfig -> IO ()
app clientConfig = do
  putStrLn helpInfo
  runInputT def loop
  where
    loop :: InputT IO ()
    loop = handleInterrupt (liftIO (putStrLn "interrupted") >> loop) $
      withInterrupt $ do
        input <- getInputLine "> "
        case input of
          Nothing   -> return ()
          Just ":q" -> return ()
          Just xs   -> do
            case words xs of
              ":h" : _                          -> liftIO $ putStrLn helpInfo
              "show" : "queries" : _            -> undefined
              "terminate" : "query" : "all" : _ -> undefined
              "terminate" : "query" : dbid      -> undefined
              val@(_ : _)                       -> do
                let sql = T.pack (unwords val)
                (liftIO . try . parseAndRefine $ sql) >>= \case
                  Left (e :: SomeException) -> liftIO . print $ e
                  Right rsql                -> case rsql of
                    RQSelect _ -> liftIO $ sqlStreamAction clientConfig (TL.fromStrict sql)
                    _          -> liftIO $ sqlAction       clientConfig (TL.fromStrict sql)
              [] -> return ()
            loop

sqlStreamAction :: ClientConfig -> TL.Text -> IO ()
sqlStreamAction clientConfig sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000 [] action)
  return ()
  where action _call _meta recv = go
          where go = do msg <- recv
                        case msg of
                          Left err            -> print err
                          Right Nothing       -> print "terminated"
                          Right (Just result) -> do
                            let object = structToJsonObject result
                            print object
                            go

sqlAction :: ClientConfig -> TL.Text -> IO ()
sqlAction clientConfig sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 [])
  case resp of
    ClientNormalResponse x@CommandQueryResponse{..} _meta1 _meta2 _status _details -> do
      print x
    ClientErrorResponse clientError -> print $ "client error: " <> show clientError
