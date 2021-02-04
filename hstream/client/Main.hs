{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Conduit
import           Control.Exception        (SomeException, try)
import           Data.Aeson               (FromJSON, eitherDecode')
import qualified Data.ByteString.Char8    as BC
import qualified Data.ByteString.Lazy     as BL
import           Data.IORef
import qualified Data.List                as L
import           Data.Proxy               (Proxy (..))
import           Data.Text                (pack)
import           HStream.SQL
import           HStream.Server.Type
import           Network.HTTP.Simple      (Request, getResponseBody, httpBS,
                                           httpSink, parseRequest,
                                           setRequestBodyJSON, setRequestMethod)
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, metavar,
                                           option, progDesc, short, showDefault,
                                           strOption, value, (<**>))
import           System.Console.Haskeline (Completion, CompletionFunc, InputT,
                                           Settings, completeWord,
                                           defaultSettings, getInputLine,
                                           handleInterrupt, runInputT,
                                           setComplete, simpleCompletion,
                                           withInterrupt)
import           System.Random
import           Text.Pretty.Simple       (pPrint)

parseConfig :: Parser ClientConfig
parseConfig =
  ClientConfig
    <$> strOption (long "host" <> metavar "HOST" <> showDefault <> value "http://localhost" <> help "host url")
    <*> option auto (long "port" <> showDefault <> value 8081 <> short 'p' <> help "client port value" <> metavar "INT")

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

main :: IO ()
main = do
  putStrLn "Start HStream-Cli!"
  cf <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "start hstream-cli")
  putStrLn helpInfo
  clientState <- newIORef Nothing
  runInputT def $ loop cf clientState
  where
    loop :: ClientConfig -> ClientState -> InputT IO ()
    loop c@ClientConfig {..} clientState = handleInterrupt ((liftIO $ putStrLn "interrupted") >> loop c clientState) $
      (liftIO $ readIORef clientState) >>= \case
        Just taskName -> do
          let createRequest api = liftIO $ parseRequest (cHttpUrl ++ ":" ++ show cServerPort ++ api)
          createRequest ("/terminate/queryByName/" ++ taskName) >>= handleReq @Resp Proxy
          liftIO $ writeIORef clientState Nothing
          loop c clientState
        Nothing -> do
          withInterrupt $ do
            input <- getInputLine "> "
            let createRequest api = liftIO $ parseRequest (cHttpUrl ++ ":" ++ show cServerPort ++ api)
            case input of
              Nothing -> return ()
              Just ":q" -> return ()
              Just xs -> do
                case words xs of
                  ":h" : _ -> liftIO $ putStrLn helpInfo
                  "show" : "queries" : _ -> createRequest "/show/queries" >>= handleReq @[TaskInfo] Proxy
                  "terminate" : "query" : "all" : _ -> createRequest ("/terminate/query/all") >>= handleReq @Resp Proxy
                  "terminate" : "query" : dbid -> createRequest ("/terminate/query/" ++ unwords dbid) >>= handleReq @Resp Proxy
                  val@(_ : _) -> do
                    (liftIO $ try $ parseAndRefine $ pack $ unwords val) >>= \case
                      Left (err :: SomeException) -> liftIO $ putStrLn $ show err
                      Right sql -> case sql of
                        RQSelect _ -> do
                          name <- liftIO $ mapM (\_ -> randomRIO ('a', 'z')) [1..8 :: Int]
                          re <- createRequest ("/create/stream/query/" ++ name)
                          liftIO $
                            handleStreamReq $
                              setRequestBodyJSON (ReqSQL (pack $ unwords val)) $
                                setRequestMethod "POST" re
                          liftIO $ writeIORef clientState (Just name)
                        _ -> do
                          re <- createRequest "/create/query"
                          handleReq @(Either String TaskInfo) Proxy $
                            setRequestBodyJSON (ReqSQL (pack $ unwords val)) $
                              setRequestMethod "POST" re
                  [] -> return ()
                loop c clientState

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

handleReq :: forall a. (Show a, FromJSON a) => Proxy a -> Request -> InputT IO ()
handleReq _ req = liftIO $ do
  (try $ httpBS req) >>= \case
    Left (e :: SomeException) -> print e
    Right a -> do
      case getResponseBody a of
        "" -> putStrLn "invalid command"
        body -> case eitherDecode' (BL.fromStrict body) of
          Left e           -> print e
          Right (rsp :: a) -> pPrint rsp

handleStreamReq :: Request -> IO ()
handleStreamReq req = do
  (try $ httpSink req (\_ -> mapM_C BC.putStrLn)) >>= \case
    Left (e :: SomeException) -> print e
    Right _                   -> return ()

