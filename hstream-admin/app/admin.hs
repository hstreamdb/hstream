{-# LANGUAGE BangPatterns #-}

module Main (main) where

import           Data.List                    (uncons)
import qualified Data.Text                    as Text
import qualified Data.Text.Encoding           as TE
import           Options.Applicative          ((<**>))
import qualified Options.Applicative          as O
import           System.Environment           (getArgs)
import           System.Process

import           HStream.Admin.Server.Command (getResourceType)
import qualified HStream.Admin.Server.Command as Server
import qualified HStream.Admin.Server.Types   as Server
import qualified HStream.Logger               as Log
import qualified HStream.Store.Logger         as CLog

main :: IO ()
main = do
  args <- getArgs
  case uncons args of
    Nothing                    -> run
    Just ("store", store_args) -> runStoreCli store_args
    _                          -> run
  where
    run = runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
    opts = O.info
      (cliParser <**> O.helper)
      (O.fullDesc <> O.header "======= HStream Admin CLI =======")

-- TODO
--
-- data Cli = ServerCli Server.Cli
--          | StoreCli ...
data Cli = ServerCli Server.Cli

cliParser :: O.Parser Cli
cliParser =
  O.hsubparser
    ( O.command "server" (O.info (ServerCli <$> Server.cliParser) (O.progDesc "Admin command"))
    )

runCli :: Cli -> IO ()
runCli (ServerCli cli) = runServerCli cli

runServerCli :: Server.Cli -> IO ()
runServerCli Server.Cli{..} = do
  CLog.setLogDeviceDbgLevel CLog.C_DBG_ERROR
  Log.setDefaultLoggerLevel logLevel
  runServerCli' cliOpts command

runServerCli' :: Server.CliOpts -> Server.Command -> IO ()
runServerCli' s (Server.ServerSqlCmd opts)  = Server.serverSqlRepl s opts
runServerCli' s (Server.ServerAdminCmd adminCmd) = do
  let (needLookup, rId) = checkLookup adminCmd
  if needLookup
    then do
      (host, port) <- Server.withAdminClient s $ Server.sendLookupCommand (getResourceType adminCmd) rId
      cmd <- Text.unwords . map Text.pack <$> getArgs
      putStrLn =<< Server.formatCommandResponse
               =<< Server.withAdminClient' (TE.encodeUtf8 host) (fromIntegral port) (Server.sendAdminCommand cmd)
    else do
      cmd <- Text.unwords . map Text.pack <$> getArgs
      putStrLn =<< Server.formatCommandResponse
               =<< Server.withAdminClient s (Server.sendAdminCommand cmd)
 where
  checkLookup :: Server.AdminCommand -> (Bool, Text.Text)
  checkLookup cmd =
    case cmd of
      Server.AdminQueryCommand (Server.QueryCmdResume qid) -> (True, qid)
      Server.AdminConnectorCommand (Server.ConnectorCmdRecover cId) -> (True, cId)
      _                                                    -> (False, "")

-- TODO
runStoreCli :: [String] -> IO ()
runStoreCli args = do
  out <- readProcess "hadmin-store" args ""
  putStr $ dropWhile (`elem` ("\ESC7\ESC[10000;10000H" :: String)) out
