{-# LANGUAGE BangPatterns #-}

module Main (main) where

import qualified Data.Text                    as T
import qualified Data.Text.IO                 as TIO
import           Numeric                      (showFFloat)
import           Options.Applicative          ((<**>))
import qualified Options.Applicative          as O
import           System.Environment           (getArgs)
import           Z.IO.Time                    (SystemTime (..), getSystemTime')

import qualified HStream.Admin.Server.Command as Server
import qualified HStream.Admin.Server.Types   as Server
import qualified HStream.Admin.Store.API      as Store hiding (checkImpact)
import qualified HStream.Admin.Store.Command  as Store
import qualified HStream.Admin.Store.Types    as Store
import           HStream.Admin.Types
import qualified HStream.Logger               as Log
import qualified HStream.Store.Logger         as CLog

main :: IO ()
main = runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
  where
    opts = O.info
      (cliParser <**> O.helper)
      (O.fullDesc <> O.header "======= HStream Admin CLI =======")

runCli :: Cli -> IO ()
runCli (ServerCli cli) = runServerCli cli
runCli (StoreCli cli)  = runStoreCli cli

runServerCli :: Server.Cli -> IO ()
runServerCli Server.Cli{..} = do
  CLog.setLogDeviceDbgLevel CLog.C_DBG_ERROR
  Log.setLogLevel logLevel True
  runServerCli' cliOpts command

runServerCli' :: Server.CliOpts -> Server.Command -> IO ()
runServerCli' s (Server.ServerSqlCmd opts)  = Server.serverSqlRepl s opts
runServerCli' s (Server.ServerAdminCmd _) = do
  cmd <- T.unwords . map T.pack <$> getArgs
  putStrLn =<< Server.formatCommandResponse
           =<< Server.withAdminClient s (Server.sendAdminCommand cmd)

runStoreCli :: Store.Cli -> IO ()
runStoreCli Store.Cli{..} =
  Log.setLogLevel logLevel True >> runStoreCli' headerConfig command

runStoreCli' :: Store.HeaderConfig Store.AdminAPI -> Store.Command -> IO ()
runStoreCli' s (Store.StatusCmd statusOpts) = printTime $ putStrLn =<< Store.runStatus s statusOpts
runStoreCli' s (Store.NodesConfigCmd cmd) = printTime $ Store.runNodesConfigCmd s cmd
runStoreCli' s (Store.ConfigCmd _) = printTime $ TIO.putStrLn =<< Store.dumpConfig s
runStoreCli' s (Store.LogsCmd cmd) = printTime $ Store.runLogsCmd s cmd
runStoreCli' s (Store.CheckImpactCmd checkImpactOpts) = printTime $ Store.checkImpact s checkImpactOpts
runStoreCli' s (Store.MaintenanceCmd opts) = printTime $ Store.runMaintenanceCmd s opts
runStoreCli' s (Store.StartSQLReplCmd opts) = Store.startSQLRepl s opts

-------------------------------------------------------------------------------

printTime :: IO a -> IO a
printTime f = do
  MkSystemTime sec nano <- getSystemTime'
  let !start = fromIntegral sec + fromIntegral nano * 1e-9
  !x <- f
  MkSystemTime sec' nano' <- getSystemTime'
  let !end = fromIntegral sec' + fromIntegral nano' * 1e-9
  putStrLn $ "Took " <> showFFloat @Double (Just 3) (end - start) "s"
  return x
