module Main (main) where

import qualified Data.Text             as T
import           Options.Applicative   ((<**>))
import qualified Options.Applicative   as O
import           System.Environment    (getArgs)

import           HStream.Admin.Command
import           HStream.Admin.Types
import qualified HStream.Logger        as Log
import qualified HStream.Store.Logger  as CLog

main :: IO ()
main = runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
  where
    opts = O.info
      (cliParser <**> O.helper)
      (O.fullDesc <> O.header "======= HStream Admin CLI =======")

runCli :: Cli -> IO ()
runCli Cli{..} = do
  CLog.setLogDeviceDbgLevel CLog.C_DBG_ERROR
  Log.setLogLevel logLevel True
  runCli' cliOpts command

runCli' :: CliOpts -> Command -> IO ()
runCli' s (ServerSqlCmd opts)  = serverSqlRepl s opts
runCli' s (ServerAdminCmd _) = do
  cmd <- T.unwords . map T.pack <$> getArgs
  putStrLn =<< formatCommandResponse =<< withAdminClient s (sendAdminCommand cmd)
