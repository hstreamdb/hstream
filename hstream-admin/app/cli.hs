{-# LANGUAGE BangPatterns #-}

module Main where

import qualified Data.Text.IO                as TIO
import           Numeric                     (showFFloat)
import           Options.Applicative         ((<**>))
import qualified Options.Applicative         as O
import           Z.IO.Time                   (SystemTime (..), getSystemTime')

import qualified HStream.Logger              as Log
import qualified HStream.Store.Admin.API     as AA
import           HStream.Store.Admin.Command
import           HStream.Store.Admin.Types

main :: IO ()
main = runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
  where
    opts = O.info
      (cliParser <**> O.helper)
      (O.fullDesc <> O.header "======= HStore Admin CLI =======")

runCli :: Cli -> IO ()
runCli Cli{..} = Log.setLogLevel logLevel True >> runCli' headerConfig command

runCli' :: AA.HeaderConfig AA.AdminAPI -> Command -> IO ()
runCli' s (StatusCmd statusOpts) = printTime $ putStrLn =<< runStatus s statusOpts
runCli' s (NodesConfigCmd cmd) = printTime $ runNodesConfigCmd s cmd
runCli' s (ConfigCmd _) = printTime $ TIO.putStrLn =<< dumpConfig s
runCli' s (LogsCmd cmd) = printTime $ runLogsCmd s cmd
runCli' s (CheckImpactCmd checkImpactOpts) = printTime $ checkImpact s checkImpactOpts
runCli' s (MaintenanceCmd opts) = printTime $ runMaintenanceCmd s opts
runCli' s (StartSQLReplCmd opts) = startSQLRepl s opts

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
