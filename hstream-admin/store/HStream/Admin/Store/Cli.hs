{-# LANGUAGE BangPatterns #-}

module HStream.Admin.Store.Cli
  ( runStoreCli
  ) where

import qualified Data.Text.IO                as TIO
import           Numeric                     (showFFloat)
import           Z.IO.Time                   (SystemTime (..), getSystemTime')

import qualified HStream.Admin.Store.API     as Store hiding (checkImpact)
import qualified HStream.Admin.Store.Command as Store
import qualified HStream.Admin.Store.Types   as Store
import qualified HStream.Logger              as Log

runStoreCli :: Store.Cli -> IO ()
runStoreCli Store.Cli{..} =
  Log.setDefaultLoggerLevel logLevel >> runStoreCli' headerConfig command

runStoreCli' :: Store.HeaderConfig Store.AdminAPI -> Store.Command -> IO ()
runStoreCli' s (Store.StatusCmd statusOpts) = printTime $ putStrLn =<< Store.runStatus s statusOpts
runStoreCli' s (Store.NodesConfigCmd cmd) = printTime $ Store.runNodesConfigCmd s cmd
runStoreCli' s (Store.ConfigCmd _) = printTime $ TIO.putStrLn =<< Store.dumpConfig s
runStoreCli' s (Store.LogsCmd cmd) = printTime $ Store.runLogsCmd s cmd
runStoreCli' s (Store.CheckImpactCmd checkImpactOpts) = printTime $ Store.checkImpact s checkImpactOpts
runStoreCli' s (Store.MaintenanceCmd opts) = printTime $ Store.runMaintenanceCmd s opts
runStoreCli' s (Store.StartSQLReplCmd opts) = Store.startSQLRepl s opts

printTime :: IO a -> IO a
printTime f = do
  MkSystemTime sec nano <- getSystemTime'
  let !start = fromIntegral sec + fromIntegral nano * 1e-9
  !x <- f
  MkSystemTime sec' nano' <- getSystemTime'
  let !end = fromIntegral sec' + fromIntegral nano' * 1e-9
  putStrLn $ "Took " <> showFFloat @Double (Just 3) (end - start) "s"
  return x
