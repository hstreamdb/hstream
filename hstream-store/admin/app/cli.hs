{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Applicative         (liftA2)
import qualified Data.Text.IO                as TIO
import           Numeric                     (showFFloat)
import           Options.Applicative         ((<**>))
import qualified Options.Applicative         as O
import           Z.IO.Time                   (SystemTime (..), getSystemTime')

import qualified HStream.Store.Admin.API     as AA
import           HStream.Store.Admin.Command
import           HStream.Store.Admin.Types

main :: IO ()
main = uncurry runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
  where
    opts = O.info
      (cli <**> O.helper)
      (O.fullDesc <> O.header "======= HStore Admin CLI =======")

cli :: O.Parser (AA.HeaderConfig AA.AdminAPI, Command)
cli = liftA2 (,) headerConfigParser commandParser

runCli :: AA.HeaderConfig AA.AdminAPI -> Command -> IO ()
runCli s (StatusCmd statusOpts) = printTime $ putStrLn =<< runStatus s statusOpts
runCli s (NodesConfigCmd (NodesConfigShow c)) = printTime $ TIO.putStrLn =<< showConfig s c
runCli s (NodesConfigCmd (NodesConfigBootstrap ps)) = printTime $ bootstrap s ps
runCli s (ConfigCmd _) = printTime $ TIO.putStrLn =<< dumpConfig s
runCli s (LogsCmd cmd) = printTime $ runLogsCmd cmd

printTime :: IO a -> IO a
printTime f = do
  MkSystemTime sec nano <- getSystemTime'
  !x <- f
  MkSystemTime sec' nano' <- getSystemTime'
  putStrLn $ "Took " <> showFFloat @Double (Just 3) (fromIntegral (sec' - sec) + fromIntegral (nano' - nano) * 10e-9) "s"
  return x

data Command
  = StatusCmd StatusOpts
  | NodesConfigCmd NodesConfigOpts
  | ConfigCmd ConfigCmdOpts
  | LogsCmd LogsSubCmd
  deriving (Show)

commandParser :: O.Parser Command
commandParser = O.hsubparser
  ( O.command "status" (O.info (StatusCmd <$> statusParser) (O.progDesc "Cluster status"))
 <> O.command "nodes-config" (O.info (NodesConfigCmd <$> nodesConfigParser) (O.progDesc "Manipulates the cluster's NodesConfig"))
 <> O.command "config" (O.info (ConfigCmd <$> configCmdParser) (O.progDesc "Commands about logdevice config"))
 <> O.command "logs" (O.info (LogsCmd <$> logsSubCmdParser) (O.progDesc "Control the logs config of logdevice dynamically"))
  )
