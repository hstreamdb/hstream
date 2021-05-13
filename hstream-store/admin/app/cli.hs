{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Applicative             (liftA2)
import qualified Data.Text.IO                    as TIO
import           Numeric                         (showFFloat)
import           Options.Applicative             ((<**>))
import qualified Options.Applicative             as O
import           Z.IO.Time                       (SystemTime (..),
                                                  getSystemTime')

import qualified HStream.Store.Admin.API         as AA
import           HStream.Store.Admin.NodesConfig
import           HStream.Store.Admin.Status
import           HStream.Store.Admin.Types

main :: IO ()
main = uncurry runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
  where
    opts = O.info
      (cli <**> O.helper)
      (O.fullDesc <> O.header "======= HStore Admin CLI =======")

cli :: O.Parser (AA.SocketConfig AA.AdminAPI, Command)
cli = liftA2 (,) socketConfigParser commandParser

runCli :: AA.SocketConfig AA.AdminAPI -> Command -> IO ()
runCli s (StatusCmd statusOpts) = printTime $ putStrLn =<< runStatus s statusOpts
runCli s (NodesConfigCmd (NodesConfigShow c)) = printTime $ TIO.putStrLn =<< showConfig s c
runCli s (NodesConfigCmd (NodesConfigBootstrap ps)) = printTime $ bootstrap s ps

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
  deriving (Show)

commandParser :: O.Parser Command
commandParser = O.hsubparser
  ( O.command "status" (O.info (StatusCmd <$> statusParser) (O.progDesc "Cluster status"))
 <> O.command "nodes-config" (O.info (NodesConfigCmd <$> nodesConfigParser) (O.progDesc "Manipulates the cluster's NodesConfig"))
  )

socketConfigParser :: O.Parser (AA.SocketConfig AA.AdminAPI)
socketConfigParser = AA.SocketConfig
  <$> O.strOption ( O.long "host"
                 <> O.metavar "HOST"
                 <> O.showDefault
                 <> O.value "127.0.0.1"
                 <> O.help "Admin server host, e.g. ::1"
                  )
  <*> O.option O.auto ( O.long "port"
                     <> O.metavar "PORT"
                     <> O.help "Admin server port"
                      )
  <*> O.option O.auto ( O.long "protocol"
                     <> O.metavar "INT"
                     <> O.showDefault
                     <> O.value AA.binaryProtocolId
                     <> O.help "Protocol id, 0 for binary, 2 for compact"
                      )
