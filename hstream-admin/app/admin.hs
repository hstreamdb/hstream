module Main (main) where

import           Options.Applicative   ((<**>))
import qualified Options.Applicative   as O

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
runCli' s (ServerSqlCmd opts) = serverSqlRepl s opts

data Cli = Cli
  { cliOpts  :: CliOpts
  , logLevel :: Log.Level
  , command  :: Command
  }

cliParser :: O.Parser Cli
cliParser = Cli
  <$> cliOptsParser
  <*> logLevelParser
  <*> commandParser

newtype Command
  = ServerSqlCmd ServerSqlCmdOpts
  deriving (Show)

commandParser :: O.Parser Command
commandParser = O.hsubparser
  ( O.command "hserver-sql" (O.info (ServerSqlCmd <$> serverSqlCmdOptsParser)
                                    (O.progDesc "Start an interactive SQL shell"))
  )
