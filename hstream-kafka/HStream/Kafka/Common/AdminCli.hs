module HStream.Kafka.Common.AdminCli
  ( AdminCommand (..)
  , parseAdminCommand
  , adminCommandParser
  ) where

import qualified Options.Applicative        as O
import qualified Options.Applicative.Help   as O

import qualified HStream.Admin.Server.Types as AT

data AdminCommand
  = AdminInitCommand
  | AdminCheckReadyCommand
  | AdminStatusCommand
  deriving (Show, Eq)

adminCommandParser :: O.Parser AdminCommand
adminCommandParser = O.hsubparser
  ( O.command "init" (O.info (pure AdminInitCommand)
                             (O.progDesc "Init an hserver kafka cluster"))
 <> O.command "ready" (O.info (pure AdminCheckReadyCommand)
                              (O.progDesc "Check if an hserver kafka cluster is ready"))
 <> O.command "status" (O.info (pure AdminStatusCommand)
                               (O.progDesc "Cluster status"))
  )

parseAdminCommand :: [String] -> IO AdminCommand
parseAdminCommand args = execParser
  where
    execParser = handleParseResult $ O.execParserPure O.defaultPrefs cliInfo args
    cliInfo = O.info adminCommandParser
                     (O.progDesc "The parser to use for admin commands")

handleParseResult :: O.ParserResult a -> IO a
handleParseResult (O.Success a) = return a
handleParseResult (O.Failure failure) = do
  let (h, _exit, _cols) = O.execFailure failure ""
      errmsg = (O.displayS . O.renderCompact . O.extractChunk $ O.helpError h) ""
  AT.throwParsingErr errmsg
handleParseResult (O.CompletionInvoked compl) =
  AT.throwParsingErr =<< O.execCompletion compl ""
