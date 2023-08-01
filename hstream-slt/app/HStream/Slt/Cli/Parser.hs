module HStream.Slt.Cli.Parser where

import           Options.Applicative

data Opts = Opts
  { globalOpts :: GlobalOpts,
    globalCmd  :: Cmd
  }
  deriving (Show)

data GlobalOpts = GlobalOpts
  { debug         :: Bool,
    executorsAddr :: [(ExecutorKind, String)]
  }
  deriving (Show)

data Cmd
  = CmdParse ParseOpts
  | CmdExec ExecOpts
  | CmdComplete CompleteOpts
  deriving (Show)

newtype ParseOpts = ParseOpts {unParseOpts :: [FilePath]}
  deriving (Show)

data ExecOpts = ExecOpts
  { files     :: [FilePath],
    executors :: [ExecutorKind]
  }
  deriving (Show)

data CompleteOpts = CompleteOpts
  deriving (Show)

data ExecutorKind
  = ExecutorKindHStream
  | ExecutorKindSQLite
  deriving (Show)

strExecutorKind :: ExecutorKind -> String
strExecutorKind = \case
  ExecutorKindHStream -> "hstream"
  ExecutorKindSQLite  -> "sqlite"

executorKindOption :: ReadM ExecutorKind
executorKindOption =
  eitherReader $ \case
    "hstream" -> Right ExecutorKindHStream
    "sqlite"  -> Right ExecutorKindSQLite
    _         -> Left "Invalid executor kind"

executorKindOption' :: Parser ExecutorKind
executorKindOption' = option executorKindOption (short 'e' <> metavar "EXECUTOR")

pParseOpts :: Parser ParseOpts
pParseOpts = ParseOpts <$> some (argument str (metavar "FILES..."))

pExecOpts :: Parser ExecOpts
pExecOpts =
  ExecOpts
    <$> some (argument str (metavar "FILES..."))
    <*> many executorKindOption'

pCompleteOpts :: Parser CompleteOpts
pCompleteOpts = pure CompleteOpts

pGlobalOpts :: Parser GlobalOpts
pGlobalOpts =
  GlobalOpts
    <$> switch (short 'd' <> long "debug" <> help "Enable debug mode")
    <*> many
      ( (ExecutorKindHStream,) <$> strOption (long "addr-hstream" <> metavar "ADDRESS" <> help "Set the address for the HStream executor")
          <|> (ExecutorKindSQLite,) <$> strOption (long "addr-sqlite" <> metavar "ADDRESS" <> help "Set the address for the SQLite executor")
      )

parseCmd :: Parser Cmd
parseCmd = CmdParse <$> pParseOpts

execCmd :: Parser Cmd
execCmd = CmdExec <$> pExecOpts

completeCmd :: Parser Cmd
completeCmd = CmdComplete <$> pCompleteOpts

pOpts :: Parser Opts
pOpts =
  Opts
    <$> pGlobalOpts
    <*> subparser
      ( command "parse" (info parseCmd (progDesc "Parse command"))
          <> command "exec" (info execCmd (progDesc "Exec command"))
          <> command "complete" (info completeCmd (progDesc "Complete command"))
      )

mainOptsParser :: IO Opts
mainOptsParser = execParser (info (pOpts <**> helper) idm)
