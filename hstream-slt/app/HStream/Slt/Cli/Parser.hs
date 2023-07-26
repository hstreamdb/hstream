module Slt.Cli.Parser where

import           Options.Applicative

mainOptsParser :: IO Opts
mainOptsParser = execParser $ info (helper <*> parseOpts) mempty

----------------------------------------
-- Command Line Options Parser
----------------------------------------

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

parseOpts :: Parser Opts
parseOpts = Opts <$> parseGlobalOpts <*> parseCmd

parseGlobalOpts :: Parser GlobalOpts
parseGlobalOpts = GlobalOpts <$> parseDebug <*> parseExecutorsAddr

parseDebug :: Parser Bool
parseDebug = switch (long "debug" <> help "Enable debug mode")

parseExecutorsAddr :: Parser [(ExecutorKind, String)]
parseExecutorsAddr = many parseExecutorAddr

parseExecutorAddr :: Parser (ExecutorKind, String)
parseExecutorAddr = (,) <$> parseExecutorKind <*> strOption (long "executor" <> metavar "ADDR" <> help "Executor address")

parseCmd :: Parser Cmd
parseCmd = parseCmdParse <|> parseCmdExec <|> parseCmdComplete

parseCmdParse :: Parser Cmd
parseCmdParse = CmdParse . ParseOpts <$> some (argument str (metavar "FILES..."))

parseCmdExec :: Parser Cmd
parseCmdExec = CmdExec <$> parseExecOpts

parseCmdComplete :: Parser Cmd
parseCmdComplete = pure $ CmdComplete CompleteOpts

parseExecOpts :: Parser ExecOpts
parseExecOpts =
  ExecOpts
    <$> some (argument str (metavar "FILES..."))
    <*> many parseExecutorKind

parseExecutorKind :: Parser ExecutorKind
parseExecutorKind =
  h ExecutorKindHStream <|> h ExecutorKindSQLite
  where
    h x = flag' x (long $ strExecutorKind x)

----------------------------------------
-- Misc
----------------------------------------

data ExecutorKind
  = ExecutorKindHStream
  | ExecutorKindSQLite
  deriving (Show)

strExecutorKind :: ExecutorKind -> String
strExecutorKind = \case
  ExecutorKindHStream -> "hstream"
  ExecutorKindSQLite  -> "sqlite"
