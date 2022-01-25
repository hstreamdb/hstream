module HStream.Admin.Types
  ( Cli (..)
  , cliParser
  ) where

import qualified Options.Applicative        as O

import qualified HStream.Admin.Server.Types as Server
import qualified HStream.Admin.Store.Types  as Store

data Cli = ServerCli Server.Cli
         | StoreCli Store.Cli

cliParser :: O.Parser Cli
cliParser = O.hsubparser
  ( O.command "server" (O.info (ServerCli <$> Server.cliParser) (O.progDesc "Admin command"))
 <> O.command "store" (O.info (StoreCli <$> Store.cliParser) (O.progDesc "Internal store admin command"))
  )
