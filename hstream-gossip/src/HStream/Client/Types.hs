module HStream.Client.Types where

import           Data.ByteString     (ByteString)
import           Data.Text           (Text)
import           Data.Word           (Word32)
import qualified Options.Applicative as O

data CliOpts = CliOpts {
    targetHost :: ByteString
  , targetPort :: Word32
  , cliCmd     :: Command}

data Command
  = Join ByteString Word32
  | Status
  | Event Text ByteString

eventName :: O.Parser Text
eventName = O.strOption
  $  O.long "event-name" <> O.metavar "NAME" <> O.short 'n'
  <> O.help "The name of the event"

eventPayload :: O.Parser ByteString
eventPayload = O.strOption
  $  O.long "event-payload" <> O.short 'p'
  <> O.help "The payload of the event"
