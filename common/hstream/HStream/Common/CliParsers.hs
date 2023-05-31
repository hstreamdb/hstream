module HStream.Common.CliParsers
  ( streamParser
  , subscriptionParser
  ) where

import qualified Options.Applicative       as O
import           Proto3.Suite              (Enumerated (Enumerated))

import           HStream.Instances         ()
import qualified HStream.Server.HStreamApi as API

streamParser :: O.Parser API.Stream
streamParser = API.Stream
  <$> O.strArgument (O.metavar "STREAM_NAME"
                 <> O.help "The name of the stream"
                  )
  <*> O.option O.auto ( O.long "replication-factor"
                     <> O.short 'r'
                     <> O.metavar "INT"
                     <> O.showDefault
                     <> O.value 1
                     <> O.help "The replication factor for the stream"
                      )
  <*> O.option O.auto ( O.long "backlog-duration"
                     <> O.short 'b'
                     <> O.metavar "INT"
                     <> O.showDefault
                     <> O.value 0
                     <> O.help "The backlog duration of records in stream in seconds"
                      )
  <*> O.option O.auto ( O.long "shards"
                     <> O.short 's'
                     <> O.metavar "INT"
                     <> O.showDefault
                     <> O.value 1
                     <> O.help "The number of shards the stream should have"
                      )
  <*> pure Nothing

subscriptionParser :: O.Parser API.Subscription
subscriptionParser = API.Subscription
  <$> O.strArgument ( O.help "Subscription ID" <> O.metavar "SUB_ID" <> O.help "The ID of the subscription")
  <*> O.strOption ( O.long "stream" <> O.metavar "STREAM_NAME"
                 <> O.help "The stream associated with the subscription" )
  <*> O.option O.auto ( O.long "ack-timeout" <> O.metavar "INT" <> O.value 60
                     <> O.help "Timeout for acknowledgements in seconds")
  <*> O.option O.auto ( O.long "max-unacked-records" <> O.metavar "INT"
                     <> O.value 10000
                     <> O.help "Maximum number of unacked records allowed per subscription")
  <*> (Enumerated . Right <$> O.option O.auto ( O.long "offset"
                                     <> O.metavar "[earliest|latest]"
                                     <> O.value (API.SpecialOffsetLATEST)
                                     <> O.help "The offset of the subscription to start from"
                                      )
    )
  <*> pure Nothing
