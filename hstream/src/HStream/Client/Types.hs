{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Client.Types where

import           Control.Concurrent            (MVar)
import           Data.ByteString               (ByteString)
import qualified Data.ByteString               as BS
import           Data.List.Split               (splitOn)
import           Data.Maybe                    (isNothing)
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as T
import           Data.Word                     (Word32, Word64)
import           Network.GRPC.HighLevel.Client (ClientConfig (..),
                                                ClientSSLConfig (..),
                                                ClientSSLKeyCertPair (..))
import           Network.URI
import qualified Options.Applicative           as O
import           Text.Read                     (readMaybe)

import           HStream.Admin.Server.Types    (subscriptionParser)
import qualified HStream.Server.HStreamApi     as API
import           HStream.Server.Types          (ServerID)
import           HStream.Utils                 (ResourceType, SocketAddr (..),
                                                mkGRPCClientConfWithSSL)
import           Proto3.Suite                  (Enumerated (Enumerated))

data HStreamCommand = HStreamCommand
  { cliConnOpts :: CliConnOpts
  , cliCommand  :: Command
  }

data Resource = Resource ResourceType Text

data Command
  = HStreamSql HStreamSqlOpts
  | HStreamNodes HStreamNodes
  | HStreamInit HStreamInitOpts
  | HStreamStream StreamCommand
  | HStreamSubscription SubscriptionCommand

commandParser :: O.Parser HStreamCommand
commandParser = HStreamCommand
  <$> connOptsParser
  <*> O.hsubparser
    (  O.command "sql"   (O.info (HStreamSql <$> hstreamSqlOptsParser) (O.progDesc "Start HStream SQL Shell"))
    <> O.command "node" (O.info (HStreamNodes <$> hstreamNodesParser) (O.progDesc "Manage HStream Server Cluster"))
    <> O.command "init"  (O.info (HStreamInit <$> hstreamInitOptsParser ) (O.progDesc "Init HStream Server Cluster"))
    <> O.command "stream"        (O.info (HStreamStream <$> streamCmdParser ) (O.progDesc "Manage Streams in HStreamDB"))
    <> O.command "subscription"  (O.info (HStreamSubscription <$> subscriptionCmdParser) (O.progDesc "Manage Subscriptions in HStreamDB (`sub` is an alias for this command"))
    -- Also see: https://github.com/pcapriotti/optparse-applicative#command-groups
    <> O.command "sub"  (O.info (HStreamSubscription <$> subscriptionCmdParser) (O.progDesc "Alias for the command `subscription`"))
    )

data StreamCommand
  = StreamCmdList
  | StreamCmdCreate API.Stream
  | StreamCmdDelete Text Bool
  | StreamCmdDescribe Text
  | StreamCmdListShard Text
  | StreamCmdReadShard ReadShardArgs
  deriving (Show)

streamCmdParser :: O.Parser StreamCommand
streamCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure StreamCmdList) (O.progDesc "Get all streams"))
 <> O.command "create" (O.info (StreamCmdCreate <$> streamParser) (O.progDesc "Create a stream"))
 <> O.command "describe" (O.info (StreamCmdDescribe <$> O.strArgument ( O.metavar "STREAM_NAME"
                                                                      <> O.help "The name of the stream"))
                               (O.progDesc "Get the details of a stream"))
 <> O.command "delete" (O.info (StreamCmdDelete <$> O.strArgument ( O.metavar "STREAM_NAME"
                                                               <> O.help "The name of the stream to delete")
                                                <*> O.switch ( O.long "force"
                                                            <> O.short 'f'
                                                            <> O.help "Whether to enable force deletion" ))
                               (O.progDesc "Delete a stream")
                        )
 <> O.command "list-shard" (O.info (StreamCmdListShard <$> O.strArgument
                                                               ( O.metavar "STREAM_NAME"
                                                              <> O.help "The name of the stream to be queried"))
                             (O.progDesc "List shards of specific stream"))
 <> O.command "read-shard" (O.info (StreamCmdReadShard <$> readShardRequestParser)
                             (O.progDesc "Read records from specific shard"))
  )

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

data ReadShardArgs = ReadShardArgs
  { shardIdArgs     :: Word64
  , shardOffsetArgs :: Maybe API.ShardOffset
  } deriving (Show)

readShardRequestParser :: O.Parser ReadShardArgs
readShardRequestParser = ReadShardArgs
  <$> O.argument O.auto ( O.metavar "SHARD_ID" <> O.help "The shard you want to read" )
  <*> (Just <$> offsetParser)

offsetParser :: O.Parser API.ShardOffset
offsetParser = specialOffsetParser O.<|> recordOffsetParser O.<|> timestampOffsetParser
  O.<|> (pure . API.ShardOffset . Just . API.ShardOffsetOffsetSpecialOffset . Enumerated . Right $ API.SpecialOffsetEARLIEST)

specialOffsetParser :: O.Parser API.ShardOffset
specialOffsetParser = API.ShardOffset . Just . API.ShardOffsetOffsetSpecialOffset . Enumerated . Right
  <$> (specialOffsetToAPI <$> specialOffsetParser')

data SpecialOffset = Earliest | Latest

specialOffsetToAPI :: SpecialOffset -> API.SpecialOffset
specialOffsetToAPI offset = case offset of
  Earliest -> API.SpecialOffsetEARLIEST
  Latest   -> API.SpecialOffsetLATEST

specialOffsetParser' :: O.Parser SpecialOffset
specialOffsetParser' =
   O.flag' Latest ( O.long "latest" <> O.help "Read from latest offset" )
   O.<|> O.flag' Earliest ( O.long "earliest" <> O.help "Read from earliest offset" )

recordOffsetParser :: O.Parser API.ShardOffset
recordOffsetParser = API.ShardOffset . Just . API.ShardOffsetOffsetRecordOffset <$> recordIdParser

recordIdParser :: O.Parser API.RecordId
recordIdParser = O.option parseRecordId ( O.long "record"
                                       <> O.metavar "RECORDID"
                                       <> O.help "Read from specific recordId")

parseRecordId :: O.ReadM API.RecordId
parseRecordId = O.eitherReader parseRecordId'
 where
   parseRecordId' :: String -> Either String API.RecordId
   parseRecordId' s =
    let res = splitOn "-" s
     in if length res /= 3 then Left $ "invalied recordId " <> s
                           else Right API.RecordId { recordIdShardId = read $ head res
                                                   , recordIdBatchId = read $ res !! 1
                                                   , recordIdBatchIndex = read $ res !! 2
                                                   }

timestampOffsetParser :: O.Parser API.ShardOffset
timestampOffsetParser = API.ShardOffset . Just . API.ShardOffsetOffsetTimestampOffset <$> timestampParser

timestampParser :: O.Parser API.TimestampOffset
timestampParser = API.TimestampOffset
  <$> O.option O.auto ( O.long "timestamp"
                       <> O.metavar "TIMESTAMP"
                       <> O.help "Read from specific millisecond time stamp")
  <*> (not <$> O.switch ( O.long "approximate"
              <> O.help "Use approximate timestamp"))

data SubscriptionCommand
  = SubscriptionCmdList
  | SubscriptionCmdCreate API.Subscription
  | SubscriptionCmdDelete Text Bool
  | SubscriptionCmdDescribe Text
  deriving (Show)

subscriptionCmdParser :: O.Parser SubscriptionCommand
subscriptionCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure SubscriptionCmdList) (O.progDesc "Get all subscriptions"))
 <> O.command "create" (O.info (SubscriptionCmdCreate <$> subscriptionParser)
                               (O.progDesc "Create a subscription"))
 <> O.command "describe" (O.info (SubscriptionCmdDescribe <$> O.strArgument ( O.metavar "SUB_ID"
                                                                           <> O.help "The ID of the subscription"))
                                 (O.progDesc "Get the details of a subscription"))
 <> O.command "delete" (O.info (SubscriptionCmdDelete <$> O.strArgument ( O.metavar "SUB_ID"
                                                                      <> O.help "The ID of the subscription")
                                                      <*> O.switch ( O.long "force"
                                                                  <> O.short 'f' ))
                               (O.progDesc "Delete a subscription")
                       )
  )

data HStreamCliContext = HStreamCliContext
  { availableServers :: MVar [SocketAddr]
  , currentServer    :: MVar SocketAddr
  , sslConfig        :: Maybe ClientSSLConfig
  }

data HStreamSqlContext = HStreamSqlContext
  { hstreamCliContext :: HStreamCliContext
  , updateInterval    :: Int
  , retryLimit        :: Word32
  , retryInterval     :: Word32
  }

data HStreamSqlOpts = HStreamSqlOpts
  { _updateInterval :: Int
  , _retryInterval  :: Word32
  , _retryLimit     :: Word32
  , _execute        :: Maybe String
  , _historyFile    :: Maybe FilePath
  }

hstreamSqlOptsParser :: O.Parser HStreamSqlOpts
hstreamSqlOptsParser = HStreamSqlOpts
  <$> O.option O.auto (O.long "update-interval" <> O.metavar "INT" <> O.showDefault <> O.value 30 <> O.help "interval to update available servers in seconds")
  <*> O.option O.auto (O.long "retry-interval" <> O.metavar "INT" <> O.showDefault <> O.value 5 <> O.help "interval to retry request to server")
  <*> O.option O.auto (O.long "retry-limit" <> O.metavar "INT" <> O.showDefault <> O.value 3 <> O.help "maximum number of retries allowed")

  <*> (O.optional . O.option O.str) (O.long "execute" <> O.short 'e' <> O.metavar "STRING" <> O.help "execute the statement and quit")
  <*> (O.optional . O.option O.str) (O.long "history-file" <> O.metavar "STRING" <> O.help "history file path to write interactively executed statements")

data HStreamNodes
  = HStreamNodesList
  | HStreamNodesStatus (Maybe ServerID)
  | HStreamNodesCheck (Maybe Word32)

hstreamNodesParser :: O.Parser HStreamNodes
hstreamNodesParser = O.hsubparser
  (  O.command "list"     (O.info (pure HStreamNodesList) (O.progDesc "List all running nodes in the cluster"))
  <> O.command "status"   (O.info (HStreamNodesStatus <$> (O.optional . O.option O.auto) (O.long "id" <> O.help "Specify the id of the node"))
                                  (O.progDesc "Show the status of nodes specified, if not specified show the status of all nodes"))
  <> O.command "check-running" (O.info (HStreamNodesCheck <$> (O.optional . O.option O.auto) (O.long "minimum-running" <> O.short 'n' <> O.help "Specify minimum number of the nodes") )
                                       (O.progDesc "Check if all nodes in the the cluster are running, and the number of nodes is at least as specified"))
  )

newtype HStreamInitOpts = HStreamInitOpts { _timeoutSec :: Int }

hstreamInitOptsParser :: O.Parser HStreamInitOpts
hstreamInitOptsParser = HStreamInitOpts
 <$> O.option O.auto (O.long "timeout" <> O.metavar "INT" <> O.showDefault <> O.value 5 <> O.help "timeout for the wait of cluster ready")

data CliConnOpts = CliConnOpts
  { _serverHost   :: ByteString
  , _serverPort   :: Int
  , _tlsCa        :: Maybe FilePath
  , _tlsKey       :: Maybe FilePath
  , _tlsCert      :: Maybe FilePath
  , _retryTimeout :: Int
  , _serviceUri   :: Maybe URI
  } deriving (Show, Eq)

serverHost :: O.Parser ByteString
serverHost =
  O.strOption ( O.long "host" <> O.metavar "SERVER-HOST"
              <> O.showDefault <> O.value "127.0.0.1"
              <> O.help "Server host value"
              )

serverPort :: O.Parser Int
serverPort =
  O.option O.auto ( O.long "port" <> O.metavar "INT"
                  <> O.showDefault <> O.value 6570
                  <> O.help "Server port value"
                  )

connOptsParser :: O.Parser CliConnOpts
connOptsParser = CliConnOpts
  <$> serverHost
  <*> serverPort
  <*> (O.optional . O.option O.str) (O.long "tls-ca"   <> O.metavar "STRING" <> O.help "path name of the file that contains list of trusted TLS Certificate Authorities")
  <*> (O.optional . O.option O.str) (O.long "tls-key"  <> O.metavar "STRING" <> O.help "path name of the client TLS private key file")
  <*> (O.optional . O.option O.str) (O.long "tls-cert" <> O.metavar "STRING" <> O.help "path name of the client TLS public key certificate file")
  <*> O.option O.auto (O.long "retry-timeout"   <> O.metavar "INT" <> O.showDefault <> O.value 60 <> O.help "timeout to retry connecting to a server in seconds")
  <*> (O.optional . O.option (O.maybeReader parseURI)) (O.long "service-url"  <> O.help "The endpoint to connect to")

data RefinedCliConnOpts = RefinedCliConnOpts {
    addr         :: SocketAddr
  , clientConfig :: ClientConfig
  , retryTimeout :: Int
  }

refineCliConnOpts :: CliConnOpts -> IO RefinedCliConnOpts
refineCliConnOpts CliConnOpts {..} = do
  clientSSLKeyCertPair <- do
    case _tlsKey of
      Nothing -> case _tlsCert of
        Nothing -> pure Nothing
        Just _  -> errorWithoutStackTrace "got `tls-cert`, but `tls-key` is missing"
      Just tlsKey -> case _tlsCert of
        Nothing      -> errorWithoutStackTrace "got `tls-key`, but `tls-cert` is missing"
        Just tlsCert -> pure . Just $ ClientSSLKeyCertPair {
          clientPrivateKey = tlsKey
        , clientCert       = tlsCert
        }
  let sslConfig = if isNothing _tlsCa && isNothing clientSSLKeyCertPair
        then Nothing
        else Just $ ClientSSLConfig {
          serverRootCert       = _tlsCa
        , clientSSLKeyCertPair = clientSSLKeyCertPair
        , clientMetadataPlugin = Nothing
        }
  let addr = case _serviceUri of
        Nothing -> SocketAddr _serverHost _serverPort
        Just URI{..}
          | uriScheme == "hstreams:" -> case sslConfig of
              Nothing -> errorWithoutStackTrace "Tls certificates are not provided"
              _       -> uriAuthToSocketAddress uriAuthority
          | uriScheme == "hstream:"  -> uriAuthToSocketAddress uriAuthority
          | otherwise -> errorWithoutStackTrace "Unsupported URI scheme"
  let clientConfig = mkGRPCClientConfWithSSL addr sslConfig
  pure $ RefinedCliConnOpts addr clientConfig _retryTimeout
  where
    uriAuthToSocketAddress (Just URIAuth{..}) =
      let host = T.encodeUtf8 . T.pack $ uriUserInfo <> uriRegName in
      if BS.null host then errorWithoutStackTrace "Incomplete URI"
      else case uriPort of
        []     -> SocketAddr host _serverPort
        (x:xs) -> case readMaybe xs of
          Nothing   -> SocketAddr host _serverPort
          Just port -> SocketAddr host port
    uriAuthToSocketAddress Nothing = errorWithoutStackTrace "Incomplete URI"
