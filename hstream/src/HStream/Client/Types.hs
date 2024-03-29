{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Client.Types where

import           Control.Applicative           ((<|>))
import           Control.Concurrent            (MVar)
import qualified Data.Attoparsec.Text          as AP
import           Data.ByteString               (ByteString)
import qualified Data.ByteString               as BS
import           Data.ByteString.Internal      (c2w)
import           Data.Foldable                 (foldl')
import           Data.Functor                  (($>))
import           Data.Int                      (Int64)
import qualified Data.Map.Strict               as M
import           Data.Maybe                    (isNothing)
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as T
import           Data.Vector                   (Vector)
import           Data.Word                     (Word32, Word64)
import           HStream.Common.CliParsers     (streamParser,
                                                subscriptionParser)
import           HStream.Common.Types
import qualified HStream.Server.HStreamApi     as API
import           HStream.Server.Types          (ServerID)
import qualified HStream.Store                 as S
import           HStream.Utils                 (ResourceType, SocketAddr (..),
                                                mkGRPCClientConfWithSSL,
                                                textToCBytes)
import           Network.GRPC.HighLevel.Client (ClientConfig (..),
                                                ClientSSLConfig (..),
                                                ClientSSLKeyCertPair (..))
import           Network.URI
import           Options.Applicative           (eitherReader)
import qualified Options.Applicative           as O
import           Options.Applicative.Types
import           Proto3.Suite                  (Enumerated (Enumerated))
import qualified Text.Read                     as Read

data CliCmd = CliCmd HStreamCommand | GetVersionCmd

cliCmdParser :: O.Parser CliCmd
cliCmdParser = CliCmd <$> commandParser
  O.<|> O.flag' GetVersionCmd ( O.long "version" <> O.short 'v' <> O.help "Get client version")

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
  | HStreamConnector ConnectorCommand

commandParser :: O.Parser HStreamCommand
commandParser = HStreamCommand
  <$> connOptsParser
  <*> O.hsubparser
      (  O.command "sql" (O.info (HStreamSql <$> hstreamSqlOptsParser) (O.progDesc "Start HStream SQL Shell"))
      <> O.command "node" (O.info (HStreamNodes <$> hstreamNodesParser) (O.progDesc "Manage HStream Server Cluster"))
      <> O.command "init" (O.info (HStreamInit <$> hstreamInitOptsParser ) (O.progDesc "Init HStream Server Cluster"))
      <> O.command "stream" (O.info (HStreamStream <$> streamCmdParser ) (O.progDesc "Manage Streams in HStreamDB"))
      <> O.command "subscription" (O.info (HStreamSubscription <$> subscriptionCmdParser) (O.progDesc "Manage Subscriptions in HStreamDB (`sub` is an alias for this command)"))
      -- Also see: https://github.com/pcapriotti/optparse-applicative#command-groups
      <> O.command "sub" (O.info (HStreamSubscription <$> subscriptionCmdParser) (O.progDesc "Alias for the command `subscription`"))
      <> O.command "connector" (O.info (HStreamConnector <$> connectorCmdParser) (O.progDesc "Manage Connector in HStreamDB"))
      )

data StreamCommand
  = StreamCmdList
  | StreamCmdCreate API.Stream
  | StreamCmdDelete Text Bool
  | StreamCmdDescribe Text
  | StreamCmdListShard Text
  | StreamCmdReadShard ReadShardArgs
  | StreamCmdReadStream ReadStreamArgs
  | StreamCmdAppend AppendOpts
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
                               (O.progDesc "Delete a stream"))
 <> O.command "append" (O.info (StreamCmdAppend <$> appendOptsParser)
                               (O.progDesc "Append record into stream"))
 <> O.command "list-shard" (O.info (StreamCmdListShard <$> O.strArgument
                                                               ( O.metavar "STREAM_NAME"
                                                              <> O.help "The name of the stream to be queried"))
                                   (O.progDesc "List shards of specific stream"))
 <> O.command "read-shard" (O.info (StreamCmdReadShard <$> readShardRequestParser)
                                   (O.progDesc "Read records from specific shard"))
 <> O.command "read-stream" (O.info (StreamCmdReadStream <$> readStreamRequestParser)
                                    (O.progDesc "Read records from specific stream"))
  )

data AppendOpts = AppendOpts
  { _appStream        :: T.Text
  , _appKeySeparator  :: BS.ByteString
  , _appRetryInterval :: Word32
  , _appRetryLimit    :: Word32
  } deriving (Show)

appendOptsParser :: O.Parser AppendOpts
appendOptsParser = AppendOpts
  <$> O.strArgument ( O.metavar "STREAM_NAME" <> O.help "The stream you want to write to")
  <*> O.option O.str (O.long "separator" <> O.metavar "String" <> O.showDefault <> O.value "@" <> O.help "Separator of key. e.g. key1@value")
  <*> O.option O.auto (O.long "retry-interval" <> O.metavar "INT" <> O.showDefault <> O.value 5 <> O.help "Interval to retry request to server")
  <*> O.option O.auto (O.long "retry-limit" <> O.metavar "INT" <> O.showDefault <> O.value 3 <> O.help "Maximum number of retries allowed")

type ShardMap = M.Map ShardKey S.C_LogID

mkShardMap :: Vector API.Shard -> ShardMap
mkShardMap =
  foldl'
    (
       \acc API.Shard{shardShardId=sId, shardStartHashRangeKey=startKey} ->
          M.insert (cBytesToKey . textToCBytes $ startKey) sId acc
    ) M.empty

getShardIdByKey :: ShardKey -> ShardMap ->Maybe S.C_LogID
getShardIdByKey key mp = snd <$> M.lookupLE key mp

data AppendContext = AppendContext
  { cliCtx           :: HStreamCliContext
  , appStream        :: T.Text
  , appKeySeparator  :: BS.ByteString
  , appRetryInterval :: Word32
  , appRetryLimit    :: Word32
  , appShardMap      :: M.Map ShardKey S.C_LogID
  }

instance Read API.CompressionType where
  readPrec = do
    l <- Read.lexP
    case l of
      Read.Ident "none" -> return API.CompressionTypeNone
      Read.Ident "gzip" -> return API.CompressionTypeGzip
      Read.Ident "zstd" -> return API.CompressionTypeZstd
      x -> errorWithoutStackTrace $ "cannot parse compression type: " <> show x

data ReadStreamArgs = ReadStreamArgs
  { readStreamStreamNameArgs :: T.Text
  , readStreamStartOffset    :: Maybe API.StreamOffset
  , readStreamEndOffset      :: Maybe API.StreamOffset
  , readStreamMaxReadBatches :: Word64
  } deriving (Show)

readStreamRequestParser :: O.Parser ReadStreamArgs
readStreamRequestParser = ReadStreamArgs
  <$> O.strArgument ( O.metavar "STREAM_NAME" <> O.help "The stream you want to read" )
  <*> O.optional (shardOffsetToPbStreamOffset <$> O.option offsetReader ( O.metavar "[EARLIEST|LATEST|TIMESTAMP]"
                                                                       <> O.long "from"
                                                                       <> O.help ( "Read from offset, e.g. earliest, latest, "
                                                                                <> "1684486287810")
                                                                        ))
  <*> O.optional (shardOffsetToPbStreamOffset <$> O.option offsetReader ( O.metavar "[EARLIEST|LATEST|TIMESTAMP]"
                                                                       <> O.long "until"
                                                                       <> O.help ( "Read until offset, e.g. earliest, latest, "
                                                                                <> "1684486287810")
                                                                        ))
  <*> (fromIntegral <$> O.option positiveNumParser ( O.long "total"
                                                  <> O.metavar "INT"
                                                  <> O.value 0
                                                  <> O.help "Max total number of batches read by reader"
                                                   ))

data ReadShardArgs = ReadShardArgs
  { shardIdArgs    :: Word64
  , startOffset    :: Maybe API.ShardOffset
  , endOffset      :: Maybe API.ShardOffset
  , maxReadBatches :: Word64
  } deriving (Show)

readShardRequestParser :: O.Parser ReadShardArgs
readShardRequestParser = ReadShardArgs
  <$> O.argument O.auto ( O.metavar "SHARD_ID" <> O.help "The shard you want to read" )
  <*> O.optional (shardOffsetToPb <$> O.option offsetReader ( O.metavar "[EARLIEST|LATEST|RECORDID|TIMESTAMP]"
                                                           <> O.long "from"
                                                           <> O.help ( "Read from offset, e.g. earliest, latest, "
                                                                    <> "1789764666323849-4294967385-0, "
                                                                    <> "1684486287810")
                                                            ))
  <*> O.optional (shardOffsetToPb <$> O.option offsetReader ( O.metavar "[EARLIEST|LATEST|RECORDID|TIMESTAMP]"
                                                           <> O.long "until"
                                                           <> O.help ( "Read until offset, e.g. earliest, latest, "
                                                                    <> "1789764666323849-4294967385-0, "
                                                                    <> "1684486287810")
                                                            ))
  <*> (fromIntegral <$> O.option positiveNumParser ( O.long "total"
                                                  <> O.metavar "INT"
                                                  <> O.value 0
                                                  <> O.help "Max total number of batches read by reader"
                                                   ))

data ShardOffset = EARLIEST
                 | LATEST
                 | RecordId Word64 Word64 Word32
                 | Timestamp Int64
  deriving (Show)

offsetReader :: ReadM ShardOffset
offsetReader = eitherReader (parseShardOffset . T.pack)

parseShardOffset :: Text -> Either String ShardOffset
parseShardOffset t = case AP.parseOnly parseOffset t of
  Right res -> Right res
  Left e    -> Left $ "invalid offset, error: " <> show e
  where
   parseEarliest = AP.string "earliest" $> EARLIEST
   parseLatest = AP.string "latest" $> LATEST
   parseTimestamp = Timestamp <$> AP.decimal <* AP.endOfInput
   parseRecordId = do shardId <- AP.decimal
                      _ <- AP.char '-'
                      batchId <- AP.decimal
                      _ <- AP.char '-'
                      batchIndex <- AP.decimal
                      AP.endOfInput
                      return $ RecordId shardId batchId batchIndex
   parseOffset = parseEarliest <|> parseLatest <|> parseTimestamp <|> parseRecordId

shardOffsetToPb :: ShardOffset -> API.ShardOffset
shardOffsetToPb EARLIEST = API.ShardOffset . Just . API.ShardOffsetOffsetSpecialOffset . Enumerated . Right $ API.SpecialOffsetEARLIEST
shardOffsetToPb LATEST   = API.ShardOffset . Just . API.ShardOffsetOffsetSpecialOffset . Enumerated . Right $ API.SpecialOffsetLATEST
shardOffsetToPb (RecordId recordIdShardId recordIdBatchId recordIdBatchIndex) =
  API.ShardOffset . Just . API.ShardOffsetOffsetRecordOffset $ API.RecordId {..}
shardOffsetToPb (Timestamp t) = API.ShardOffset . Just . API.ShardOffsetOffsetTimestampOffset $
  API.TimestampOffset {timestampOffsetTimestampInMs = t, timestampOffsetStrictAccuracy = True}

shardOffsetToPbStreamOffset :: ShardOffset -> API.StreamOffset
shardOffsetToPbStreamOffset EARLIEST = API.StreamOffset . Just . API.StreamOffsetOffsetSpecialOffset . Enumerated . Right $ API.SpecialOffsetEARLIEST
shardOffsetToPbStreamOffset LATEST   = API.StreamOffset . Just . API.StreamOffsetOffsetSpecialOffset . Enumerated . Right $ API.SpecialOffsetLATEST
shardOffsetToPbStreamOffset RecordId {} = errorWithoutStackTrace "invalid offset"
shardOffsetToPbStreamOffset (Timestamp t) = API.StreamOffset . Just . API.StreamOffsetOffsetTimestampOffset $
  API.TimestampOffset {timestampOffsetTimestampInMs = t, timestampOffsetStrictAccuracy = True}

positiveNumParser :: ReadM Int
positiveNumParser = do
  s <- readerAsk
  case Read.readMaybe s of
    Just n | n >= 0 -> return n
    _               -> readerError $ "Expected positive integer but get: " ++ s


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
                                                                  <> O.short 'f'
                                                                  <> O.help "Whether to enable force deletion"))
                               (O.progDesc "Delete a subscription"))
  )

data ConnectorCommand
  = ConnectorCmdAlterConfig Text (Maybe Text) (Maybe Text)
  deriving (Show)

connectorCmdParser :: O.Parser ConnectorCommand
connectorCmdParser = O.hsubparser
  ( O.command "alter-config" (O.info (ConnectorCmdAlterConfig <$> O.strArgument ( O.metavar "CONNECTOR_NAME"
                                                                       <> O.help "The Name of the connector")
                                                      <*> (O.optional . O.option O.str) (O.long "config-json"
                                                                  <> O.metavar "STRING" <> O.help "connector config json string")
                                                      <*> (O.optional . O.option O.str) (O.long "config-path"
                                                                  <> O.metavar "STRING" <> O.help "connector config file path"))
                               (O.progDesc "alter connector config"))
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
  , _rpcAuthToken :: Maybe ByteString
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
  <*> (O.optional . O.option O.str) (O.long "token" <> O.metavar "STRING" <> O.help "token send to grpc")
  <*> O.option O.auto (O.long "retry-timeout" <> O.metavar "INT" <> O.showDefault <> O.value 60 <> O.help "timeout to retry connecting to a server in seconds")
  <*> (O.optional . O.option (O.maybeReader parseURI)) (O.long "service-url" <> O.help "The endpoint to connect to")

data RefinedCliConnOpts = RefinedCliConnOpts
  { addr         :: SocketAddr
  , clientConfig :: ClientConfig
  , retryTimeout :: Int
  , rpcMetadata  :: M.Map ByteString [ByteString]
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
      rpcMetadata = case _rpcAuthToken of
                      Nothing -> M.empty
                      Just tk -> M.singleton "authorization" [("Basic " <> tk)]
  pure $ RefinedCliConnOpts addr clientConfig _retryTimeout rpcMetadata
  where
    uriAuthToSocketAddress (Just URIAuth{..}) =
      let host = T.encodeUtf8 . T.pack $ uriUserInfo <> uriRegName in
      if BS.null host then errorWithoutStackTrace "Incomplete URI"
      else case uriPort of
        []     -> SocketAddr host _serverPort
        (_:xs) -> case Read.readMaybe xs of
          Nothing   -> SocketAddr host _serverPort
          Just port -> SocketAddr host port
    uriAuthToSocketAddress Nothing = errorWithoutStackTrace "Incomplete URI"
