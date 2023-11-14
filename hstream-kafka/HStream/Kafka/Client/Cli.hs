{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE InterruptibleFFI      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE UnliftedFFITypes      #-}

module HStream.Kafka.Client.Cli
  ( Command (..)
  , runCliParser
  , handleTopicCommand
  , handleGroupCommand
  , handleNodeCommand
  , handleProduceCommand
  , handleConsumeCommand
  ) where

import           Colourista               (formatWith, yellow)
import           Control.Exception        (finally)
import           Control.Monad
import           Control.Monad.IO.Class   (liftIO)
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import           Data.Char                (toUpper)
import           Data.Int
import           Data.IORef
import           Data.Maybe
import           Data.Text                (Text)
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8, encodeUtf8)
import qualified Data.Vector              as V
import           Data.Word
import           Foreign.C.Types
import           Foreign.Ptr
import qualified HsForeign
import           Options.Applicative
import qualified Options.Applicative      as O
import qualified System.Console.Haskeline as HL
import           System.IO                (hFlush, stdout)
import           System.IO.Unsafe         (unsafePerformIO)

import           HStream.Base.Table       as Table
import qualified HStream.Kafka.Client.Api as KA
import           HStream.Utils            (newRandomText, splitOn)
import qualified Kafka.Protocol.Encoding  as K
import qualified Kafka.Protocol.Error     as K
import qualified Kafka.Protocol.Message   as K

-------------------------------------------------------------------------------

-- TODO: move to HStream.Kafka.Client.Api
--
-- For cli usage, there should not be only multi thread running at the same time.
gloCorrelationId :: IORef Int32
gloCorrelationId = unsafePerformIO $ newIORef 1
{-# NOINLINE gloCorrelationId #-}

getCorrelationId :: IO Int32
getCorrelationId = do
  r <- readIORef gloCorrelationId
  modifyIORef' gloCorrelationId (+ 1)
  pure r

data Options = Options
  { host :: !String
  , port :: !Int
  } deriving (Show)

data Command
  = TopicCommand TopicCommand
  | GroupCommand GroupCommand
  | NodeCommand NodeCommand
  | ProduceCommand ProduceCommandOpts
  | ConsumeCommand ConsumeCommandOpts
  deriving (Show)

optionsParser :: Parser Options
optionsParser =
  Options
    <$> strOption (long "host" <> metavar "HOST" <> value "127.0.0.1" <> help "Server host")
    <*> option auto (long "port" <> metavar "PORT" <> value 9092 <> help "Server port")

commandParser :: Parser Command
commandParser = hsubparser
    ( command "topic"
        (info (TopicCommand <$> topicCommandParser) (progDesc "topic command"))
   <> command "group"
        (info (GroupCommand <$> groupCommandParser) (progDesc "group command"))
   <> command "node"
        (info (NodeCommand <$> nodeCommandParser) (progDesc "node command"))
   <> command "produce"
        (info (ProduceCommand <$> produceCommandParser)
              (progDesc "Produce messages to topics"))
   <> command "consume"
        (info (ConsumeCommand <$> consumeCommandParser)
              (progDesc "Consume messages from topics"))
    )
  <|> hsubparser
    ( command "p"
        (info (ProduceCommand <$> produceCommandParser)
              (progDesc "Alias for the command 'produce'"))
   <> command "c"
        (info (ConsumeCommand <$> consumeCommandParser)
              (progDesc "Alias for the command 'consume'"))
   <> commandGroup "Alias:"
   <> hidden
    )

parseCommandWithOptions :: Parser (Options, Command)
parseCommandWithOptions = (,) <$> optionsParser <*> commandParser

runCliParser :: IO (Options, Command)
runCliParser = execParser $ info (parseCommandWithOptions <**> helper) mempty

-------------------------------------------------------------------------------

data TopicCommand
  = TopicCommandList
  | TopicCommandInfo Text
  | TopicCommandCreate K.CreateTopicsRequestV0
  | TopicCommandDelete TopicCommandDeleteOpts
  deriving (Show)

data TopicCommandDeleteOpts = TopicCommandDeleteOpts
  { name :: Either () Text
  , yes  :: Bool
  } deriving (Show, Eq)

topicCommandParser :: Parser TopicCommand
topicCommandParser = hsubparser
  ( O.command "list" (O.info (pure TopicCommandList) (O.progDesc "Get all topics"))
 <> O.command "describe" (O.info (TopicCommandInfo <$> topicNameParser) (O.progDesc "List details of given topic"))
 <> O.command "create" (O.info (TopicCommandCreate <$> createTopicsRequestParserV0) (O.progDesc "Create a topic"))
 <> O.command "delete" (O.info (TopicCommandDelete <$> topicDeleteOptsParser) (O.progDesc "Delete a topic"))
  )

topicNameParser :: Parser Text
topicNameParser =
  O.strArgument (O.metavar "TopicName" <> O.help "Topic name")

topicNameParser' :: Parser (Either () Text)
topicNameParser' =
      Right <$> O.strArgument (O.metavar "TopicName" <> O.help "Topic name")
  <|> flag' (Left ()) (O.long "all" <> O.help "All topics")

topicDeleteOptsParser :: Parser TopicCommandDeleteOpts
topicDeleteOptsParser = TopicCommandDeleteOpts
  <$> topicNameParser'
  <*> O.switch (O.long "yes" <> O.short 'y' <> O.help "Delete without prompt")

handleTopicCommand :: Options -> TopicCommand -> IO ()
handleTopicCommand opts TopicCommandList       = handleTopicList opts
handleTopicCommand opts (TopicCommandInfo n)   = handleTopicDescribe opts n
handleTopicCommand opts (TopicCommandCreate n) = handleTopicCreate opts n
handleTopicCommand opts (TopicCommandDelete o) = handleTopicDelete opts o

handleTopicList :: Options -> IO ()
handleTopicList Options{..} = do
  let req = K.MetadataRequestV0 (K.KaArray Nothing)
  correlationId <- getCorrelationId
  resp <- KA.withSendAndRecv host port (KA.metadata correlationId req)
  let K.NonNullKaArray topics = resp.topics
      failed = V.filter (\t -> t.errorCode /= K.NONE) topics
  if V.null failed
    then do
      let titles = ["Name", "IsInternal"]
          lenses = [ Text.unpack . (.name)
                   , show . (.isInternal)
                   ]
          stats = (\s -> ($ s) <$> lenses) <$> (V.toList topics)
      putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats
    else do
      putStrLn $ "List topic error: " <> show ((.errorCode) . V.head $ failed)

handleTopicDescribe :: Options -> Text -> IO ()
handleTopicDescribe Options{..} name = do
  tp <- describeTopic host port name
  let titles = ["Name", "IsInternal", "Partition", "LeaderId"]
      lenses = [ const (Text.unpack tp.name)
               , const (show tp.isInternal)
               , show . (.partitionIndex)
               , show . (.leaderId)
               ]
      stats = (\s -> ($ s) <$> lenses) <$> (V.toList $ K.unNonNullKaArray tp.partitions)
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats

handleTopicCreate :: Options -> K.CreateTopicsRequestV0 -> IO ()
handleTopicCreate Options{..} req@K.CreateTopicsRequestV0{..} = do
  correlationId <- getCorrelationId
  K.CreateTopicsResponseV0 (K.KaArray (Just rets)) <-
    KA.withSendAndRecv host port (KA.createTopics correlationId req)
  case V.toList rets of
    [K.CreatableTopicResultV0{..}]
      | errorCode /= K.NONE -> putStrLn $ "Create topic " <> show name <> " failed: " <> show errorCode
      | otherwise -> showTopic topics
    _ -> putStrLn $ "UnexpectedError: create topic " <> " receive " <> show rets
 where
   showTopic topic = do
     let titles = ["Name", "Partitions", "Replication-factor"]
         lenses = [ Text.unpack . (.name)
                  , show . (.numPartitions)
                  , show . (.replicationFactor)
                  ]
         stats = (\s -> ($ s) <$> lenses) <$> V.toList (K.unNonNullKaArray topic)
     putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats

handleTopicDelete :: Options -> TopicCommandDeleteOpts -> IO ()
handleTopicDelete Options{..} cmdopts = do
  correlationId <- getCorrelationId
  topics <- case cmdopts.name of
              Left _ -> do
                let req = K.MetadataRequestV0 (K.KaArray Nothing)
                resp <- KA.withSendAndRecv host port (KA.metadata correlationId req)
                let topic_names = V.map (.name)
                                $ V.filter (not . (.isInternal))
                                $ K.unNonNullKaArray resp.topics
                pure $ K.NonNullKaArray $ topic_names
              Right n -> pure $ K.NonNullKaArray $ V.singleton n
  if cmdopts.yes
     then doDelete topics
     else do putStr $ "Are you sure you want to delete topics "
                   <> show (K.unNonNullKaArray topics)
                   <> " [y/N]? "
             hFlush stdout
             c <- getChar
             if toUpper c == 'Y' then doDelete topics else putStrLn "Abort"
  where
    doDelete topics = do
      let req = K.DeleteTopicsRequestV0 topics 5000{- timeoutMs -}
      correlationId <- getCorrelationId
      K.DeleteTopicsResponseV0 (K.KaArray (Just rets)) <-
        KA.withSendAndRecv host port (KA.deleteTopics correlationId req)
      V.forM_ rets $ \ret -> do
        when (ret.errorCode /= K.NONE) $
          putStrLn $ "Delete topic " <> show ret.name <> " failed: " <> show ret.errorCode
      putStrLn "DONE"

-------------------------------------------------------------------------------

data GroupCommand
  = GroupCommandList
  | GroupCommandShow Text
  deriving (Show)

groupIdParser :: Parser Text
groupIdParser =
  O.strArgument (O.metavar "Text" <> O.help "Group id")

groupCommandParser :: Parser GroupCommand
groupCommandParser = hsubparser
  ( O.command "list" (O.info (pure GroupCommandList) (O.progDesc "Get all consumer groups"))
 <> O.command "show" (O.info (GroupCommandShow <$> groupIdParser) (O.progDesc "Show topic info"))
  )

handleGroupCommand :: Options -> GroupCommand -> IO ()
handleGroupCommand opts GroupCommandList     = handleGroupList opts
handleGroupCommand opts (GroupCommandShow n) = handleGroupShow opts n

handleGroupList :: Options -> IO ()
handleGroupList Options{..} = do
  let req = K.ListGroupsRequestV0
  correlationId <- getCorrelationId
  resp <- KA.withSendAndRecv host port (KA.listGroups correlationId req)
  when (resp.errorCode /= K.NONE) $
    errorWithoutStackTrace $ "List groups failed: " <> show resp.errorCode
  let titles = ["ID", "ProtocolType"]
      K.NonNullKaArray groups = resp.groups
      lenses = [ Text.unpack . (.groupId)
               , Text.unpack . (.protocolType)
               ]
      stats = (\s -> ($ s) <$> lenses) <$> (V.toList groups)
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats

handleGroupShow :: Options -> Text -> IO ()
handleGroupShow Options{..} name = do
  let req = K.DescribeGroupsRequestV0 (K.KaArray $ Just $ V.singleton name)
  correlationId <- getCorrelationId
  resp <- KA.withSendAndRecv host port (KA.describeGroups correlationId req)
  let K.NonNullKaArray groups = resp.groups
      group = V.head groups  -- We only send one group id in the request, so
                             -- there should be only one group in the response
  when (group.errorCode /= K.NONE) $
    errorWithoutStackTrace $ "Describe group failed: " <> show group.errorCode
  let emit idt s = putStrLn $ replicate idt ' ' <> s
      members = K.unNonNullKaArray $ group.members
  emit 0 . formatWith [yellow] $ "\9678 " <> Text.unpack group.groupId
  emit 2 $ "GroupState: " <> Text.unpack group.groupState
  emit 2 $ "ProtocolType: " <> Text.unpack group.protocolType
  emit 2 $ "ProtocolData: " <> Text.unpack group.protocolData
  if V.null members
     then emit 2 $ "Members: None"
     else do
       emit 2 $ "Members:"
       V.forM_ members $ \m -> do
         emit 4 . formatWith [yellow] $ "\9678 " <> Text.unpack m.memberId
         emit 6 $ "ClientId: " <> Text.unpack m.clientId
         emit 6 $ "ClientHost: " <> Text.unpack m.clientHost

-------------------------------------------------------------------------------

data NodeCommand
  = NodeCommandList
  deriving Show

nodeCommandParser :: Parser NodeCommand
nodeCommandParser = hsubparser
  ( O.command "list"
              (O.info (pure NodeCommandList)
                      (O.progDesc "List all alive brokers."))
  )

handleNodeCommand :: Options -> NodeCommand -> IO ()
handleNodeCommand opts NodeCommandList = handleNodeList opts

handleNodeList :: Options -> IO ()
handleNodeList Options{..} = do
  let req = K.MetadataRequestV0 (K.KaArray Nothing)
  correlationId <- getCorrelationId
  resp <- KA.withSendAndRecv host port (KA.metadata correlationId req)
  let titles = ["ID", "Address", "Controller"]
      K.NonNullKaArray brokers = resp.brokers
      lenses = [ show . (.nodeId)
               , \b -> Text.unpack b.host <> ":" <> show b.port
               , show . (== resp.controllerId) . (.nodeId)
               ]
      stats = (\s -> ($ s) <$> lenses) <$> (V.toList brokers)
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats

-------------------------------------------------------------------------------

data ProduceData = ProduceInteractive | ProduceData Text
  deriving (Show, Eq)

data ProduceCommandOpts = ProduceCommandOpts
  { topic        :: Text
  , partition    :: Maybe Int32
  , timeoutMs    :: Int32
  , keySeparator :: BS.ByteString
  , produceData  :: ProduceData
  } deriving (Show, Eq)

produceCommandParser :: Parser ProduceCommandOpts
produceCommandParser = ProduceCommandOpts
  <$> strArgument (metavar "TopicName" <> help "Topic name")
  <*> O.optional (option auto (long "partition" <> short 'p' <> metavar "Int32" <> help "Partition index"))
  <*> option auto (long "timeout" <> metavar "Int32" <> value 5000 <> help "Timeout in milliseconds")
  <*> O.option O.str (O.long "separator" <> short 's' <> O.metavar "String" <> O.showDefault <> O.value "@" <> O.help "Separator of key. e.g. key1@value")
  <*> ( ProduceData <$> strOption (long "data" <> short 'd' <> metavar "Text" <> help "Data")
    <|> flag' ProduceInteractive (long "interactive" <> short 'i' <> help "Interactive mode")
      )

handleProduceCommand :: Options -> ProduceCommandOpts -> IO ()
handleProduceCommand Options{..} cmdopts = do
  let brokers = encodeUtf8 $ Text.pack (host <> ":" <> show port)
  producer <- HsForeign.withByteString brokers $ \brokers' size -> do
    (errmsg, p) <- unsafeWithStdString $ hs_new_producer brokers' size
    when (p == nullPtr) $ errorWithoutStackTrace $
      "Create producer failed: " <> (Text.unpack $ decodeUtf8 errmsg)
    pure p
  case cmdopts.produceData of
    ProduceData d -> flip finally (hs_delete_producer producer) $ do
      let (k, v) = splitKeyValue cmdopts.keySeparator (encodeUtf8 d)
      doProduce producer cmdopts.topic (fromMaybe (-1) cmdopts.partition) k v
    ProduceInteractive -> flip finally (hs_delete_producer producer) $ do
      putStrLn $ "Type message value and hit enter "
              <> "to produce message. Use Ctrl-c to exit."
      HL.runInputT HL.defaultSettings
        (loopReadLine producer cmdopts.topic (fromMaybe (-1) cmdopts.partition))
  where
    splitKeyValue sep input =
      let items = splitOn sep input
       in case items of
            [payload] -> (BS.empty, payload)
            [key,payload] -> (key, payload)
            _ -> errorWithoutStackTrace $ "invalid input: "  <> show input <> " with separator " <> show sep

    doProduce p topic partition key payload = do
      (errmsg, ret) <-
        HsForeign.withByteString (encodeUtf8 topic) $ \topic' topic_size ->
          HsForeign.withByteString payload $ \payload'' payload_size ->
            HsForeign.withByteString key $ \key' key_size ->
              unsafeWithStdString $
                hs_producer_produce p topic' topic_size partition
                                    payload'' payload_size key' key_size
      when (ret /= 0) $ errorWithoutStackTrace $
        "Produce failed: " <> (Text.unpack $ decodeUtf8 errmsg)
      hs_producer_flush p

    loopReadLine p topic partition = do
      minput <- HL.getInputLine "> "
      case minput of
        Nothing -> return ()
        Just payload -> do
          let (k, v) = splitKeyValue cmdopts.keySeparator (encodeUtf8 . Text.pack $ payload)
          liftIO $ doProduce p topic partition k v
          loopReadLine p topic partition

data HsProducer

foreign import ccall unsafe "hs_new_producer"
  hs_new_producer
    :: Ptr Word8 -> Int -> (Ptr HsForeign.StdString) -> IO (Ptr HsProducer)

foreign import ccall interruptible "hs_producer_produce"
  hs_producer_produce
    :: Ptr HsProducer
    -> Ptr Word8 -> Int             -- Topic
    -> Int32                        -- Partition
    -> Ptr Word8 -> Int             -- Payload
    -> Ptr Word8 -> Int             -- Key
    -> (Ptr HsForeign.StdString)    -- errmsg
    -> IO Int

foreign import ccall safe "hs_producer_flush"
  hs_producer_flush :: Ptr HsProducer -> IO ()

foreign import ccall unsafe "hs_delete_producer"
  hs_delete_producer :: Ptr HsProducer -> IO ()

-------------------------------------------------------------------------------
-- TODO

data OffsetReset
  = OffsetResetEarliest
  | OffsetResetLatest
  deriving (Show, Eq)

data ConsumeCommandOpts = ConsumeCommandOpts
  { groupId     :: Text
  , topics      :: [Text]
  , offsetReset :: Maybe OffsetReset
  , eof         :: Bool
  , autoCommit  :: Bool
  } deriving (Show, Eq)

-- librdkafka doesn't support set `allow.auto.create.topics` any more for consumer since v1.6
-- see: https://github.com/confluentinc/confluent-kafka-go/issues/615
consumeCommandParser :: Parser ConsumeCommandOpts
consumeCommandParser = ConsumeCommandOpts
  <$> strOption (long "group-id" <> short 'g' <> metavar "Text" <> value Text.empty <> help "Group id")
  <*> some (strOption (long "topic" <> short 't' <> metavar "Text" <> help "Topic name"))
  <*> optional ( flag' OffsetResetEarliest (long "earliest" <> help "Reset offset to earliest")
             <|> flag' OffsetResetLatest (long "latest" <> help "Reset offset to latest, default")
               )
  <*> switch ( long "eof" <> short 'e'
            <> help "Exit consumer when last message in partition has been received."
             )
  <*> flag True False (long "no-auto-commit" <> help "disable auto commit")

handleConsumeCommand :: Options -> ConsumeCommandOpts -> IO ()
handleConsumeCommand Options{..} cmdopts = do
  let topics = filter (not . Text.null) cmdopts.topics
  when (null topics) $
    errorWithoutStackTrace "Topic name is required"
  groupId <- if Text.null cmdopts.groupId then newRandomText 10  else return cmdopts.groupId

  let brokers = encodeUtf8 $ Text.pack (host <> ":" <> show port)
      groupIdBs = encodeUtf8 groupId :: ByteString
      offsetResetBs = case cmdopts.offsetReset of
                        Nothing                  -> "latest"
                        Just OffsetResetEarliest -> "earliest"
                        Just OffsetResetLatest   -> "latest"
      cAutoCommit = if cmdopts.autoCommit then 1 else 0
      ceof = if cmdopts.eof then 1 else 0
  consumer <-
    HsForeign.withByteString brokers $ \brokers' brokers_size ->
    HsForeign.withByteString groupIdBs $ \groupid' groupid_size ->
    HsForeign.withByteString offsetResetBs $ \offset' offset_size -> do
      (errmsg, c) <- unsafeWithStdString $
        hs_new_consumer brokers' brokers_size
                        groupid' groupid_size
                        offset' offset_size
                        ceof
                        cAutoCommit
      when (c == nullPtr) $ errorWithoutStackTrace $
        "Create consumer failed: " <> (Text.unpack $ decodeUtf8 errmsg)
      pure c

  HsForeign.withByteStringList (map encodeUtf8 topics) $ \tds' tss' tl -> do
    (errmsg, ret) <- unsafeWithStdString $
      hs_consumer_consume consumer tds' tss' tl
    when (ret /= 0) $ errorWithoutStackTrace $
      "Consume failed: " <> (Text.unpack $ decodeUtf8 errmsg)

data HsConsumer

foreign import ccall unsafe "hs_new_consumer"
  hs_new_consumer
    :: Ptr Word8 -> Int   -- brokers
    -> Ptr Word8 -> Int   -- group id
    -> Ptr Word8 -> Int   -- offsetReset
    -> CBool              -- exit_eof
    -> CBool              -- auto_commit
    -> Ptr HsForeign.StdString
    -> IO (Ptr HsConsumer)

foreign import ccall interruptible "hs_consumer_consume"
  hs_consumer_consume
    :: Ptr HsConsumer
    -> Ptr (Ptr Word8) -> Ptr Int -> Int  -- topics
    -> Ptr HsForeign.StdString
    -> IO Int

-------------------------------------------------------------------------------
-- TODO: auto generate

creatableTopicParserV0 :: Parser K.CreatableTopicV0
creatableTopicParserV0 = K.CreatableTopicV0
  <$> strArgument (metavar "TopicName")
  <*> option auto (O.long "num-partitions" <> O.short 'p' <> O.value 1 <> O.showDefault <> metavar "Int32")
  <*> option auto (O.long "replication-factor" <> O.short 'r' <> O.value 1 <> O.showDefault <> metavar "Int16")
  <*> pure (K.KaArray $ Just V.empty)
  <*> pure (K.KaArray $ Just V.empty)

createTopicsRequestParserV0 :: Parser K.CreateTopicsRequestV0
createTopicsRequestParserV0 = K.CreateTopicsRequestV0
  <$> (K.KaArray . Just . V.singleton <$> creatableTopicParserV0)
  <*> option auto (O.long "timeout" <> O.short 't' <> O.value 5000 <> O.showDefault <> metavar "Int32")

-------------------------------------------------------------------------------

-- Helper to get one topic info, throw exception if error occurs
describeTopic :: String -> Int -> Text -> IO K.MetadataResponseTopicV1
describeTopic host port name = do
  let repTopics = V.singleton $ K.MetadataRequestTopicV0 name
      req = K.MetadataRequestV0 (K.NonNullKaArray repTopics)
  correlationId <- getCorrelationId
  resp <- KA.withSendAndRecv host port (KA.metadata correlationId req)
  let K.NonNullKaArray topics = resp.topics
      -- XXX: There should be only one topic in the response
      m_topic = V.find ((== name) . (.name)) topics
  case m_topic of
    Nothing -> errorWithoutStackTrace "Topic not found!"
    Just tp -> do
      when (tp.errorCode /= K.NONE) $
        errorWithoutStackTrace $ "Get topic info failed: " <> show tp.errorCode
      pure tp

-- TODO: importe from HsForeign if we have a upgraded version
unsafeWithStdString :: (Ptr HsForeign.StdString -> IO a) -> IO (ByteString, a)
unsafeWithStdString f = do
  !ptr <- HsForeign.hs_new_std_string_def
  !r <- f ptr
  bs <- HsForeign.unsafePeekStdString ptr
  pure (bs, r)
