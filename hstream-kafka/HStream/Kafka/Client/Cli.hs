{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE InterruptibleFFI      #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MagicHash             #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE UnliftedFFITypes      #-}

module HStream.Kafka.Client.Cli
  ( Command (..)
  , runCliParser
  , handleTopicCommand
  , handleNodeCommand
  , handleProduceCommand
  , handleConsumeCommand
  ) where

import           Control.Exception        (finally)
import           Control.Monad
import           Control.Monad.IO.Class   (liftIO)
import           Data.ByteString          (ByteString)
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
import qualified Kafka.Protocol.Encoding  as K
import qualified Kafka.Protocol.Error     as K
import qualified Kafka.Protocol.Message   as K

-------------------------------------------------------------------------------

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
 <> O.command "info" (O.info (TopicCommandInfo <$> topicNameParser) (O.progDesc "topic info"))
 <> O.command "create" (O.info (TopicCommandCreate <$> createTopicsRequestParserV0) (O.progDesc "Create a topic"))
 <> O.command "delete" (O.info (TopicCommandDelete <$> topicDeleteOptsParser) (O.progDesc "delete a topic"))
  )

topicNameParser :: Parser Text
topicNameParser =
  O.strOption (O.long "name" <> O.metavar "Text" <> O.help "Topic name")

topicNameParser' :: Parser (Either () Text)
topicNameParser' =
      Right <$> O.strOption (O.long "name" <> O.metavar "Text" <> O.help "Topic name")
  <|> flag' (Left ()) (O.long "all" <> O.help "All topics")

topicDeleteOptsParser :: Parser TopicCommandDeleteOpts
topicDeleteOptsParser = TopicCommandDeleteOpts
  <$> topicNameParser'
  <*> O.switch (O.long "yes" <> O.short 'y' <> O.help "Delete without prompt")

handleTopicCommand :: Options -> TopicCommand -> IO ()
handleTopicCommand opts TopicCommandList       = handleTopicList opts
handleTopicCommand opts (TopicCommandInfo n)   = handleTopicInfo opts n
handleTopicCommand opts (TopicCommandCreate n) = handleTopicCreate opts n
handleTopicCommand opts (TopicCommandDelete o) = handleTopicDelete opts o

handleTopicList :: Options -> IO ()
handleTopicList Options{..} = do
  let req = K.MetadataRequestV0 (K.KaArray Nothing)
  correlationId <- getCorrelationId
  resp <- KA.withSendAndRecv host port (KA.metadata correlationId req)
  let titles = ["Name", "ErrorCode", "IsInternal"]
      K.NonNullKaArray topics = resp.topics
      lenses = [ show . (.name)
               , show . (.errorCode)
               , show . (.isInternal)
               ]
      stats = (\s -> ($ s) <$> lenses) <$> (V.toList topics)
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats

handleTopicInfo :: Options -> Text -> IO ()
handleTopicInfo Options{..} name = do
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
handleTopicCreate Options{..} req = do
  correlationId <- getCorrelationId
  K.CreateTopicsResponseV0 (K.KaArray (Just rets)) <-
    KA.withSendAndRecv host port (KA.createTopics correlationId req)
  V.forM_ rets $ \ret -> do
    when (ret.errorCode /= K.NONE) $
      putStrLn $ "Create topic " <> show ret.name <> " failed: " <> show ret.errorCode
  putStrLn "DONE"

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
  { topic       :: Text
  , partition   :: Maybe Int32
  , timeoutMs   :: Int32
  , produceData :: ProduceData
  } deriving (Show, Eq)

produceCommandParser :: Parser ProduceCommandOpts
produceCommandParser = ProduceCommandOpts
  <$> strOption (long "topic" <> metavar "Text" <> help "Topic name")
  <*> O.optional (option auto (long "partition" <> short 'p' <> metavar "Int32" <> help "Partition index"))
  <*> option auto (long "timeout" <> metavar "Int32" <> value 5000 <> help "Timeout in milliseconds")
  <*> ( ProduceData <$> strOption (long "data" <> short 'd' <> metavar "Text" <> help "Data")
    <|> flag' ProduceInteractive (long "interactive" <> short 'i' <> help "Interactive mode")
      )

handleProduceCommand :: Options -> ProduceCommandOpts -> IO ()
handleProduceCommand Options{..} cmdopts = do
  let brokers = encodeUtf8 $ Text.pack (host <> ":" <> show port)
  -- First check topic exists because we do not support auto topic creation yet.
  -- Or you will get a coredump.
  void $ describeTopic host port cmdopts.topic

  producer <- HsForeign.withByteString brokers $ \brokers' size -> do
    (errmsg, p) <- unsafeWithStdString $ hs_new_producer brokers' size
    when (p == nullPtr) $ errorWithoutStackTrace $
      "Create producer failed: " <> (Text.unpack $ decodeUtf8 errmsg)
    pure p
  case cmdopts.produceData of
    ProduceData d -> flip finally (hs_delete_producer producer) $ do
      doProduce producer cmdopts.topic (fromMaybe (-1) cmdopts.partition) d
      hs_producer_flush producer
    ProduceInteractive -> flip finally (hs_delete_producer producer) $ do
      putStrLn $ "Type message value and hit enter "
              <> "to produce message. Use Ctrl-c to exit."
      HL.runInputT HL.defaultSettings
        (loopReadLine producer cmdopts.topic (fromMaybe (-1) cmdopts.partition))
  where
    doProduce p topic partition payload = do
      (errmsg, ret) <-
        HsForeign.withByteString (encodeUtf8 topic) $ \topic' topic_size ->
          HsForeign.withByteString (encodeUtf8 payload) $ \payload'' payload_size ->
            unsafeWithStdString $
              hs_producer_produce p topic' topic_size partition
                                  payload'' payload_size
      when (ret /= 0) $ errorWithoutStackTrace $
        "Produce failed: " <> (Text.unpack $ decodeUtf8 errmsg)

    loopReadLine p topic partition = do
      minput <- HL.getInputLine "> "
      case minput of
        Nothing -> return ()
        Just payload -> do
          liftIO $ doProduce p topic partition (Text.pack payload)
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

consumeCommandParser :: Parser ConsumeCommandOpts
consumeCommandParser = ConsumeCommandOpts
  <$> strOption (long "group-id" <> metavar "Text" <> help "Group id")
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
  when (Text.null cmdopts.groupId) $
    errorWithoutStackTrace "Group id is required"
  -- First check topic exists because we do not support auto topic creation yet.
  -- Or you will get a coredump.
  forM_ topics $ describeTopic host port

  let brokers = encodeUtf8 $ Text.pack (host <> ":" <> show port)
      groupIdBs = encodeUtf8 cmdopts.groupId :: ByteString
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
  <$> option str (O.long "name" <> metavar "Text")
  <*> option auto (O.long "num-partitions" <> metavar "Int32")
  <*> option auto (O.long "replication-factor" <> metavar "Int16")
  <*> pure (K.KaArray $ Just V.empty)
  <*> pure (K.KaArray $ Just V.empty)

createTopicsRequestParserV0 :: Parser K.CreateTopicsRequestV0
createTopicsRequestParserV0 = K.CreateTopicsRequestV0
  <$> (K.KaArray . Just . V.fromList <$> some creatableTopicParserV0)
  <*> pure 5000

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
