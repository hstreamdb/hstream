{-# LANGUAGE DeriveAnyClass #-}

module HStream.Store.RPC.MessagePack
  ( serve
  , FileBasedCheckpointConfig (..)
  , CheckpointConfig (..)
  , ConsumerConfig (..)
  ) where

import           Control.Monad        (forM, forM_, void)
import           Data.IORef           (newIORef, readIORef, writeIORef)
import           Data.Int             (Int32, Int64)
import           Data.Map.Strict      (Map)
import           Data.Maybe           (fromMaybe)
import           Data.Word            (Word32)
import           GHC.Generics         (Generic)
import           Z.Data.CBytes        (CBytes)
import qualified Z.Data.JSON          as JSON
import qualified Z.Data.MessagePack   as MP
import qualified Z.IO.BIO             as Z
import qualified Z.IO.Network         as Z
import qualified Z.IO.RPC.MessagePack as MP

import qualified HStream.Store.Stream as S

-------------------------------------------------------------------------------

data ContextData = ContextData
  { ctxStreamClient :: S.StreamClient
  , ctxReaderClient :: Maybe S.StreamSyncCheckpointedReader
  , ctxReaderTopics :: Maybe [CBytes]
  }

serve :: Z.TCPServerConfig -> IO ()
serve tcpConf = MP.serveRPC (Z.startTCPServer tcpConf) $ MP.simpleRouter
  [ ("hi", MP.CallHandler prepare)
  , ("create-topic", MP.CallHandler createTopic)
  , ("pub", MP.CallHandler pub)
  , ("sub", MP.StreamHandler sub)
  , ("subc", MP.StreamHandler subFromCheckpoint)
  , ("commit-checkpoint", MP.CallHandler commitCheckpoint)
  , ("has-topic", MP.CallHandler hasTopic)
  ]

-------------------------------------------------------------------------------

prepare :: MP.SessionCtx ContextData -> CBytes -> IO ()
prepare ctx configUri = do
  client <- S.newStreamClient configUri
  MP.writeSessionCtx ctx $ ContextData client Nothing Nothing

createTopic :: MP.SessionCtx ContextData -> Map CBytes S.TopicAttrs -> IO ()
createTopic ctx ts = withStreamClient ctx $ \client -> do
  S.createTopicsSync client ts

pub :: MP.SessionCtx ContextData -> S.ProducerRecord -> IO ()
pub ctx record@S.ProducerRecord{..} = withStreamClient ctx $ \client -> do
  topicID <- S.getTopicIDByName client dataInTopic
  void $ S.append client topicID (S.encodeRecord record) Nothing

data FileBasedCheckpointConfig = FileBasedCheckpointConfig
  { fileCheckpointPath    :: CBytes
  , fileCheckpointRetries :: Word32
  } deriving (Show, Generic, JSON.JSON, MP.MessagePack)

data CheckpointConfig
  = FileCheckpoint FileBasedCheckpointConfig
  | ZookeeperCheckpoint Word32
  deriving (Show, Generic, JSON.JSON, MP.MessagePack)

data ConsumerConfig = ConsumerConfig
  { consumerName       :: CBytes
    -- ^ Unique identifier of one consumer
  , consumerBufferSize :: Maybe Int64
    -- ^ specify the read buffer size for this client, fallback
    -- to the value in settings if it is Nothing.
  , consumerTopics     :: [CBytes]
  , consumerMaxRecords :: Int
  , consumerTimeout    :: Int32
  , consumerCheckpoint :: CheckpointConfig
  } deriving (Show, Generic, JSON.JSON, MP.MessagePack)

sub :: MP.SessionCtx ContextData -> ConsumerConfig -> IO (Z.Source S.ConsumerRecord)
sub ctx ConsumerConfig{..} = withStreamClient ctx $ \client -> do
  topics <- forM consumerTopics $ \t -> do
    topicID <- S.getTopicIDByName client t
    lastSN <- S.getTailSequenceNum client topicID
    return (topicID, lastSN + 1, maxBound)
  (s, ckpReader) <- newSubscriber client consumerName (Left topics) consumerMaxRecords
                                  consumerBufferSize consumerTimeout consumerCheckpoint
  MP.modifySessionCtx ctx $ \x -> Just x{ ctxReaderClient=Just ckpReader
                                        , ctxReaderTopics=Just consumerTopics
                                        }
  return s

subFromCheckpoint :: MP.SessionCtx ContextData -> ConsumerConfig -> IO (Z.Source S.ConsumerRecord)
subFromCheckpoint ctx ConsumerConfig{..} = withStreamClient ctx $ \client -> do
  topics <- forM consumerTopics $ \t -> do
    topicID <- S.getTopicIDByName client t
    return (topicID, maxBound)
  (s, ckpReader) <- newSubscriber client consumerName (Right topics) consumerMaxRecords
                                  consumerBufferSize consumerTimeout consumerCheckpoint
  MP.modifySessionCtx ctx $ \x -> Just x{ ctxReaderClient=Just ckpReader
                                        , ctxReaderTopics=Just consumerTopics
                                        }
  return s

commitCheckpoint :: MP.SessionCtx ContextData -> [CBytes] -> IO ()
commitCheckpoint ctx ts =
  withStreamClient ctx $ \client ->
  withReaderClient ctx $ \reader -> do
    topicIDs <- forM ts (S.getTopicIDByName client)
    S.writeLastCheckpointsSync reader topicIDs

hasTopic :: MP.SessionCtx ContextData -> CBytes -> IO Bool
hasTopic ctx topic =
  withStreamClient ctx $ flip S.doesTopicExists topic

-------------------------------------------------------------------------------

newSubscriber
  :: S.StreamClient
  -> CBytes
  -> Either [(S.TopicID, S.SequenceNum, S.SequenceNum)] [(S.TopicID, S.SequenceNum)]
  -> Int
  -> Maybe Int64
  -> Int32
  -> CheckpointConfig
  -> IO (Z.Source S.ConsumerRecord, S.StreamSyncCheckpointedReader)
newSubscriber client name e_topics maxRecords m_buffSize timeout ckpConfig = do
  let bufSize = fromMaybe (-1) m_buffSize
      topics = case e_topics of
                 Left ts -> map (\(i, start, end) reader -> S.checkpointedReaderStartReading reader i start end) ts
                 Right ts -> map (\(i, end) reader -> S.startReadingFromCheckpoint reader i end) ts

  reader <- S.newStreamReader client (fromIntegral $ length topics) bufSize
  ckpReader <-
    case ckpConfig of
      FileCheckpoint FileBasedCheckpointConfig{..} -> do
        ckpStore <- S.newFileBasedCheckpointStore fileCheckpointPath
        S.newStreamSyncCheckpointedReader name reader ckpStore fileCheckpointRetries
      ZookeeperCheckpoint retries -> do
        ckpStore <- S.newZookeeperBasedCheckpointStore client
        S.newStreamSyncCheckpointedReader name reader ckpStore retries

  -- start reading
  forM_ topics $ \f -> f ckpReader
  void $ S.checkpointedReaderSetTimeout ckpReader timeout
  src <- flattenListSource $
    Z.sourceFromIO (Just . map S.decodeRecord <$> S.checkpointedReaderRead ckpReader maxRecords)
  return (src, ckpReader)

withStreamClient :: MP.SessionCtx ContextData -> (S.StreamClient -> IO a) -> IO a
withStreamClient ctx f = do
  m_client <- MP.readSessionCtx ctx
  f $ maybe (error "Empty client session context.") ctxStreamClient m_client

withReaderClient :: MP.SessionCtx ContextData -> (S.StreamSyncCheckpointedReader -> IO a) -> IO a
withReaderClient ctx f = do
  m_client <- MP.readSessionCtx ctx
  f $ fromMaybe (error "Empty reader session context.") (m_client >>= ctxReaderClient)

flattenListSource :: Z.Source [a] -> IO (Z.Source a)
flattenListSource sxs = newIORef [] >>= \ref -> return (Z.BIO{ pull = loop ref })
  where
    loop ref = do
        rs' <- readIORef ref
        case rs' of
          [] -> do m_xs <- Z.pull sxs
                   case m_xs of
                     Just xs -> writeIORef ref xs >> loop ref
                     Nothing -> return Nothing
          (a:rest) -> writeIORef ref rest >> return (Just a)
