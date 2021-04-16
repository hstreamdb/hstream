{-# LANGUAGE DeriveAnyClass #-}

module HStream.Store.RPC.MessagePack
  ( serve
  , FileBasedCheckpointConfig (..)
  , CheckpointConfig (..)
  , ConsumerConfig (..)
  ) where

import           Control.Monad           (forM, forM_, void)
import           Data.IORef              (newIORef, readIORef, writeIORef)
import           Data.Int                (Int32, Int64)
import           Data.Map.Strict         (Map)
import           Data.Maybe              (fromMaybe)
import           Data.Word               (Word32)
import           GHC.Generics            (Generic)
import           Z.Data.CBytes           (CBytes)
import qualified Z.Data.JSON             as JSON
import qualified Z.Data.MessagePack      as MP
import qualified Z.Data.Text             as T
import           Z.Data.Vector           (Bytes)
import qualified Z.IO.BIO                as Z
import qualified Z.IO.Network            as Z
import qualified Z.IO.RPC.MessagePack    as MP

import           HStream.Store.Exception
import           HStream.Store.Logger
import qualified HStream.Store.Stream    as S

-------------------------------------------------------------------------------

serve :: Z.TCPServerConfig -> IO ()
serve tcpConf = MP.serveRPC (Z.startTCPServer tcpConf) $ MP.simpleRouter
  [ ("hi", MP.CallHandler prepare)
  , ("create-topic", MP.CallHandler createTopic)
  , ("pub", MP.CallHandler pub)
  , ("sub", MP.StreamHandler sub)
  ]

data ContextData = ContextData
  { ctxStreamClient :: S.StreamClient
  , ctxReaderClient :: Maybe S.StreamSyncCheckpointedReader
  }

-------------------------------------------------------------------------------

newtype PrepareRequest = PrepareRequest
  { initReqConfigUri :: CBytes
  } deriving (Show, Generic)
    deriving newtype (JSON.JSON, MP.MessagePack)

prepare :: MP.SessionCtx ContextData -> PrepareRequest -> IO ()
prepare ctx PrepareRequest{..} = do
  client <- S.newStreamClient initReqConfigUri
  MP.writeSessionCtx ctx $ ContextData client Nothing

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

-- TODO
data LogBasedCheckpointConfig

data CheckpointConfig
  = FileCheckpoint FileBasedCheckpointConfig
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
    return (topicID, lastSN)
  let bufSize = fromMaybe (-1) consumerBufferSize
  reader <- S.newStreamReader client (fromIntegral $ length consumerTopics) bufSize
  ckpReader <-
    case consumerCheckpoint of
      FileCheckpoint FileBasedCheckpointConfig{..} -> do
        ckpStore <- S.newFileBasedCheckpointStore fileCheckpointPath
        S.newStreamSyncCheckpointedReader consumerName reader ckpStore fileCheckpointRetries
  MP.modifySessionCtx ctx $ \x -> Just x{ctxReaderClient=Just ckpReader}

  forM_ topics $ \(topicID, lastSN) ->
    S.checkpointedReaderStartReading ckpReader topicID (lastSN + 1) maxBound
  void $ S.checkpointedReaderSetTimeout ckpReader consumerTimeout
  flattenListSource $ Z.sourceFromIO (Just . map S.decodeRecord <$> S.checkpointedReaderRead ckpReader consumerMaxRecords)

-------------------------------------------------------------------------------

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
