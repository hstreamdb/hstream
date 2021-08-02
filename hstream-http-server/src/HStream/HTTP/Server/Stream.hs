{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp  #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Stream
  ( StreamsAPI
  , streamServer
  , listStreamsHandler
  , StreamBO(..)
  ) where

import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import           Data.Swagger                     (ToSchema)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Data.Word                        (Word32)
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Client     (Client)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   Post, ReqBody, type (:>),
                                                   (:<|>) (..))
import           Servant.Server                   (Handler, Server)

import           HStream.Server.HStreamApi

-- BO is short for Business Object
data StreamBO = StreamBO
  { name              :: T.Text
  , replicationFactor :: Word32
  } deriving (Eq, Show, Generic)

instance ToJSON StreamBO
instance FromJSON StreamBO
instance ToSchema StreamBO

type StreamsAPI =
  "streams" :> Get '[JSON] [StreamBO]
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  :<|> "streams" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "streams" :> Capture "name" T.Text :> Get '[JSON] (Maybe StreamBO)

streamToStreamBO :: Stream -> StreamBO
streamToStreamBO (Stream name rep) = StreamBO (TL.toStrict name) rep

createStreamHandler :: Client -> StreamBO -> Handler StreamBO
createStreamHandler hClient (StreamBO sName replicationFactor) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createStreamRequest = Stream { streamStreamName = TL.pack $ T.unpack sName
                                   , streamReplicationFactor = replicationFactor
                                   }
  resp <- hstreamApiCreateStream (ClientNormalRequest createStreamRequest 100 (MetadataMap $ Map.empty))
  case resp of
    -- TODO: should return streambo; but we need to update hstream api first
    ClientNormalResponse _ _meta1 _meta2 _status _details -> return $ StreamBO sName replicationFactor
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ StreamBO sName replicationFactor

listStreamsHandler :: Client -> Handler [StreamBO]
listStreamsHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiListStreams $ ClientNormalRequest ListStreamsRequest 100 (MetadataMap Map.empty)
  case resp of
    ClientNormalResponse x@ListStreamsResponse{} _meta1 _meta2 _status _details -> do
      case x of
        ListStreamsResponse {listStreamsResponseStreams = streams} -> do
          return $ V.toList $ V.map streamToStreamBO streams
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

deleteStreamHandler :: Client -> String -> Handler Bool
deleteStreamHandler hClient sName = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let deleteStreamRequest = DeleteStreamRequest { deleteStreamRequestStreamName = TL.pack sName, deleteStreamRequestIgnoreNonExist = True }
  resp <- hstreamApiDeleteStream (ClientNormalRequest deleteStreamRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False
    _ -> return False

getStreamHandler :: Client -> T.Text -> Handler (Maybe StreamBO)
getStreamHandler hClient sName = do
  streams <- listStreamsHandler hClient
  return $ find (\StreamBO{..} -> sName == name) streams

streamServer :: Client -> Server StreamsAPI
streamServer hClient = listStreamsHandler hClient
                   :<|> createStreamHandler hClient
                   :<|> deleteStreamHandler hClient
                   :<|> getStreamHandler hClient
