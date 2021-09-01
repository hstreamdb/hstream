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

import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.List                    (find)
import           Data.Maybe                   (isJust)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Text.Lazy               as TL
import qualified Data.Vector                  as V
import           Data.Word                    (Word32)
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Delete, Get, JSON, Post,
                                               ReqBody, type (:>), (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils    (getServerResp,
                                               mkClientNormalRequest)
import qualified HStream.Logger               as Log
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

streamBOTOStream :: StreamBO -> Stream
streamBOTOStream (StreamBO name rep) = Stream (TL.fromStrict name) rep

createStreamHandler :: Client -> StreamBO -> Handler StreamBO
createStreamHandler hClient streamBO = liftIO $ do
  Log.debug $ "Send create stream request to HStream server. "
    <> "Stream Name: " <> Log.buildText (name streamBO)
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiCreateStream
    (mkClientNormalRequest (streamBOTOStream streamBO))
  -- FIXME: return Nothing when failed
  maybe (StreamBO "" 0) streamToStreamBO <$> getServerResp resp

listStreamsHandler :: Client -> Handler [StreamBO]
listStreamsHandler hClient = liftIO $ do
  Log.debug "Send list streams request to HStream server. "
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiListStreams $ mkClientNormalRequest ListStreamsRequest
  maybe [] (V.toList . V.map streamToStreamBO . listStreamsResponseStreams) <$> getServerResp resp

deleteStreamHandler :: Client -> String -> Handler Bool
deleteStreamHandler hClient sName = liftIO $ do
  Log.debug $ "Send delete stream request to HStream server. "
    <> "Stream Name: " <> Log.buildString sName
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiDeleteStream
    (mkClientNormalRequest def
      { deleteStreamRequestStreamName = TL.pack sName
      , deleteStreamRequestIgnoreNonExist = False } )
  isJust <$> getServerResp resp

-- FIXME: This is broken.
getStreamHandler :: Client -> T.Text -> Handler (Maybe StreamBO)
getStreamHandler hClient sName = do
  liftIO . Log.debug $ "Send get stream request to HStream server. "
    <> "Stream Name: " <> Log.buildText sName
  streams <- listStreamsHandler hClient
  return $ find (\StreamBO{..} -> sName == name) streams

streamServer :: Client -> Server StreamsAPI
streamServer hClient = listStreamsHandler hClient
                  :<|> createStreamHandler hClient
                  :<|> deleteStreamHandler hClient
                  :<|> getStreamHandler hClient
