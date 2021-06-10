{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.Admin.Server.Stream (
  StreamsAPI, streamServer
) where

import           Control.Monad.IO.Class   (liftIO)
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text)
import           GHC.Generics             (Generic)
import           Servant                  (Capture, Delete, Get, JSON,
                                           PlainText, Post, ReqBody, type (:>),
                                           (:<|>) (..))
import           Servant.Server           (Handler, Server)
import qualified Z.Data.CBytes            as ZDC

import           HStream.Connector.HStore as HCH
import           HStream.Store            as HS

data StreamBO = StreamBO
  {
    name              :: Text,
    replicationFactor :: Int,
    beginTimestamp    :: Int
  } deriving (Eq, Show, Generic)
instance ToJSON StreamBO
instance FromJSON StreamBO

type StreamsAPI =
  "streams" :> Get '[JSON] [StreamBO]
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  :<|> "streams" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "streams" :> Capture "name" String :> Get '[JSON] Bool

streams :: [StreamBO]
streams = [StreamBO "test" 0 0, StreamBO "test2" 1 0]

queryStreamHandler :: HS.LDClient -> String -> Handler Bool
queryStreamHandler ldClient name = do
  res <- liftIO $ doesStreamExists ldClient (mkStreamName $ ZDC.pack name)
  return True

removeStreamHandler :: HS.LDClient -> String -> Handler Bool
removeStreamHandler ldClient name = do
  liftIO $ removeStream ldClient (mkStreamName $ ZDC.pack name)
  return True

createStreamHandler :: HS.LDClient -> StreamBO -> Handler StreamBO
createStreamHandler ldClient stream = do
  liftIO $ createStream ldClient (HCH.transToStreamName $ name stream)
          (LogAttrs $ HsLogAttrs (replicationFactor stream) Map.empty)
  return stream

fetchStreamHandler :: HS.LDClient -> Handler [StreamBO]
fetchStreamHandler ldClient = do
  streamNames <- liftIO $ findStreams ldClient True
  -- liftIO (print (show streamNames))
  return streams

streamServer :: HS.LDClient -> Server StreamsAPI
streamServer ldClient = (fetchStreamHandler ldClient) :<|> (createStreamHandler ldClient) :<|> (removeStreamHandler ldClient) :<|> (queryStreamHandler ldClient)
