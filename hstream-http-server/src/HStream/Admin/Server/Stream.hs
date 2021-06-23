{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp  #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.Admin.Server.Stream (
  StreamsAPI, streamServer
) where

import           Control.Monad.IO.Class   (liftIO)
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text)
import qualified Data.Text                as T
import           GHC.Generics             (Generic)
import           GHC.Int                  (Int64)
import           Servant                  (Capture, Delete, Get, JSON,
                                           PlainText, Post, ReqBody, type (:>),
                                           (:<|>) (..))
import           Servant.Server           (Handler, Server)
import qualified Z.Data.CBytes            as ZDC

import           HStream.Connector.HStore as HCH
import qualified HStream.Store            as HS
import           HStream.Utils.Converter  (cbytesToText)

data StreamBO = StreamBO
  {
    name              :: Text,
    replicationFactor :: Int,
    beginTimestamp    :: Maybe Int64
  } deriving (Eq, Show, Generic)
instance ToJSON StreamBO
instance FromJSON StreamBO

type StreamsAPI =
  "streams" :> Get '[JSON] [StreamBO]
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  :<|> "streams" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "streams" :> Capture "name" String :> Get '[JSON] (Maybe StreamBO)

queryStreamHandler :: HS.LDClient -> String -> Handler (Maybe StreamBO)
queryStreamHandler ldClient name = do
  res <- liftIO $ do
    exists <- HS.doesStreamExists ldClient (HS.mkStreamName $ ZDC.pack name)
    if exists
      then do
        let streamName = HS.mkStreamName $ ZDC.pack name
        rep <- HS.getStreamReplicaFactor ldClient streamName
        ts  <- HS.getStreamHeadTimestamp ldClient streamName
        return $ Just $ StreamBO (T.pack name) rep ts
      else return Nothing

  return res

removeStreamHandler :: HS.LDClient -> String -> Handler Bool
removeStreamHandler ldClient name = do
  liftIO $ HS.removeStream ldClient (HS.mkStreamName $ ZDC.pack name)
  return True

createStreamHandler :: HS.LDClient -> StreamBO -> Handler StreamBO
createStreamHandler ldClient stream = do
  liftIO $ HS.createStream ldClient (HCH.transToStreamName $ name stream)
          (HS.LogAttrs $ HS.HsLogAttrs (replicationFactor stream) Map.empty)
  return stream

fetchStreamHandler :: HS.LDClient -> Handler [StreamBO]
fetchStreamHandler ldClient = do
  streams <- liftIO $ do
    streamNames <- HS.findStreams ldClient True
    let names = (\streamName -> cbytesToText $ HS.getStreamName streamName) <$> streamNames
    replicationFactors <- sequence $ (HS.getStreamReplicaFactor ldClient) <$> streamNames
    timestamps <- sequence $ (HS.getStreamHeadTimestamp ldClient) <$> streamNames

    return [StreamBO name rep ts | name <- names
                                 | rep  <- replicationFactors
                                 | ts   <- timestamps
           ]

  return streams

streamServer :: HS.LDClient -> Server StreamsAPI
streamServer ldClient = (fetchStreamHandler ldClient) :<|> (createStreamHandler ldClient) :<|> (removeStreamHandler ldClient) :<|> (queryStreamHandler ldClient)
