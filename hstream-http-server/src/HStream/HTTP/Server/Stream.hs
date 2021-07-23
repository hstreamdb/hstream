{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp  #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Stream (
  StreamsAPI, streamServer, listStreamsHandler
) where

import           Control.Monad.IO.Class   (liftIO)
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Map.Strict          as Map
import           Data.Swagger             (ToSchema)
import           Data.Text                (Text)
import qualified Data.Text                as T
import           GHC.Generics             (Generic)
import           GHC.Int                  (Int64)
import           Servant                  (Capture, Delete, Get, JSON, Post,
                                           ReqBody, type (:>), (:<|>) (..))
import           Servant.Server           (Handler, Server)
import qualified Z.Data.CBytes            as ZDC

import           HStream.Connector.HStore as HCH
import qualified HStream.Store            as HS
import           HStream.Utils.Converter  (cBytesToText)

-- BO is short for Business Object
data StreamBO = StreamBO
  { name              :: Text
  , replicationFactor :: Int
  , beginTimestamp    :: Maybe Int64
  } deriving (Eq, Show, Generic)

instance ToJSON StreamBO
instance FromJSON StreamBO
instance ToSchema StreamBO

type StreamsAPI =
  "streams" :> Get '[JSON] [StreamBO]
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  :<|> "streams" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "streams" :> Capture "name" String :> Get '[JSON] (Maybe StreamBO)

queryStreamHandler :: HS.LDClient -> String -> Handler (Maybe StreamBO)
queryStreamHandler ldClient s = liftIO $ do
  exists <- HS.doesStreamExists ldClient (HS.mkStreamId HS.StreamTypeStream $ ZDC.pack s)
  if exists
    then do
      let streamName = HS.mkStreamId HS.StreamTypeStream $ ZDC.pack s
      rep <- HS.getStreamReplicaFactor ldClient streamName
      ts  <- HS.getStreamHeadTimestamp ldClient streamName
      return $ Just $ StreamBO (T.pack s) rep ts
    else return Nothing

removeStreamHandler :: HS.LDClient -> String -> Handler Bool
removeStreamHandler ldClient s = do
  liftIO $ HS.removeStream ldClient (HS.mkStreamId HS.StreamTypeStream $ ZDC.pack s)
  return True

createStreamHandler :: HS.LDClient -> StreamBO -> Handler StreamBO
createStreamHandler ldClient stream = do
  liftIO $ HS.createStream ldClient (HCH.transToStreamName $ name stream)
          (HS.LogAttrs $ HS.HsLogAttrs (replicationFactor stream) Map.empty)
  return stream

listStreamsHandler :: HS.LDClient -> Handler [StreamBO]
listStreamsHandler ldClient = liftIO $ do
    streamNames <- HS.findStreams ldClient HS.StreamTypeStream True
    let names = T.pack . HS.showStreamName <$> streamNames
    replicationFactors <- sequence $ HS.getStreamReplicaFactor ldClient <$> streamNames
    timestamps <- sequence $ HS.getStreamHeadTimestamp ldClient <$> streamNames

    return [StreamBO n rep ts | n <- names
                              | rep  <- replicationFactors
                              | ts   <- timestamps
           ]

streamServer :: HS.LDClient -> Server StreamsAPI
streamServer ldClient = listStreamsHandler ldClient
                   :<|> createStreamHandler ldClient
                   :<|> removeStreamHandler ldClient
                   :<|> queryStreamHandler ldClient
