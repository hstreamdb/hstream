{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp  #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Stream
  ( StreamsAPI, streamServer
  , listStreamsHandler
  , StreamBO(..), AppendBO(..), AppendResult(..)
  , buildAppendBO
  ) where

import           Control.Monad                (void)
import           Control.Monad.IO.Class       (liftIO)
import qualified Data.Aeson                   as A
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Base64       as BSE
import qualified Data.ByteString.Base64.Lazy  as BSL
import qualified Data.ByteString.Char8        as BS
import qualified Data.ByteString.Lazy         as BSL
import qualified Data.ByteString.Lazy.Char8   as BSL
import qualified Data.HashMap.Strict          as HM
import qualified Data.List                    as L
import qualified Data.Map                     as Map
import           Data.Swagger                 (ToSchema (..))
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import qualified Data.Vector                  as V
import           Data.Word                    (Word32)
import           GHC.Generics                 (Generic)
import           HStream.HTTP.Server.Utils
import qualified HStream.Logger               as Log
import           HStream.Server.HStreamApi
import           HStream.Utils                (buildRecord, buildRecordHeader,
                                               getProtoTimestamp)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      hiding (Stream)

-- BO is short for Business Object
data StreamBO = StreamBO
  { name              :: T.Text
  , replicationFactor :: Word32
  } deriving (Eq, Show, Generic)

instance A.ToJSON   StreamBO
instance A.FromJSON StreamBO
instance ToSchema   StreamBO

streamToStreamBO :: Stream -> StreamBO
streamToStreamBO (Stream name rep) = StreamBO name rep

streamBOTOStream :: StreamBO -> Stream
streamBOTOStream (StreamBO name rep) = Stream name rep

data AppendBO = AppendBO
  { streamName :: T.Text
  , records    :: T.Text
  } deriving (Eq, Show, Generic)

instance A.ToJSON   AppendBO
instance A.FromJSON AppendBO
instance ToSchema   AppendBO

processRecords :: AppendBO -> V.Vector BS.ByteString
processRecords = V.fromList . BS.lines . BSE.decodeLenient . T.encodeUtf8 . records

buildAppendBO :: T.Text -> [[(T.Text, A.Value)]] -> AppendBO
buildAppendBO streamName = AppendBO streamName . T.decodeUtf8 . BS.concat . BSL.toChunks .
  BSL.encode . BSL.unlines . map
    (A.encode . HM.fromList)

data AppendResult = AppendResult
  { recordIds :: V.Vector RecordId
  } deriving (Eq, Show, Generic)

instance A.ToJSON   AppendResult
instance A.FromJSON AppendResult
instance ToSchema   AppendResult

type StreamsAPI
  =    "streams" :> Get '[JSON] [StreamBO]
  -- ^ List all streams
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  -- ^ Create a new stream
  :<|> "streams" :> Capture "name" T.Text :> Delete '[JSON] ()
  -- ^ Delete a stream
  :<|> "streams" :> Capture "name" T.Text :> Get '[JSON] (Maybe StreamBO)
  -- ^ Get a stream
  :<|> "streams" :> "publish" :> ReqBody '[JSON] AppendBO :> Post '[JSON] AppendResult
  -- ^ Append records to a stream

createStreamHandler :: Client -> StreamBO -> Handler StreamBO
createStreamHandler hClient streamBO = do
  resp <- liftIO $ do
    Log.debug $ "Send create stream request to HStream server. "
             <> "Stream Name: " <> Log.buildText (name streamBO)
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiCreateStream $ mkClientNormalRequest (streamBOTOStream streamBO)
  streamToStreamBO <$> getServerResp' resp

listStreamsHandler :: Client -> Handler [StreamBO]
listStreamsHandler hClient = do
  resp <- liftIO $ do
    Log.debug "Send list streams request to HStream server. "
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiListStreams $ mkClientNormalRequest ListStreamsRequest
  V.toList . V.map streamToStreamBO . listStreamsResponseStreams <$> getServerResp' resp

deleteStreamHandler :: Client -> T.Text -> Handler ()
deleteStreamHandler hClient sName = do
  resp <- liftIO $ do
    Log.debug $ "Send delete stream request to HStream server. "
             <> "Stream Name: " <> Log.buildText sName
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiDeleteStream $
      mkClientNormalRequest def
        { deleteStreamRequestStreamName     = sName
        , deleteStreamRequestIgnoreNonExist = False
        }
  void $ getServerResp' resp

-- FIXME: This is broken.
getStreamHandler :: Client -> T.Text -> Handler (Maybe StreamBO)
getStreamHandler hClient sName = do
  liftIO . Log.debug $ "Send get stream request to HStream server. "
                    <> "Stream Name: " <> Log.buildText sName
  (L.find $ \StreamBO{..} -> sName == name) <$> listStreamsHandler hClient

appendHandler :: Client -> AppendBO -> Handler AppendResult
appendHandler hClient appendBO = do
  resp <- liftIO $ do
    HStreamApi{..} <- hstreamApiClient hClient
    timestamp      <- getProtoTimestamp
    Log.debug $ "Append records to HStream server. "
             <> "Stream Name: " <> Log.buildText (streamName appendBO)
    let header  = buildRecordHeader HStreamRecordHeader_FlagJSON Map.empty timestamp T.empty
        record  = buildRecord header `V.map` processRecords appendBO
    hstreamApiAppend . mkClientNormalRequest $ def
      { appendRequestStreamName = streamName appendBO
      , appendRequestRecords    = record
      }
  AppendResult . appendResponseRecordIds <$> getServerResp' resp

streamServer :: Client -> Server StreamsAPI
streamServer hClient = listStreamsHandler  hClient
                  :<|> createStreamHandler hClient
                  :<|> deleteStreamHandler hClient
                  :<|> getStreamHandler    hClient
                  :<|> appendHandler       hClient
