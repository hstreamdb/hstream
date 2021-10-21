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
  , StreamBO(..), Records(..), AppendResult(..)
  , buildRecords
  ) where

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
import           Data.Maybe
import           Data.Swagger                 (ToSchema (..))
import qualified Data.Text                    as T
import qualified Data.Text.Encoding           as T
import qualified Data.Text.Lazy               as TL
import qualified Data.Vector                  as V
import           Data.Word                    (Word32)
import           GHC.Generics                 (Generic)
import           HStream.HTTP.Server.Utils    (getServerResp,
                                               mkClientNormalRequest)
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
streamToStreamBO (Stream name rep) = StreamBO (TL.toStrict name) rep

streamBOTOStream :: StreamBO -> Stream
streamBOTOStream (StreamBO name rep) = Stream (TL.fromStrict name) rep

data Records = Records
  { records :: T.Text
  } deriving (Eq, Show, Generic)

instance A.ToJSON   Records
instance A.FromJSON Records
instance ToSchema   Records

processRecords :: Records -> V.Vector BS.ByteString
processRecords = V.fromList . BS.lines . BSE.decodeLenient . T.encodeUtf8 . records

buildRecords :: [[(T.Text, A.Value)]] -> Records
buildRecords = Records . T.decodeUtf8 . BS.concat . BSL.toChunks .
  BSL.encode . BSL.unlines . map
    (A.encode . HM.fromList)

data AppendResult = AppendResult
  { appendResultRecordIds :: V.Vector RecordId
  } deriving (Eq, Show, Generic)

instance A.ToJSON   AppendResult
instance A.FromJSON AppendResult
instance ToSchema   AppendResult

type StreamsAPI
  =    "streams" :> Get '[JSON] [StreamBO]
  -- ^ List all streams
  :<|> "streams" :> ReqBody '[JSON] StreamBO :> Post '[JSON] StreamBO
  -- ^ Create a new stream
  :<|> "streams" :> Capture "name" T.Text :> Delete '[JSON] Bool
  -- ^ Delete a stream
  :<|> "streams" :> Capture "name" T.Text :> Get '[JSON] (Maybe StreamBO)
  -- ^ Get a stream
  :<|> "streams" :> Capture "name" T.Text :> "publish" :> ReqBody '[JSON] Records :> Post '[JSON] AppendResult
  -- ^ Append records to a stream

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

deleteStreamHandler :: Client -> T.Text -> Handler Bool
deleteStreamHandler hClient sName = liftIO $ do
  Log.debug $ "Send delete stream request to HStream server. "
           <> "Stream Name: " <> Log.buildText sName
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiDeleteStream $
    mkClientNormalRequest def
      { deleteStreamRequestStreamName     = TL.fromStrict sName
      , deleteStreamRequestIgnoreNonExist = False
      }
  isJust <$> getServerResp resp

-- FIXME: This is broken.
getStreamHandler :: Client -> T.Text -> Handler (Maybe StreamBO)
getStreamHandler hClient sName = do
  liftIO . Log.debug $ "Send get stream request to HStream server. "
                    <> "Stream Name: " <> Log.buildText sName
  (L.find $ \StreamBO{..} -> sName == name) <$> listStreamsHandler hClient

appendHandler :: Client
              -> T.Text -> Records
              -> Handler AppendResult
appendHandler hClient sName recs = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  timestamp      <- getProtoTimestamp
  Log.debug $ ""
           <> "Stream Name: " <> Log.buildText sName
  let header  = buildRecordHeader HStreamRecordHeader_FlagJSON Map.empty timestamp TL.empty
      record  = buildRecord header `V.map` processRecords recs
  resp <- hstreamApiAppend . mkClientNormalRequest $ def
      { appendRequestStreamName = TL.fromStrict sName
      , appendRequestRecords    = record
      }
  AppendResult . appendResponseRecordIds . fromJust <$> getServerResp resp

streamServer :: Client -> Server StreamsAPI
streamServer hClient = listStreamsHandler  hClient
                  :<|> createStreamHandler hClient
                  :<|> deleteStreamHandler hClient
                  :<|> getStreamHandler    hClient
                  :<|> appendHandler       hClient
