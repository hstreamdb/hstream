module HStream.Store.Stream
  ( -- * Client Record Types
    ProducerRecord (..)
  , ConsumerRecord (..)
  , encodeRecord
  , decodeRecord

  -- * Stream Client
  , StreamClient
  , newStreamClient
  , getTailSequenceNum
    -- ** Client Settings
  , setClientSettings
  , getClientSettings
  , getMaxPayloadSize
    -- ** Sequence Number
  , SequenceNum (unSequenceNum)
  , FFI.sequenceNumInvalid
    -- ** Data Record
  , DataRecord (..)
    -- ** KeyType
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable

    -- * Topic
  , module HStream.Store.Stream.Topic

    -- * Writer
  , module HStream.Store.Stream.Appender

    -- * Reader
  , module HStream.Store.Stream.Reader

    -- * Checkpoint
  , module HStream.Store.Stream.Checkpoint
  ) where

import           Control.Monad                   (void)
import           Data.Int                        (Int64)
import           Foreign.ForeignPtr              (newForeignPtr, withForeignPtr)
import           Foreign.Ptr                     (nullPtr)
import           GHC.Generics                    (Generic)
import           GHC.Stack                       (HasCallStack)
import           Z.Data.CBytes                   (CBytes)
import qualified Z.Data.CBytes                   as ZC
import qualified Z.Data.JSON                     as JSON
import qualified Z.Data.MessagePack              as MP
import           Z.Data.Vector                   (Bytes)
import qualified Z.Foreign                       as Z

import qualified HStream.Store.Exception         as E
import           HStream.Store.Internal.FFI      (DataRecord (..),
                                                  SequenceNum (..),
                                                  StreamClient (..),
                                                  TopicID (..))
import qualified HStream.Store.Internal.FFI      as FFI
import           HStream.Store.Stream.Appender
import           HStream.Store.Stream.Checkpoint
import           HStream.Store.Stream.Reader
import           HStream.Store.Stream.Topic

-------------------------------------------------------------------------------

data ProducerRecord = ProducerRecord
  { dataInTopic     :: Topic
  , dataInKey       :: Maybe CBytes
  , dataInValue     :: Bytes
  , dataInTimestamp :: Int64
  } deriving (Show, Generic, JSON.JSON, MP.MessagePack)

encodeRecord :: ProducerRecord -> Bytes
encodeRecord = JSON.encode

data ConsumerRecord = ConsumerRecord
  { dataOutTopic     :: Topic
  , dataOutOffset    :: SequenceNum
  , dataOutKey       :: Maybe CBytes
  , dataOutValue     :: Bytes
  , dataOutTimestamp :: Int64
  } deriving (Show, Generic, JSON.JSON, MP.MessagePack)

decodeRecord :: DataRecord -> ConsumerRecord
decodeRecord DataRecord{..} = do
  case JSON.decode' recordPayload of
    Left _err -> error "JSON decode error!"
    Right ProducerRecord{..} ->
      ConsumerRecord { dataOutTopic     = dataInTopic
                     , dataOutOffset    = recordLSN
                     , dataOutKey       = dataInKey
                     , dataOutValue     = dataInValue
                     , dataOutTimestamp = dataInTimestamp
                     }

-------------------------------------------------------------------------------

-- | Create a new stream client from config url.
newStreamClient :: HasCallStack => CBytes -> IO StreamClient
newStreamClient config = ZC.withCBytesUnsafe config $ \config' -> do
  (client', _) <- Z.withPrimUnsafe nullPtr $ \client'' ->
    E.throwStreamErrorIfNotOK $ FFI.c_new_logdevice_client config' client''
  StreamClient <$> newForeignPtr FFI.c_free_logdevice_client_fun client'

getTailSequenceNum :: StreamClient -> TopicID -> IO SequenceNum
getTailSequenceNum client (TopicID topicid) =
  withForeignPtr (unStreamClient client) $ \p ->
    SequenceNum <$> FFI.c_ld_client_get_tail_lsn_sync p topicid

-- | Returns the maximum permitted payload size for this client.
--
-- The default is 1MB, but this can be increased via changing the
-- max-payload-size setting.
getMaxPayloadSize :: StreamClient -> IO Word
getMaxPayloadSize (StreamClient client) =
  withForeignPtr client FFI.c_ld_client_get_max_payload_size

-- | Change settings for the Client.
--
-- Settings that are commonly used on the client:
--
-- connect-timeout
--    Connection timeout
--
-- handshake-timeout
--    Timeout for LogDevice protocol handshake sequence
--
-- num-workers
--    Number of worker threads on the client
--
-- client-read-buffer-size
--    Number of records to buffer while reading
--
-- max-payload-size
--    The maximum payload size that could be appended by the client
--
-- ssl-boundary
--    Enable SSL in cross-X traffic, where X is the setting. Example: if set
--    to "rack", all cross-rack traffic will be sent over SSL. Can be one of
--    "none", "node", "rack", "row", "cluster", "dc" or "region". If a value
--    other than "none" or "node" is specified, --my-location has to be
--    specified as well.
--
-- my-location
--    Specifies the location of the machine running the client. Used for
--    determining whether to use SSL based on --ssl-boundary. Format:
--    "{region}.{dc}.{cluster}.{row}.{rack}"
--
-- client-initial-redelivery-delay
--    Initial delay to use when downstream rejects a record or gap
--
-- client-max-redelivery-delay
--    Maximum delay to use when downstream rejects a record or gap
--
-- on-demand-logs-config
--    Set this to true if you want the client to get log configuration on
--    demand from the server when log configuration is not included in the
--    main config file.
--
-- enable-logsconfig-manager
--    Set this to true if you want to use the internal replicated storage for
--    logs configuration, this will ignore loading the logs section from the
--    config file.
setClientSettings :: HasCallStack => StreamClient -> CBytes -> CBytes -> IO ()
setClientSettings (StreamClient client) key val =
  withForeignPtr client $ \client' ->
  ZC.withCBytesUnsafe key $ \key' ->
  ZC.withCBytesUnsafe val $ \val' -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_client_set_settings client' key' val'

getClientSettings :: StreamClient -> CBytes -> IO Bytes
getClientSettings (StreamClient client) key =
  withForeignPtr client $ \client' ->
  ZC.withCBytesUnsafe key $ \key' ->
    Z.fromStdString $ FFI.c_ld_client_get_settings client' key'
