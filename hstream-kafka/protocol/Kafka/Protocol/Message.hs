{-# LANGUAGE DuplicateRecordFields #-}

module Kafka.Protocol.Message
  ( RequestHeader (..)
  , ResponseHeader (..)
  , putResponseHeader
  , runPutResponseHeaderLazy
  , getResponseHeader

  , module Kafka.Protocol.Message.Struct
  ) where

import qualified Data.ByteString.Lazy          as BL
import           Data.Int

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message.Struct

data RequestHeader = RequestHeader
  { requestApiKey        :: {-# UNPACK #-} !ApiKey
  , requestApiVersion    :: {-# UNPACK #-} !Int16
  , requestCorrelationId :: {-# UNPACK #-} !Int32
  , requestClientId      :: !(Maybe NullableString)
  , requesteTaggedFields :: !(Maybe TaggedFields)
  } deriving (Show, Eq)

instance Serializable RequestHeader where
  get = do
    requestApiKey        <- get
    requestApiVersion    <- get
    requestCorrelationId <- get
    let (reqHeaderVer, _) = getHeaderVersion requestApiKey requestApiVersion
    case reqHeaderVer of
      2 -> do requestClientId <- getMaybe True
              requesteTaggedFields <- getMaybe True
              pure RequestHeader{..}
      1 -> do requestClientId <- getMaybe True
              let requesteTaggedFields = Nothing
               in pure RequestHeader{..}
      0 -> let requestClientId = Nothing
               requesteTaggedFields = Nothing
            in pure RequestHeader{..}
      v -> error $ "Unknown request header version" <> show v
  {-# INLINE get #-}

  put RequestHeader{..} =
       put requestApiKey
    <> put requestApiVersion
    <> put requestCorrelationId
    <> putMaybe requestClientId
    <> putMaybe requesteTaggedFields
  {-# INLINE put #-}

data ResponseHeader = ResponseHeader
  { responseCorrelationId :: {-# UNPACK #-} !Int32
  , responseTaggedFields  :: !(Maybe TaggedFields)
  } deriving (Show, Eq)

putResponseHeader :: ResponseHeader -> Builder
putResponseHeader ResponseHeader{..} =
     put responseCorrelationId
  <> putMaybe responseTaggedFields
{-# INLINE putResponseHeader #-}

runPutResponseHeaderLazy :: ResponseHeader -> BL.ByteString
runPutResponseHeaderLazy = toLazyByteString . putResponseHeader
{-# INLINE runPutResponseHeaderLazy #-}

getResponseHeader :: Int16 -> Parser ResponseHeader
getResponseHeader 0 = do responseCorrelationId <- get
                         let responseTaggedFields = Nothing
                          in pure ResponseHeader{..}
getResponseHeader 1 = do responseCorrelationId <- get
                         responseTaggedFields <- getMaybe True
                         pure ResponseHeader{..}
getResponseHeader v = error $ "Unknown response header version " <> show v
