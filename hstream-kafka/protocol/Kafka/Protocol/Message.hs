{-# LANGUAGE DuplicateRecordFields #-}

module Kafka.Protocol.Message
  ( RequestHeader (..)
  , ResponseHeader (..)
  , getResponseHeaderV0
  , getResponseHeaderV1
  , putResponseHeader
  , runPutResponseHeaderLazy
  , Unsupported (..)

  , module Kafka.Protocol.Message.Struct
  ) where

import qualified Data.ByteString.Lazy          as BL
import           Data.Int
import           GHC.Generics

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message.Struct

data Unsupported = Unsupported
  deriving (Show, Eq, Generic)

instance Serializable Unsupported

data RequestHeader = RequestHeader
  { requestApiKey        :: {-# UNPACK #-} !ApiKey
  , requestApiVersion    :: {-# UNPACK #-} !Int16
  , requestCorrelationId :: {-# UNPACK #-} !Int32
  , requestClientId      :: !(Either Unsupported NullableString)
  , requesteTaggedFields :: !(Either Unsupported TaggedFields)
  } deriving (Show, Eq)

instance Serializable RequestHeader where
  get = do
    requestApiKey        <- get
    requestApiVersion    <- get
    requestCorrelationId <- get
    let (reqHeaderVer, _) = getHeaderVersion requestApiKey requestApiVersion
    case reqHeaderVer of
      2 -> do requestClientId <- getEither True
              requesteTaggedFields <- getEither True
              pure RequestHeader{..}
      1 -> do requestClientId <- getEither True
              let requesteTaggedFields = Left Unsupported
               in pure RequestHeader{..}
      0 -> let requestClientId = Left Unsupported
               requesteTaggedFields = Left Unsupported
            in pure RequestHeader{..}
      v -> error $ "Unknown request header version" <> show v
  {-# INLINE get #-}

  put RequestHeader{..} =
       put requestApiKey
    <> put requestApiVersion
    <> put requestCorrelationId
    <> putEither requestClientId
    <> putEither requesteTaggedFields
  {-# INLINE put #-}

data ResponseHeader = ResponseHeader
  { responseCorrelationId :: {-# UNPACK #-} !Int32
  , responseTaggedFields  :: !(Either Unsupported TaggedFields)
  } deriving (Show, Eq)

getResponseHeaderV0 :: Parser ResponseHeader
getResponseHeaderV0 = do
  responseCorrelationId <- get
  responseTaggedFields  <- pure $ Left Unsupported
  pure ResponseHeader{..}

getResponseHeaderV1 :: Parser ResponseHeader
getResponseHeaderV1 = do
  responseCorrelationId <- get
  responseTaggedFields  <- getEither True
  pure ResponseHeader{..}

putResponseHeader :: ResponseHeader -> Builder
putResponseHeader ResponseHeader{..} =
     put responseCorrelationId
  <> putEither responseTaggedFields
{-# INLINE putResponseHeader #-}

runPutResponseHeaderLazy :: ResponseHeader -> BL.ByteString
runPutResponseHeaderLazy = toLazyByteString . putResponseHeader
{-# INLINE runPutResponseHeaderLazy #-}
