{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module HStream.Server.Exception where

import           Control.Exception                    (Exception (..),
                                                       Handler (Handler),
                                                       IOException,
                                                       SomeException, catches,
                                                       displayException)
import qualified Data.ByteString.Char8                as BS
import           Data.Text                            (Text)
import           Data.Text.Encoding                   (encodeUtf8)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server        (ServerResponse (ServerBiDiResponse, ServerWriterResponse))
import           ZooKeeper.Exception

import qualified HStream.Logger                       as Log
import           HStream.Server.Persistence.Exception (PersistenceException)
import qualified HStream.Store                        as Store
import           HStream.Utils                        (mkServerErrResp)

defaultExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
defaultExceptionHandle = mkExceptionHandle $ setRespType mkServerErrResp defaultHandlers

defaultServerStreamExceptionHandle :: ExceptionHandle (ServerResponse 'ServerStreaming a)
defaultServerStreamExceptionHandle = mkExceptionHandle $ setRespType (ServerWriterResponse mempty) defaultHandlers

defaultBiDiStreamExceptionHandle :: ExceptionHandle (ServerResponse 'BiDiStreaming a)
defaultBiDiStreamExceptionHandle = mkExceptionHandle $ setRespType (ServerBiDiResponse mempty) defaultHandlers

--------------------------------------------------------------------------------
-- Exceptions shared by most of the handlers

newtype InvalidArgument = InvalidArgument String
  deriving (Show)
instance Exception InvalidArgument

data ObjectNotExist = ObjectNotExist
  deriving (Show)
instance Exception ObjectNotExist

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist

newtype SubscriptionIdNotFound = SubscriptionIdNotFound Text
  deriving (Show)
instance Exception SubscriptionIdNotFound

data ServerNotAvailable = ServerNotAvailable
  deriving (Show)
instance Exception ServerNotAvailable

--------------------------------------------------------------------------------

type MkResp t a = StatusCode -> StatusDetails -> ServerResponse t a
type Handlers a = [Handler a]
type ExceptionHandle a = IO a -> IO a

setRespType :: MkResp t a -> Handlers (StatusCode, StatusDetails) -> Handlers (ServerResponse t a)
setRespType mkResp = map (uncurry mkResp <$>)

defaultHandlers :: Handlers (StatusCode, StatusDetails)
defaultHandlers = [
  Handler (\(err :: ServerNotAvailable) -> do
    Log.warning $ Log.buildString' err
    return (StatusUnavailable, "Server is still starting")),
  Handler (\(err :: Store.EXISTS) -> do
    Log.warning $ Log.buildString' err
    return (StatusAlreadyExists, "Stream already exists in store")),
  Handler (\(err :: InvalidArgument) -> do
    Log.warning $ Log.buildString' err
    return (StatusInvalidArgument, mkStatusDetails err)),
  Handler (\(err :: ObjectNotExist) -> do
    Log.warning $ Log.buildString' err
    return (StatusNotFound, "Object not found")),
  Handler (\(err :: Store.SomeHStoreException) -> do
    Log.warning $ Log.buildString' err
    return (StatusInternal, mkStatusDetails err)),
  Handler (\(err :: PersistenceException) -> do
    Log.warning $ Log.buildString' err
    return (StatusAborted, mkStatusDetails err)),
  Handler (\(err :: StreamNotExist) -> do
    Log.warning $ Log.buildString' err
    return (StatusNotFound, mkStatusDetails err)),
  Handler (\(err@(SubscriptionIdNotFound subId) :: SubscriptionIdNotFound) -> do
    Log.warning $ Log.buildString' err
    return (StatusNotFound, StatusDetails ("Subscription ID " <> encodeUtf8 subId <> " can not be found"))),
  Handler (\(err :: IOException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, mkStatusDetails err)),
  Handler (\(err :: ZNODEEXISTS) -> do
    Log.fatal $ Log.buildString' err
    return (StatusAlreadyExists, "Zookeeper exception: " <> mkStatusDetails err)),
  Handler (\(err :: ZNONODE) -> do
    Log.fatal $ Log.buildString' err
    return (StatusNotFound, "Zookeeper exception: " <> mkStatusDetails err)),
  Handler (\(err :: ZooException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, "Zookeeper exception: " <> mkStatusDetails err)),
  Handler (\(err :: SomeException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusUnknown, "UnKnown exception: " <> mkStatusDetails err))
  ]

mkExceptionHandle :: Handlers (ServerResponse t a) -> ExceptionHandle (ServerResponse t a)
mkExceptionHandle = flip catches

mkStatusDetails :: Exception a => a -> StatusDetails
mkStatusDetails = StatusDetails . BS.pack . displayException
