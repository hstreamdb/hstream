{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedLists           #-}
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
import           Database.MySQL.Base                  (ERRException)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server        (ServerResponse)
import           ZooKeeper.Exception

import qualified HStream.Logger                       as Log
import           HStream.Server.Persistence.Exception (PersistenceException)
import           HStream.SQL.Exception                (SomeSQLException,
                                                       formatSomeSQLException)
import qualified HStream.Store                        as Store
import           HStream.Utils                        (TaskStatus,
                                                       returnBiDiStreamingResp,
                                                       returnErrResp,
                                                       returnServerStreamingResp)

defaultExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
defaultExceptionHandle = mkExceptionHandle $ defaultHandlers returnErrResp

appendStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
appendStreamExceptionHandle = prependDefaultHandler handlers returnErrResp
  where
    handlers = [
      Handler (\(err :: Store.NOTFOUND) ->
        returnErrResp StatusUnavailable $ mkStatusDetails err),
      Handler (\(err :: Store.NOTINSERVERCONFIG) ->
        returnErrResp StatusUnavailable $ mkStatusDetails err)
      ]

defaultServerStreamExceptionHandle :: ExceptionHandle (ServerResponse 'ServerStreaming a)
defaultServerStreamExceptionHandle = mkExceptionHandle $ defaultHandlers returnServerStreamingResp

defaultBiDiStreamExceptionHandle :: ExceptionHandle (ServerResponse 'BiDiStreaming a)
defaultBiDiStreamExceptionHandle = mkExceptionHandle $ defaultHandlers returnBiDiStreamingResp

-- If user needs to deal with some exceptions specifically, the user can add such handlers in the very front of the
-- defaultHandlers, eg. Some rpc handlers may require cleansing after exception capture.
prependDefaultHandler :: Handlers t a -> RetFun t a -> ExceptionHandle (ServerResponse t a)
prependDefaultHandler handlers retFun = mkExceptionHandle $ handlers ++ defaultHandlers retFun

--------------------------------------------------------------------------------

newtype InvalidArgument = InvalidArgument String
  deriving (Show)
instance Exception InvalidArgument

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

newtype SubscriptionIdNotFound = SubscriptionIdNotFound Text
  deriving (Show)
instance Exception SubscriptionIdNotFound

newtype SubscriptionIdOccupied = SubscriptionIdOccupied Text
  deriving (Show)
instance Exception SubscriptionIdOccupied

data ObjectNotExist = ObjectNotExist
  deriving (Show)
instance Exception ObjectNotExist

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist

newtype ConnectorAlreadyExists = ConnectorAlreadyExists TaskStatus
  deriving (Show)
instance Exception ConnectorAlreadyExists

newtype ConnectorRestartErr = ConnectorRestartErr TaskStatus
  deriving (Show)
instance Exception ConnectorRestartErr

data ConnectorNotExist = ConnectorNotExist
  deriving (Show)
instance Exception ConnectorNotExist

newtype ConsumerExist = ConsumerExist Text
  deriving (Show)
instance Exception ConsumerExist

data FoundActiveConsumers = FoundActiveConsumers
  deriving (Show)
instance Exception FoundActiveConsumers

data SubscriptionIsDeleting = SubscriptionIsDeleting
  deriving (Show)
instance Exception SubscriptionIsDeleting

data FoundActiveSubscription = FoundActiveSubscription
  deriving (Show)
instance Exception FoundActiveSubscription

data SubscriptionWatchOnDifferentNode = SubscriptionWatchOnDifferentNode
  deriving (Show)
instance Exception SubscriptionWatchOnDifferentNode

data DataInconsistency = DataInconsistency Text Text
instance Show DataInconsistency where
  show (DataInconsistency streamName key) = "partition " <> show key
                                         <> " of stream " <> show streamName
                                         <> " doesn't appear in store,"
                                         <> " but exists in zk."
instance Exception DataInconsistency

data StreamExists = StreamExists Bool Bool
  deriving (Show)
instance Exception StreamExists

handleStreamExists :: StreamExists -> StatusDetails
handleStreamExists (StreamExists zkExists storeExists)
  | zkExists && storeExists = "StreamExists: Stream has been created"
  | zkExists    = "StreamExists: Inconsistency found. The stream was created, but not persisted to disk"
  | storeExists = "StreamExists: Inconsistency found. The stream was persisted to disk"
                <> ", but no record of creating the stream is found."
  | otherwise   = "Impossible happened: Stream does not exist, but some how throw stream exist exception"

--------------------------------------------------------------------------------

type RetFun t a = (StatusCode -> StatusDetails -> IO (ServerResponse t a))
type Handlers t a = [Handler (ServerResponse t a)]
type ExceptionHandle a = IO a -> IO a

defaultHandlers :: RetFun t a -> Handlers t a
defaultHandlers retFun = [
  Handler (\(err :: SomeSQLException) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusInvalidArgument $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(err :: InvalidArgument) -> do
    Log.warning $ Log.buildString' err
    retFun StatusInvalidArgument $ mkStatusDetails err),
  Handler (\(err :: Store.EXISTS) -> do
    Log.warning $ Log.buildString' err
    retFun StatusAlreadyExists "Stream already exists in store"),
  Handler (\(err :: StreamExists) -> do
    Log.warning $ Log.buildString' err
    retFun StatusAlreadyExists $ handleStreamExists err),
  Handler (\(err :: ObjectNotExist) -> do
    Log.warning $ Log.buildString' err
    retFun StatusNotFound "Object not found"),
  Handler (\(err :: SubscriptionWatchOnDifferentNode) -> do
    Log.warning $ Log.buildString' err
    retFun StatusAborted "Subscription is bound to a different node"),
  Handler (\(err :: FoundActiveConsumers) -> do
    Log.warning $ Log.buildString' err
    retFun StatusFailedPrecondition "Subscription still has active consumers"),
  Handler (\(err :: SubscriptionIsDeleting) -> do
    Log.warning $ Log.buildString' err
    retFun StatusAborted "Subscription is been deleting, please wait a while"),
  Handler (\(err :: FoundActiveSubscription) -> do
    Log.warning $ Log.buildString' err
    retFun StatusFailedPrecondition "Stream still has active consumers"),
  Handler (\(err :: Store.SomeHStoreException) -> do
    Log.warning $ Log.buildString' err
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler(\(err@(ConsumerExist name) :: ConsumerExist) -> do
    Log.warning $ Log.buildString' err
    retFun StatusInvalidArgument $ StatusDetails ("Consumer " <> encodeUtf8 name <> " exist")),
  Handler (\(err :: PersistenceException) -> do
    Log.warning $ Log.buildString' err
    retFun StatusAborted $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err :: QueryTerminatedOrNotExist) -> do
    Log.warning $ Log.buildString' err
    retFun StatusInvalidArgument "Query is already terminated or does not exist"),
  Handler (\(err :: StreamNotExist) -> do
    Log.warning $ Log.buildString' err
    retFun StatusNotFound $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err@(SubscriptionIdNotFound subId) :: SubscriptionIdNotFound) -> do
    Log.warning $ Log.buildString' err
    retFun StatusNotFound $ StatusDetails ("Subscription ID " <> encodeUtf8 subId <> " can not be found")),
  Handler (\(err :: IOException) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err :: ERRException) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusInternal $ "Mysql error " <> mkStatusDetails err),
  Handler (\(err :: ConnectorAlreadyExists) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusAlreadyExists $ mkStatusDetails err),
  Handler (\(err :: ConnectorRestartErr) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusInternal $ mkStatusDetails err),
  Handler (\(err :: ConnectorNotExist) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusNotFound "Connector not found"),
  Handler (\(err :: ZNODEEXISTS) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusAlreadyExists $ "Zookeeper exception: " <> mkStatusDetails err),
  Handler (\(err :: ZNONODE) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusNotFound $ "Zookeeper exception: " <> mkStatusDetails err),
  Handler (\(err :: ZooException) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusInternal $ "Zookeeper exception: " <> mkStatusDetails err),
  Handler (\(err :: SomeException) -> do
    Log.fatal $ Log.buildString' err
    retFun StatusUnknown $ "UnKnown exception: " <> mkStatusDetails err)
  ]

mkExceptionHandle :: Handlers t a -> ExceptionHandle (ServerResponse t a)
mkExceptionHandle = flip catches

mkStatusDetails :: Exception a => a -> StatusDetails
mkStatusDetails = StatusDetails . BS.pack . displayException
