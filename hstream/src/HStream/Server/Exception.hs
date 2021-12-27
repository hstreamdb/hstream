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
import qualified Data.Text                            as T
import           Database.MySQL.Base                  (ERRException)

import qualified HStream.Logger                       as Log
import           HStream.SQL.Exception                (SomeSQLException,
                                                       formatSomeSQLException)
import           HStream.Server.Persistence.Exception (PersistenceException)
import qualified HStream.Store                        as Store
import           HStream.Utils                        (TaskStatus,
                                                       returnErrResp,
                                                       returnStreamingResp)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server
import           ZooKeeper.Exception

-- TODO: More exception handle needs specific handling.
mkExceptionHandle :: (StatusCode -> StatusDetails -> IO (ServerResponse t a))
                  -> IO ()
                  -> IO (ServerResponse t a)
                  -> IO (ServerResponse t a)
mkExceptionHandle retFun cleanFun = flip catches [
  Handler (\(err :: SomeSQLException) ->
    retFun StatusInvalidArgument $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(_ :: Store.EXISTS) ->
    retFun StatusAlreadyExists "Stream already exists"),
  Handler (\(_ :: ObjectNotExist) ->
    retFun StatusNotFound "Object not found"),
  Handler (\(err :: Store.SomeHStoreException) -> do
    cleanFun
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err :: PersistenceException) ->
    retFun StatusAborted $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(_ :: QueryTerminatedOrNotExist) ->
    retFun StatusInvalidArgument "Query is already terminated or does not exist"),
  Handler (\(err :: StreamNotExist) ->
    retFun StatusNotFound $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(_ :: SubscriptionIdOccupied) ->
    retFun StatusInvalidArgument "Subscription ID has been occupied"),
  Handler (\(_ :: SubscriptionIdNotFound) ->
    retFun StatusNotFound "Subscription ID can not be found"),
  Handler (\(err :: IOException) -> do
    Log.fatal $ Log.buildString (displayException err)
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err :: ERRException) -> do
    retFun StatusInternal $ StatusDetails ("Mysql error " <> BS.pack (show err))),
  Handler (\(err :: ConnectorAlreadyExists) -> do
    let ConnectorAlreadyExists st = err
    retFun StatusAlreadyExists $ StatusDetails ("Connector exists with status  " <> BS.pack (show st))),
  Handler (\(err :: ConnectorRestartErr) -> do
    let ConnectorRestartErr st = err
    retFun StatusInternal $ StatusDetails ("Cannot restart a connector with status  " <> BS.pack (show st))),
  Handler (\(_ :: ConnectorNotExist) -> do
    retFun StatusNotFound "Connector not found"),
  Handler (\(err :: ZNODEEXISTS) -> do
    retFun StatusAlreadyExists $ StatusDetails ("Zookeeper exception: " <> BS.pack (show err))),
  Handler (\(err :: ZNONODE) -> do
    retFun StatusNotFound $ StatusDetails ("Zookeeper exception: " <> BS.pack (show err))),
  Handler (\(err :: ZooException) -> do
    retFun StatusInternal $ StatusDetails ("Zookeeper exception: " <> BS.pack (show err))),
  Handler (\(err :: SomeException) -> do
    retFun StatusUnknown $ StatusDetails ("UnKnown exception: " <> BS.pack (show err)))
  ]

defaultExceptionHandle :: IO (ServerResponse 'Normal a) -> IO (ServerResponse 'Normal a)
defaultExceptionHandle = mkExceptionHandle returnErrResp $ return ()

defaultExceptionHandle' :: IO () -> IO (ServerResponse 'Normal a) -> IO (ServerResponse 'Normal a)
defaultExceptionHandle' = mkExceptionHandle returnErrResp

defaultStreamExceptionHandle :: IO (ServerResponse 'ServerStreaming a)
                             -> IO (ServerResponse 'ServerStreaming a)
defaultStreamExceptionHandle = mkExceptionHandle returnStreamingResp $ return ()

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

data SubscriptionIdNotFound = SubscriptionIdNotFound
  deriving (Show)
instance Exception SubscriptionIdNotFound

data SubscriptionIdOccupied = SubscriptionIdOccupied
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
