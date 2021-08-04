{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedLists           #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module HStream.Server.Exception where

import           Control.Exception                    (Exception (..),
                                                       Handler (Handler),
                                                       IOException, catches)
import qualified Data.ByteString.Char8                as BS
import           Database.MySQL.Base                  (ERRException)

import           HStream.SQL.Exception                (SomeSQLException,
                                                       formatSomeSQLException)
import           HStream.Server.Persistence.Exception (PersistenceException)
import qualified HStream.Store                        as Store
import           HStream.Utils                        (returnErrResp,
                                                       returnStreamingResp)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server

import           HStream.Server.Persistence           (Status)

-- TODO: More exception handle needs specific handling.
mkExceptionHandle :: (StatusCode -> StatusDetails -> IO (ServerResponse t a))
                  -> IO ()
                  -> IO (ServerResponse t a)
                  -> IO (ServerResponse t a)
mkExceptionHandle retFun cleanFun = flip catches [
  Handler (\(err :: SomeSQLException) ->
    retFun StatusInternal $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(_ :: Store.EXISTS) ->
    retFun StatusInternal "Stream already exists"),
  Handler (\(err :: Store.SomeHStoreException) -> do
    cleanFun
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err :: PersistenceException) ->
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(_ :: QueryTerminatedOrNotExist) ->
    retFun StatusInternal "Query is already terminated or does not exist"),
  Handler (\(_ :: SubscriptionIdOccupied) ->
    retFun StatusInternal "Subscription ID has been occupied"),
  Handler (\(_ :: SubscriptionIdNotFound) ->
    retFun StatusInternal "Subscription ID can not be found"),
  Handler (\(err :: IOException) -> do
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(err :: ERRException) -> do
    retFun StatusInternal $ StatusDetails ("Mysql error " <> BS.pack (show err))),
  Handler (\(err :: ConnectorAlreadyExists) -> do
    let ConnectorAlreadyExists st = err
    retFun StatusInternal $ StatusDetails ("Connector exists with status  " <> BS.pack (show st))),
  Handler (\(err :: ConnectorRestartErr) -> do
    let ConnectorRestartErr st = err
    retFun StatusInternal $ StatusDetails ("Cannot restart a connector with status  " <> BS.pack (show st))),
  Handler (\(_ :: ConnectorNotExist) -> do
    retFun StatusInternal "Connector not found")
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

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist

newtype ConnectorAlreadyExists = ConnectorAlreadyExists Status
  deriving (Show)
instance Exception ConnectorAlreadyExists

newtype ConnectorRestartErr = ConnectorRestartErr Status
  deriving (Show)
instance Exception ConnectorRestartErr

data ConnectorNotExist = ConnectorNotExist
  deriving (Show)
instance Exception ConnectorNotExist
