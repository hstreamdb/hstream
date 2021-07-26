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

import           HStream.SQL.Exception                (SomeSQLException,
                                                       formatSomeSQLException)
import           HStream.Server.Persistence.Exception (PersistenceException)
import qualified HStream.Store                        as Store
import           HStream.Utils                        (returnErrResp,
                                                       returnStreamingResp)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server

-- TODO: More exception handle needs specific handling.
mkExceptionHandle :: (StatusCode -> StatusDetails -> IO (ServerResponse t a))
                  -> IO (ServerResponse t a)
                  -> IO (ServerResponse t a)
mkExceptionHandle retFun = flip catches [
  Handler (\(err :: SomeSQLException) ->
    retFun StatusInternal $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(_ :: Store.EXISTS) ->
    retFun StatusInternal "Stream already exists"),
  Handler (\(err :: PersistenceException) ->
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(_ :: QueryTerminatedOrNotExist) ->
    retFun StatusInternal "Query is already terminated or does not exist"),
  Handler (\(_ :: SubscriptionIdNotFound) ->
    retFun StatusInternal "Subscription ID can not be found"),
  Handler (\(err :: IOException) -> do
    retFun StatusInternal $ StatusDetails (BS.pack . displayException $ err))
  ]

defaultExceptionHandle :: IO (ServerResponse 'Normal a) -> IO (ServerResponse 'Normal a)
defaultExceptionHandle = mkExceptionHandle returnErrResp

defaultStreamExceptionHandle :: IO (ServerResponse 'ServerStreaming a)
                             -> IO (ServerResponse 'ServerStreaming a)
defaultStreamExceptionHandle = mkExceptionHandle returnStreamingResp

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

data SubscriptionIdNotFound = SubscriptionIdNotFound
  deriving (Show)
instance Exception SubscriptionIdNotFound

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist

data ConnectorAlreadyExists = ConnectorAlreadyExists
  deriving (Show)
instance Exception ConnectorAlreadyExists
