{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedLists           #-}
{-# LANGUAGE OverloadedStrings         #-}
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
import           HStream.Utils                        (returnErrResp)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server

-- TODO: More exception handle needs specific handling.
defaultExceptionHandle :: IO (ServerResponse 'Normal a) -> IO (ServerResponse 'Normal a)
defaultExceptionHandle = flip catches [
  Handler (\(err :: SomeSQLException) ->
    returnErrResp StatusInternal $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(_ :: Store.EXISTS) ->
    returnErrResp StatusInternal "Stream already exists"),
  Handler (\(err :: PersistenceException) ->
    returnErrResp StatusInternal $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(_ :: QueryTerminatedOrNotExist) ->
    returnErrResp StatusInternal "Query is already terminated or does not exist"),
  Handler (\(_ :: SubscriptionIdNotFound) ->
    returnErrResp StatusInternal "Subscription ID can not be found"),
  Handler (\(err :: IOException) -> do
    returnErrResp StatusInternal $ StatusDetails (BS.pack . displayException $ err))
  ]

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

data SubscriptionIdNotFound = SubscriptionIdNotFound
  deriving (Show)
instance Exception SubscriptionIdNotFound

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist
