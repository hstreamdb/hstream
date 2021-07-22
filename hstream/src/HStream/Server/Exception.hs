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
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server

-- TODO: More exception handle needs specific handling.
defaultExceptionHandle :: IO (ServerResponse 'Normal a) -> IO (ServerResponse 'Normal a)
defaultExceptionHandle = flip catches [
  Handler (\(err :: SomeSQLException) ->
    returnErrRes $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(_ :: Store.EXISTS) ->
    returnErrRes "Stream already exists"),
  Handler (\(err :: PersistenceException) ->
    returnErrRes $ StatusDetails (BS.pack . displayException $ err)),
  Handler (\(_ :: QueryTerminatedOrNotExist) ->
    returnErrRes "Query is already terminated or does not exist"),
  Handler (\(_ :: SubscriptionIdNotFound) ->
    returnErrRes "Subscription ID can not be found"),
  Handler (\(err :: IOException) -> do
    returnErrRes $ StatusDetails (BS.pack . displayException $ err))
  ]

-- TODO: use HStream.Utils.RPC.returnErrResp instead
returnErrRes :: Monad m => StatusDetails -> m (ServerResponse 'Normal a)
returnErrRes = return . ServerNormalResponse Nothing [] StatusInternal

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist

data SubscriptionIdNotFound = SubscriptionIdNotFound
  deriving (Show)
instance Exception SubscriptionIdNotFound

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist
