{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedLists           #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
module HStream.Server.Exception where

import           Control.Exception             (Exception (..),
                                                Handler (Handler),
                                                SomeException, catches)

import qualified Data.ByteString.Char8         as BS
import           HStream.SQL.Exception         (SomeSQLException,
                                                formatSomeSQLException)
import           HStream.Store                 (EXISTS)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server

-- TODO: More exception handle needs specific handling.
defaultExceptionHandle :: IO (ServerResponse 'Normal a) -> IO (ServerResponse 'Normal a)
defaultExceptionHandle = flip catches [
  Handler (\(err :: SomeSQLException) ->
    returnErrRes $ StatusDetails (BS.pack . formatSomeSQLException $ err)),
  Handler (\(_ :: EXISTS) ->
    returnErrRes "Stream already exists"),
  Handler (\(_ :: QueryTerminatedOrNotExist) ->
    returnErrRes "Query is already terminated or does not exist"),
  Handler (\(err :: SomeException) -> do
    putStrLn $ displayException err
    returnErrRes "Internal Server Error")
  ]

returnErrRes :: StatusDetails -> IO (ServerResponse 'Normal a)
returnErrRes = return . ServerNormalResponse Nothing [] StatusInternal

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving (Show)
instance Exception QueryTerminatedOrNotExist
