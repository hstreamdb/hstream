{-# LANGUAGE ExistentialQuantification #-}
module HStream.Server.Exception where

import           Control.Exception (Exception (..), SomeException)
import           Data.Typeable     (cast)

data ServerHandlerException = forall e . Exception e => ServerHandlerException e

instance Show ServerHandlerException where
  show (ServerHandlerException e) = show e

instance Exception ServerHandlerException

serverHandlerExceptionToException :: Exception e => e -> SomeException
serverHandlerExceptionToException = toException . ServerHandlerException

serverHandlerExceptionFromException :: Exception e => SomeException -> Maybe e
serverHandlerExceptionFromException x = do
  ServerHandlerException a <- fromException x
  cast a

---------------------------------------------------------------------

data PersistenceException = forall e . Exception e => PersistenceException e

instance Show PersistenceException where
  show (PersistenceException e) = show e

instance Exception PersistenceException where
  toException = serverHandlerExceptionToException
  fromException = serverHandlerExceptionFromException

persistenceExceptionToException :: Exception e => e -> SomeException
persistenceExceptionToException = toException . PersistenceException

persistenceExceptionFromException :: Exception e => SomeException -> Maybe e
persistenceExceptionFromException x = do
  PersistenceException a <- fromException x
  cast a

---------------------------------------------------------------------

data FailedToSetStatus = FailedToSetStatus
  deriving Show

instance Exception FailedToSetStatus where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data FailedToRecordInfo = FailedToRecordInfo
  deriving Show

instance Exception FailedToRecordInfo where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException
