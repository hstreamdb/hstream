{-# LANGUAGE ExistentialQuantification #-}
module HStream.Server.Exception where

import           Control.Exception     (Exception (..), SomeException)
import           Data.Typeable         (cast)

import           HStream.SQL.Exception (SomeSQLException)
import           HStream.Store         (SomeHStoreException)

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

data FailedToGet = FailedToGet
  deriving Show

instance Exception FailedToGet where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data FailedToRemove = FailedToRemove
  deriving Show

instance Exception FailedToRemove where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data QueryNotFound = QueryNotFound
  deriving Show

instance Exception QueryNotFound where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data ConnectorNotFound = ConnectorNotFound
  deriving Show

instance Exception ConnectorNotFound where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data QueryStillRunning = QueryStillRunning
  deriving Show

instance Exception QueryStillRunning where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data ConnectorStillRunning = ConnectorStillRunning
  deriving Show

instance Exception ConnectorStillRunning where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

---------------------------------------------------------------------

data StoreException = forall e . Exception e => StoreException e

instance Show StoreException where
  show (StoreException e) = show e

instance Exception StoreException where
  toException = serverHandlerExceptionToException
  fromException = serverHandlerExceptionFromException

storeExceptionToException :: Exception e => e -> SomeException
storeExceptionToException = toException . StoreException

storeExceptionFromException :: Exception e => SomeException -> Maybe e
storeExceptionFromException x = do
  StoreException a <- fromException x
  cast a

newtype LowLevelStoreException = LowLevelStoreException SomeHStoreException
  deriving Show

instance Exception LowLevelStoreException where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

---------------------------------------------------------------

newtype FrontSQLException = FrontSQLException SomeSQLException
  deriving Show

instance Exception FrontSQLException where
  toException = serverHandlerExceptionToException
  fromException = serverHandlerExceptionFromException

----------------------------------------------------

data QueryTerminatedOrNotExist = QueryTerminatedOrNotExist
  deriving Show

instance Exception QueryTerminatedOrNotExist where
  toException = serverHandlerExceptionToException
  fromException = serverHandlerExceptionFromException
