{-# LANGUAGE ExistentialQuantification #-}
module HStream.Server.Persistence.Exception where

import           Control.Exception (Exception (..), SomeException (..))
import           Data.Typeable     (cast)

data PersistenceException = forall e . Exception e => PersistenceException e

instance Show PersistenceException where
  show (PersistenceException e) = show e

instance Exception PersistenceException

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

data FailedToSetHServer = FailedToSetHServer
  deriving Show

instance Exception FailedToSetHServer where
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

data FailedToDecode = FailedToDecode
  deriving Show

instance Exception FailedToDecode where
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

data SubscriptionRemoved = SubscriptionRemoved
  deriving Show

instance Exception SubscriptionRemoved where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException

data UnexpectedZkEvent = UnexpectedZkEvent
  deriving Show

instance Exception UnexpectedZkEvent where
  toException   = persistenceExceptionToException
  fromException = persistenceExceptionFromException
