{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Persistence.Common
  ( ViewSchema
  , RelatedStreams
  , PersistentQuery (..)
  , PersistentConnector (..)
  , QueryType (..)
  , TaskPersistence (..)
  , SubscriptionPersistence (..)
  ) where

import           Data.Aeson    (FromJSON (..), ToJSON (..))
import           Data.Int      (Int64)
import qualified Data.Text     as T
import           GHC.Generics  (Generic)
import           GHC.Stack     (HasCallStack)
import           HStream.Utils (TaskStatus (..), cBytesToText, textToCBytes)
import           Z.Data.CBytes (CBytes)

--------------------------------------------------------------------------------

instance FromJSON CBytes where
  parseJSON v = let pText = parseJSON v in textToCBytes <$> pText

instance ToJSON CBytes where
  toJSON cb = toJSON (cBytesToText cb)

type ViewSchema     = [String]
type RelatedStreams = [CBytes]

data PersistentQuery = PersistentQuery
  { queryId          :: CBytes
  , queryBindedSql   :: T.Text
  , queryCreatedTime :: Int64
  , queryType        :: QueryType
  , queryStatus      :: TaskStatus
  , queryTimeCkp     :: Int64
  } deriving (Generic, Show, FromJSON, ToJSON)

data PersistentConnector = PersistentConnector
  { connectorId          :: CBytes
  , connectorBindedSql   :: T.Text
  , connectorCreatedTime :: Int64
  , connectorStatus      :: TaskStatus
  , connectorTimeCkp     :: Int64
  } deriving (Generic, Show, FromJSON, ToJSON)

data QueryType
  = PlainQuery  RelatedStreams
  | StreamQuery RelatedStreams CBytes            -- ^ related streams and the stream it creates
  | ViewQuery   RelatedStreams CBytes ViewSchema -- ^ related streams and the view it creates
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

--------------------------------------------------------------------------------

class TaskPersistence handle where
  insertQuery        :: HasCallStack => CBytes -> T.Text -> Int64 -> QueryType -> handle -> IO ()
  insertConnector    :: HasCallStack => CBytes -> T.Text -> Int64 -> handle -> IO ()

  setQueryStatus     :: HasCallStack => CBytes -> TaskStatus -> handle -> IO ()
  setConnectorStatus :: HasCallStack => CBytes -> TaskStatus -> handle -> IO ()

  getQueryIds        :: HasCallStack => handle -> IO [CBytes]
  getQuery           :: HasCallStack => CBytes -> handle -> IO PersistentQuery

  getQueries         :: HasCallStack => handle -> IO [PersistentQuery]
  getQueries h = getQueryIds h >>= mapM (`getQuery` h)
  getQueryStatus     :: HasCallStack => CBytes -> handle -> IO TaskStatus
  getQueryStatus qid h = queryStatus <$> getQuery qid h

  getConnectorIds    :: HasCallStack => handle -> IO [CBytes]
  getConnector       :: HasCallStack => CBytes -> handle -> IO PersistentConnector

  getConnectors      :: HasCallStack => handle -> IO [PersistentConnector]
  getConnectors h = getConnectorIds h >>= mapM (`getConnector` h)
  getConnectorStatus :: HasCallStack => CBytes -> handle -> IO TaskStatus
  getConnectorStatus cid h = connectorStatus <$> getConnector cid h

  removeQuery'       :: HasCallStack => CBytes -> handle ->  IO ()
  removeQuery        :: HasCallStack => CBytes -> handle -> IO ()

  removeConnector'   :: HasCallStack => CBytes -> handle ->  IO ()
  removeConnector    :: HasCallStack => CBytes -> handle -> IO ()

--------------------------------------------------------------------------------

class SubscriptionPersistence handle where
  -- | persist a subscription to the store
  storeSubscription :: (HasCallStack, FromJSON a, ToJSON a) => T.Text -> a -> handle -> IO ()
  -- | return the specified subscription
  getSubscription :: (HasCallStack, FromJSON a, ToJSON a) => T.Text -> handle -> IO a
  -- | check if specified subscription exists
  checkIfExist :: HasCallStack => T.Text -> handle -> IO Bool
  -- | return all subscriptions
  listSubscriptions :: (HasCallStack, FromJSON a, ToJSON a) => handle -> IO [a]
  -- | remove specified subscription
  removeSubscription :: HasCallStack => T.Text -> handle -> IO ()
  -- | remove all subscriptions
  removeAllSubscriptions :: HasCallStack => handle -> IO ()
