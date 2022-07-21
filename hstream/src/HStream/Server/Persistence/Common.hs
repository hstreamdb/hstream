{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveAnyClass         #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE PolyKinds              #-}
{-# LANGUAGE TypeFamilies           #-}

module HStream.Server.Persistence.Common
  ( ViewSchema
  , RelatedStreams
  , PersistentQuery (..)
  , PersistentConnector (..)
  , QueryType (..)
  , TaskPersistence (..)
  , BasicObjectPersistence (..)
  , ObjRepType (..)
  ) where

import           Data.Aeson                (FromJSON (..), ToJSON (..))
import           Data.Int                  (Int64)
import           Data.Map                  (Map)
import qualified Data.Text                 as T
import           GHC.Generics              (Generic)
import           GHC.Stack                 (HasCallStack)
import           Z.Data.CBytes             (CBytes)

import           HStream.Server.HStreamApi (Subscription)
import           HStream.Server.Types      (ServerID, SubscriptionWrap)
import           HStream.Utils             (TaskStatus (..))

--------------------------------------------------------------------------------

type ViewSchema     = [String]
type RelatedStreams = [CBytes]

data PersistentQuery = PersistentQuery
  { queryId          :: CBytes
  , queryBindedSql   :: T.Text
  , queryCreatedTime :: Int64
  , queryType        :: QueryType
  , queryStatus      :: TaskStatus
  , queryTimeCkp     :: Int64
  , queryHServer     :: ServerID
  } deriving (Generic, Show, FromJSON, ToJSON)

data PersistentConnector = PersistentConnector
  { connectorId          :: CBytes
  , connectorBindedSql   :: T.Text
  , connectorCreatedTime :: Int64
  , connectorStatus      :: TaskStatus
  , connectorTimeCkp     :: Int64
  , connectorHServer     :: ServerID
  } deriving (Generic, Show, FromJSON, ToJSON)

data QueryType
  = PlainQuery  RelatedStreams
  | StreamQuery RelatedStreams CBytes            -- ^ related streams and the stream it creates
  | ViewQuery   RelatedStreams CBytes ViewSchema -- ^ related streams and the view it creates
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

--------------------------------------------------------------------------------

class TaskPersistence handle where
  insertQuery        :: HasCallStack => CBytes -> T.Text -> Int64 -> QueryType -> ServerID -> handle -> IO ()
  insertConnector    :: HasCallStack => CBytes -> T.Text -> Int64 -> ServerID -> handle -> IO ()

  setQueryStatus      :: HasCallStack => CBytes -> TaskStatus -> handle -> IO ()
  setConnectorStatus  :: HasCallStack => CBytes -> TaskStatus -> handle -> IO ()
  setQueryHServer     :: HasCallStack => CBytes -> CBytes -> handle -> IO ()
  setConnectorHServer :: HasCallStack => CBytes -> CBytes -> handle -> IO ()

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

data ObjRepType = SubRep

-- | The real type of the stored object
type family RealObjType (a :: ObjRepType) where
  RealObjType 'SubRep    = SubscriptionWrap

class (RealObjType a ~ b) => BasicObjectPersistence handle (a :: ObjRepType) b | b -> a where
  -- | persist an object to the store
  storeObject :: (HasCallStack, FromJSON b, ToJSON b)
              => T.Text -> b -> handle -> IO ()
  -- | return the specified object
  getObject :: (HasCallStack, FromJSON b, ToJSON b)
            => T.Text -> handle -> IO (Maybe b)
  -- | check if specified object exists
  checkIfExist :: HasCallStack => T.Text -> handle -> IO Bool
  -- | return all objects
  listObjects :: (HasCallStack, FromJSON b, ToJSON b)
              => handle -> IO (Map T.Text b)
  -- | remove specified object
  removeObject :: HasCallStack => T.Text -> handle -> IO ()
  -- | remove all objects
  removeAllObjects :: HasCallStack => handle -> IO ()
