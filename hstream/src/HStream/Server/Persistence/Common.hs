{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveAnyClass         #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
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
import           HStream.Server.HStreamApi (Subscription)
import           HStream.Server.Types      (ProducerContext,
                                            SubscriptionContext)
import           HStream.Utils             (TaskStatus (..), cBytesToText,
                                            textToCBytes)
import           Z.Data.CBytes             (CBytes)

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
  , queryHServer     :: CBytes
  } deriving (Generic, Show, FromJSON, ToJSON)

data PersistentConnector = PersistentConnector
  { connectorId          :: CBytes
  , connectorBindedSql   :: T.Text
  , connectorCreatedTime :: Int64
  , connectorStatus      :: TaskStatus
  , connectorTimeCkp     :: Int64
  , connectorHServer     :: CBytes
  } deriving (Generic, Show, FromJSON, ToJSON)

data QueryType
  = PlainQuery  RelatedStreams
  | StreamQuery RelatedStreams CBytes            -- ^ related streams and the stream it creates
  | ViewQuery   RelatedStreams CBytes ViewSchema -- ^ related streams and the view it creates
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

--------------------------------------------------------------------------------

class TaskPersistence handle where
  insertQuery        :: HasCallStack => CBytes -> T.Text -> Int64 -> QueryType -> CBytes -> handle -> IO ()
  insertConnector    :: HasCallStack => CBytes -> T.Text -> Int64 -> CBytes -> handle -> IO ()

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

data ObjRepType = SubRep | SubCtxRep | PrdCtxRep

-- | The real type of the stored object
type family RealObjType (a :: ObjRepType) where
  RealObjType 'SubRep    = Subscription
  RealObjType 'SubCtxRep = SubscriptionContext
  RealObjType 'PrdCtxRep = ProducerContext

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
