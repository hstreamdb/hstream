{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module HStream.Server.Persistence (
    module HStream.Server.Persistence.Subscriptions
  , module HStream.Server.Persistence.Tasks
  , module HStream.Server.Persistence.Utils
  , module ZooKeeper.Exception
  , isViewQuery
  , isStreamQuery
  , getRelatedStreams
  , getQuerySink) where

import           Z.Data.CBytes                            (CBytes)

import           HStream.Server.Persistence.Subscriptions
import           HStream.Server.Persistence.Tasks
import           HStream.Server.Persistence.Utils
import           ZooKeeper.Exception

isViewQuery :: PersistentQuery -> Bool
isViewQuery PersistentQuery{..} =
  case queryType of
    ViewQuery{} -> True
    _           -> False

isStreamQuery :: PersistentQuery -> Bool
isStreamQuery PersistentQuery{..} =
  case queryType of
    StreamQuery{} -> True
    _             -> False

getRelatedStreams :: PersistentQuery -> RelatedStreams
getRelatedStreams PersistentQuery{..} =
  case queryType of
    (PlainQuery ss)    -> ss
    (StreamQuery ss _) -> ss
    (ViewQuery ss _ _) -> ss

getQuerySink :: PersistentQuery -> CBytes
getQuerySink PersistentQuery{..} =
  case queryType of
    PlainQuery{}      -> ""
    (StreamQuery _ s) -> s
    (ViewQuery _ s _) -> s
