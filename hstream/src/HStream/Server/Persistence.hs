{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module HStream.Server.Persistence (
    module HStream.Server.Persistence.Subscriptions
  , module HStream.Server.Persistence.Tasks
  , module HStream.Server.Persistence.Utils
  , module HStream.Server.Persistence.Nodes
  , isViewQuery
  , isStreamQuery
  , getRelatedStreams
  , getQuerySink) where

import           Z.Data.CBytes                            (CBytes)

import           HStream.Server.Persistence.Nodes
import           HStream.Server.Persistence.Subscriptions
import           HStream.Server.Persistence.Tasks
import           HStream.Server.Persistence.Utils

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

--------------------------------------------------------------------------------

-- type PStoreMem   = (QueriesM, ConnectorsM)
-- type ConnectorsM = IORef (HM.HashMap CBytes PersistentConnector)
-- type QueriesM    = IORef (HM.HashMap CBytes PersistentQuery)

-- queryCollection :: QueriesM
-- queryCollection = unsafePerformIO $ newIORef HM.empty
-- {-# NOINLINE queryCollection #-}

-- connectorsCollection :: ConnectorsM
-- connectorsCollection = unsafePerformIO $ newIORef HM.empty
-- {-# NOINLINE connectorsCollection #-}

-- instance Persistence PStoreMem where
--   insertQuery qid qSql qTime qType (refQ, _) = ifThrow FailedToRecordInfo $ do
--     MkSystemTime timestamp _ <- getSystemTime'
--     modifyIORef refQ $
--       HM.insert (mkQueryPath qid) $ PersistentQuery qid (ZT.pack . T.unpack $ qSql) qTime qType Created timestamp

--   insertConnector cid cSql cTime (_, refC) = ifThrow FailedToRecordInfo $ do
--     MkSystemTime timestamp _ <- getSystemTime'
--     modifyIORef refC $
--       HM.insert (mkConnectorPath cid) $ PersistentConnector cid (ZT.pack . T.unpack $ cSql) cTime Created timestamp

--   setQueryStatus qid newStatus (refQ, _) = ifThrow FailedToSetStatus $ do
--     MkSystemTime timestamp _ <- getSystemTime'
--     let f s query = query {queryStatus = s, queryTimeCkp = timestamp}
--     modifyIORef refQ $ HM.adjust (f newStatus) (mkQueryPath qid)

--   setConnectorStatus qid statusQ (_, refC) = ifThrow FailedToSetStatus $ do
--     MkSystemTime timestamp _ <- getSystemTime'
--     let f s connector = connector {connectorStatus = s, connectorTimeCkp = timestamp}
--     modifyIORef refC $ HM.adjust (f statusQ) (mkConnectorPath qid)

--   getQueryIds = ifThrow FailedToGet . (L.map queryId . HM.elems <$>) . readIORef . fst
--   getQuery qid (refQ, _) = ifThrow FailedToGet $ do
--     hmapQ <- readIORef refQ
--     case HM.lookup (mkQueryPath qid) hmapQ of
--       Nothing    -> throwIO QueryNotFound
--       Just query -> return query

--   getConnectorIds = ifThrow FailedToGet . (L.map connectorId . HM.elems <$>) . readIORef . snd
--   getConnector cid (_, refC) = ifThrow FailedToGet $ do
--     hmapC <- readIORef refC
--     case HM.lookup (mkConnectorPath cid) hmapC of
--       Nothing -> throwIO ConnectorNotFound
--       Just c  -> return c

--   removeQuery qid ref@(refQ, _) = ifThrow FailedToRemove $
--     getQueryStatus qid ref >>= \case
--       Terminated -> modifyIORef refQ . HM.delete . mkQueryPath $ qid
--       _          -> throwIO QueryStillRunning

--   removeQuery' qid (refQ, _) = ifThrow FailedToRemove $
--     modifyIORef refQ $ HM.delete . mkQueryPath $ qid

--   removeConnector cid ref@(_, refC) = ifThrow FailedToRemove $
--     getConnectorStatus cid ref >>= \st -> do
--     if st `elem` [Terminated, CreationAbort, ConnectionAbort]
--       then modifyIORef refC . HM.delete . mkConnectorPath $ cid
--       else throwIO ConnectorStillRunning

--   removeConnector' cid (_, refC) = ifThrow FailedToRemove $
--     modifyIORef refC $ HM.delete . mkConnectorPath $ cid

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
