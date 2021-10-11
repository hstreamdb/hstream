{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}

module HStream.Server.Persistence.MemoryStore where

import qualified Data.HashMap.Strict                  as HM
import           Data.IORef                           (IORef, modifyIORef,
                                                       newIORef, readIORef)
import qualified Data.List                            as L
import           Data.Time.Clock.System
import           GHC.IO                               (throwIO, unsafePerformIO)
import           Z.Data.CBytes                        (CBytes)
import           Z.IO.Time                            (getSystemTime')

import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Exception
import           HStream.Server.Persistence.Utils
import           HStream.Utils                        (TaskStatus (..))

type PStoreMem   = (QueriesM, ConnectorsM)
type ConnectorsM = IORef (HM.HashMap CBytes PersistentConnector)
type QueriesM    = IORef (HM.HashMap CBytes PersistentQuery)

queryCollection :: QueriesM
queryCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE queryCollection #-}

connectorsCollection :: ConnectorsM
connectorsCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE connectorsCollection #-}

instance TaskPersistence PStoreMem where
  insertQuery qid qSql qTime qType qHServer (refQ, _) = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef refQ $
      HM.insert (mkQueryPath qid) $ PersistentQuery qid qSql qTime qType Created timestamp qHServer

  insertConnector cid cSql cTime cHServer (_, refC) = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef refC $
      HM.insert (mkConnectorPath cid) $ PersistentConnector cid cSql cTime Created timestamp cHServer

  setQueryStatus qid newStatus (refQ, _) = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s query = query {queryStatus = s, queryTimeCkp = timestamp}
    modifyIORef refQ $ HM.adjust (f newStatus) (mkQueryPath qid)

  setConnectorStatus qid statusQ (_, refC) = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s connector = connector {connectorStatus = s, connectorTimeCkp = timestamp}
    modifyIORef refC $ HM.adjust (f statusQ) (mkConnectorPath qid)

  getQueryIds = ifThrow FailedToGet . (L.map queryId . HM.elems <$>) . readIORef . fst
  getQuery qid (refQ, _) = ifThrow FailedToGet $ do
    hmapQ <- readIORef refQ
    case HM.lookup (mkQueryPath qid) hmapQ of
      Nothing    -> throwIO QueryNotFound
      Just query -> return query

  getConnectorIds = ifThrow FailedToGet . (L.map connectorId . HM.elems <$>) . readIORef . snd
  getConnector cid (_, refC) = ifThrow FailedToGet $ do
    hmapC <- readIORef refC
    case HM.lookup (mkConnectorPath cid) hmapC of
      Nothing -> throwIO ConnectorNotFound
      Just c  -> return c

  removeQuery qid ref@(refQ, _) = ifThrow FailedToRemove $
    getQueryStatus qid ref >>= \case
      Terminated -> modifyIORef refQ . HM.delete . mkQueryPath $ qid
      _          -> throwIO QueryStillRunning

  removeQuery' qid (refQ, _) = ifThrow FailedToRemove $
    modifyIORef refQ $ HM.delete . mkQueryPath $ qid

  removeConnector cid ref@(_, refC) = ifThrow FailedToRemove $
    getConnectorStatus cid ref >>= \st -> do
    if st `elem` [Terminated, CreationAbort, ConnectionAbort]
      then modifyIORef refC . HM.delete . mkConnectorPath $ cid
      else throwIO ConnectorStillRunning

  removeConnector' cid (_, refC) = ifThrow FailedToRemove $
    modifyIORef refC $ HM.delete . mkConnectorPath $ cid
