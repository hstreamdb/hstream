module HStream.Server.Core.Common where

import           Control.Concurrent
import           Control.Exception           (SomeException (..), throwIO, try)
import           Control.Monad
import qualified Data.ByteString             as BS
import           Data.Foldable               (foldrM)
import qualified Data.HashMap.Strict         as HM
import qualified Data.List                   as L
import qualified Data.Map.Strict             as Map
import qualified Data.Vector                 as V
import           Data.Word                   (Word32, Word64)

import qualified Data.Text                   as T
import qualified HStream.Logger              as Log
import qualified HStream.MetaStore.Types     as M
import           HStream.Server.Exception    (ObjectNotExist (ObjectNotExist))
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData     as P
import           HStream.Server.Types
import           HStream.SQL.Codegen
import qualified HStream.Store               as HS
import           HStream.ThirdParty.Protobuf (Empty (Empty))
import           HStream.Utils               (TaskStatus (..), cBytesToText,
                                              decodeByteStringBatch)

deleteStoreStream
  :: ServerContext
  -> HS.StreamId
  -> Bool
  -> IO Empty
deleteStoreStream sc@ServerContext{..} s checkIfExist = do
  streamExists <- HS.doesStreamExist scLDClient s
  if streamExists then clean >> return Empty else ignore checkIfExist
  where
    clean = do
      terminateQueryAndRemove sc (cBytesToText $ HS.streamName s)
      terminateRelatedQueries sc (cBytesToText $ HS.streamName s)
      HS.removeStream scLDClient s
    ignore True  = return Empty
    ignore False = do
      Log.warning $ "Drop: tried to remove a nonexistent object: "
                 <> Log.buildCBytes (HS.streamName s)
      throwIO ObjectNotExist
--------------------------------------------------------------------------------

insertAckedRecordId
  :: ShardRecordId                        -- ^ recordId need to insert
  -> ShardRecordId                        -- ^ lowerBound of current window
  -> Map.Map ShardRecordId ShardRecordIdRange  -- ^ ackedRanges
  -> Map.Map Word64 Word32           -- ^ batchNumMap
  -> Maybe (Map.Map ShardRecordId ShardRecordIdRange)
insertAckedRecordId recordId lowerBound ackedRanges batchNumMap
  -- [..., {leftStartRid, leftEndRid}, recordId, {rightStartRid, rightEndRid}, ... ]
  --       | ---- leftRange ----    |            |  ---- rightRange ----    |
  --
  | not $ isValidRecordId recordId batchNumMap = Nothing
  | recordId < lowerBound = Nothing
  | Map.member recordId ackedRanges = Nothing
  | otherwise =
      let leftRange = lookupLTWithDefault recordId ackedRanges
          rightRange = lookupGTWithDefault recordId ackedRanges
          canMergeToLeft = isSuccessor recordId (endRecordId leftRange) batchNumMap
          canMergeToRight = isPrecursor recordId (startRecordId rightRange) batchNumMap
       in f leftRange rightRange canMergeToLeft canMergeToRight
  where
    f leftRange rightRange canMergeToLeft canMergeToRight
      | canMergeToLeft && canMergeToRight =
        let m1 = Map.delete (startRecordId rightRange) ackedRanges
         in Just $ Map.adjust (const leftRange {endRecordId = endRecordId rightRange}) (startRecordId leftRange) m1
      | canMergeToLeft = Just $ Map.adjust (const leftRange {endRecordId = recordId}) (startRecordId leftRange) ackedRanges
      | canMergeToRight =
        let m1 = Map.delete (startRecordId rightRange) ackedRanges
         in Just $ Map.insert recordId (rightRange {startRecordId = recordId}) m1
      | otherwise = if checkDuplicat leftRange rightRange
                      then Nothing
                      else Just $ Map.insert recordId (ShardRecordIdRange recordId recordId) ackedRanges

    checkDuplicat leftRange rightRange =
         recordId >= startRecordId leftRange && recordId <= endRecordId leftRange
      || recordId >= startRecordId rightRange && recordId <= endRecordId rightRange

getCommitRecordId
  :: Map.Map ShardRecordId ShardRecordIdRange -- ^ ackedRanges
  -> Map.Map Word64 Word32          -- ^ batchNumMap
  -> Maybe ShardRecordId
getCommitRecordId ackedRanges batchNumMap = do
  (_, ShardRecordIdRange _ maxRid@ShardRecordId{..}) <- Map.lookupMin ackedRanges
  cnt <- Map.lookup sriBatchId batchNumMap
  if sriBatchIndex == cnt - 1
     -- if maxRid is a complete batch, commit maxRid
    then Just maxRid
     -- else we check the precursor of maxRid and return it as commit point
    else do
      let lsn = sriBatchId - 1
      cnt' <- Map.lookup lsn batchNumMap
      Just $ ShardRecordId lsn (cnt' - 1)

lookupLTWithDefault :: ShardRecordId -> Map.Map ShardRecordId ShardRecordIdRange -> ShardRecordIdRange
lookupLTWithDefault recordId ranges = maybe (ShardRecordIdRange minBound minBound) snd $ Map.lookupLT recordId ranges

lookupGTWithDefault :: ShardRecordId -> Map.Map ShardRecordId ShardRecordIdRange -> ShardRecordIdRange
lookupGTWithDefault recordId ranges = maybe (ShardRecordIdRange maxBound maxBound) snd $ Map.lookupGT recordId ranges

-- is r1 the successor of r2
isSuccessor :: ShardRecordId -> ShardRecordId -> Map.Map Word64 Word32 -> Bool
isSuccessor r1 r2 batchNumMap
  | r2 == minBound = False
  | r1 <= r2 = False
  | sriBatchId r1 == sriBatchId r2 = sriBatchIndex r1 == sriBatchIndex r2 + 1
  | sriBatchId r1 > sriBatchId r2 = isLastInBatch r2 batchNumMap && (sriBatchId r1 == sriBatchId r2 + 1) && (sriBatchIndex r1 == 0)

isPrecursor :: ShardRecordId -> ShardRecordId -> Map.Map Word64 Word32 -> Bool
isPrecursor r1 r2 batchNumMap
  | r2 == maxBound = False
  | otherwise = isSuccessor r2 r1 batchNumMap

isLastInBatch :: ShardRecordId -> Map.Map Word64 Word32 -> Bool
isLastInBatch recordId batchNumMap =
  case Map.lookup (sriBatchId recordId) batchNumMap of
    Nothing  ->
      let msg = "no sriBatchId found: " <> show recordId <> ", head of batchNumMap: " <> show (Map.lookupMin batchNumMap)
       in error msg
    Just num | num == 0 -> True
             | otherwise -> sriBatchIndex recordId == num - 1

getSuccessor :: ShardRecordId -> Map.Map Word64 Word32 -> ShardRecordId
getSuccessor r@ShardRecordId{..} batchNumMap =
  if isLastInBatch r batchNumMap
  then ShardRecordId (sriBatchId + 1) 0
  else r {sriBatchIndex = sriBatchIndex + 1}

isValidRecordId :: ShardRecordId -> Map.Map Word64 Word32 -> Bool
isValidRecordId ShardRecordId{..} batchNumMap =
  case Map.lookup sriBatchId batchNumMap of
    Just maxIdx | sriBatchIndex >= maxIdx || sriBatchIndex < 0 -> False
                | otherwise -> True
    Nothing -> False

decodeRecordBatch :: HS.DataRecord BS.ByteString -> (HS.C_LogID, Word64, V.Vector ShardRecordId, ReceivedRecord)
decodeRecordBatch dataRecord = (logId, batchId, shardRecordIds, receivedRecords)
  where
    payload = HS.recordPayload dataRecord
    logId = HS.recordLogID dataRecord
    batchId = HS.recordLSN dataRecord
    batch@BatchedRecord{..} = decodeByteStringBatch payload
    shardRecordIds = V.map (ShardRecordId batchId) (V.fromList [0..batchedRecordBatchSize - 1])
    recordIds = V.map (RecordId logId batchId) (V.fromList [0..batchedRecordBatchSize - 1])
    receivedRecords = ReceivedRecord recordIds (Just batch)

--------------------------------------------------------------------------------
-- Query

terminateQueryAndRemove :: ServerContext -> T.Text -> IO ()
terminateQueryAndRemove sc@ServerContext{..} objectId = do
  queries <- M.listMeta zkHandle
  let queryExists = L.find (\query -> P.getQuerySink query == objectId) queries
  case queryExists of
    Just query -> do
      Log.debug . Log.buildString
         $ "TERMINATE: found query " <> show (P.queryType query)
        <> " with query id " <> show (P.queryId query)
        <> " writes to the stream being dropped " <> show objectId
      void $ handleQueryTerminate sc (OneQuery $ P.queryId query)
      M.deleteMeta @P.PersistentQuery (P.queryId query) Nothing zkHandle
      Log.debug . Log.buildString
         $ "TERMINATE: query " <> show (P.queryType query)
        <> " has been removed"
    Nothing    -> do
      Log.debug . Log.buildString
        $ "TERMINATE: found no query writes to the stream being dropped " <> show objectId

terminateRelatedQueries :: ServerContext -> T.Text -> IO ()
terminateRelatedQueries sc@ServerContext{..} name = do
  queries <- M.listMeta zkHandle
  let getRelatedQueries = [P.queryId query | query <- queries, name `elem` P.getRelatedStreams query]
  Log.debug . Log.buildString
     $ "TERMINATE: the queries related to the terminating stream " <> show name
    <> ": " <> show getRelatedQueries
  mapM_ (handleQueryTerminate sc . OneQuery) getRelatedQueries

handleQueryTerminate :: ServerContext -> TerminationSelection -> IO [T.Text]
handleQueryTerminate ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> pure ()
  P.setQueryStatus qid Terminated zkHandle
  void $ swapMVar runningQueries (HM.delete qid hmapQ)
  Log.debug . Log.buildString $ "TERMINATE: terminated query: " <> show qid
  return [qid]
handleQueryTerminate sc@ServerContext{..} AllQueries = do
  hmapQ <- readMVar runningQueries
  handleQueryTerminate sc (ManyQueries $ HM.keys hmapQ)
handleQueryTerminate ServerContext{..} (ManyQueries qids) = do
  hmapQ <- readMVar runningQueries
  qids' <- foldrM (action hmapQ) [] qids
  Log.debug . Log.buildString $ "TERMINATE: terminated queries: " <> show qids'
  return qids'
  where
    action hm x terminatedQids = do
      result <- try $ do
        case HM.lookup x hm of
          Just tid -> do
            killThread tid
            P.setQueryStatus x Terminated zkHandle
            void $ swapMVar runningQueries (HM.delete x hm)
          _        ->
            Log.debug $ "query id " <> Log.buildString' x <> " not found"
      case result of
        Left (e ::SomeException) -> do
          Log.warning . Log.buildString
            $ "TERMINATE: unable to terminate query: " <> show x
           <> "because of " <> show e
          return terminatedQids
        Right _                  -> return (x:terminatedQids)
