{-# LANGUAGE CPP             #-}
{-# LANGUAGE PatternSynonyms #-}

module HStream.Server.Core.Common where

import           Control.Applicative              ((<|>))
import           Control.Concurrent
import           Control.Concurrent.STM           (readTVarIO)
import           Control.Exception                (SomeException (..), throwIO,
                                                   try)
import           Control.Monad
import qualified Data.Attoparsec.Text             as AP
import qualified Data.ByteString                  as BS
import           Data.Foldable                    (foldrM)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)

import           HStream.Common.ConsistentHashing
import           HStream.Common.Types             (fromInternalServerNodeWithKey)
import qualified HStream.Exception                as HE
import           HStream.Gossip
import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData          as P
import           HStream.Server.Types
import           HStream.SQL
#ifdef HStreamUseV2Engine
import           HStream.SQL.Codegen
#else
import           HStream.SQL.Codegen.V1
#endif
import qualified HStream.Store                    as HS
import           HStream.Utils                    (ResourceType (..),
                                                   decodeByteStringBatch,
                                                   textToCBytes)

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

-- NOTE: if batchSize is 0 or larger than maxBound of Int, then ShardRecordIds
-- will be an empty Vector
decodeRecordBatch
  :: HS.DataRecord BS.ByteString
  -> IO (HS.C_LogID, Word64, V.Vector ShardRecordId, ReceivedRecord)
decodeRecordBatch dataRecord = do
  let payload = HS.recordPayload dataRecord
      logId = HS.recordLogID dataRecord
      batchId = HS.recordLSN dataRecord
  let batch = decodeByteStringBatch payload
      batchSize = batchedRecordBatchSize batch :: Word32
  Log.debug $ "Decoding BatchedRecord size: " <> Log.build batchSize
  let shardRecordIds = V.generate (fromIntegral batchSize) (ShardRecordId batchId . fromIntegral)
      recordIds = V.generate (fromIntegral batchSize) (RecordId logId batchId . fromIntegral)
      receivedRecords = ReceivedRecord recordIds (Just batch)
  pure (logId, batchId, shardRecordIds, receivedRecords)

--------------------------------------------------------------------------------
-- Query

terminateRelatedQueries :: ServerContext -> T.Text -> IO ()
terminateRelatedQueries sc@ServerContext{..} name = do
  queries <- M.listMeta metaHandle
  let getRelatedQueries = [P.queryId query | query <- queries, name `elem` P.getQuerySources query]
  Log.debug . Log.buildString
     $ "TERMINATE: the queries related to the terminating stream " <> show name
    <> ": " <> show getRelatedQueries
  mapM_ (handleQueryTerminate sc . OneQuery) getRelatedQueries

handleQueryTerminate :: ServerContext -> TerminationSelection -> IO [T.Text]
handleQueryTerminate ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> pure ()
  M.updateMeta qid P.QueryPaused Nothing metaHandle
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
            M.updateMeta x P.QueryPaused Nothing metaHandle
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

mkAllocationKey :: ResourceType -> T.Text -> T.Text
mkAllocationKey rtype rid = T.pack (show rtype) <> "_" <> rid

parseAllocationKey :: T.Text -> Either String (ResourceType, T.Text)
parseAllocationKey = AP.parseOnly allocationKeyP

allocationKeyP :: AP.Parser (ResourceType, T.Text)
allocationKeyP = do
  rtype <- (ResStream       <$ AP.string (T.pack $ show ResStream))
       <|> (ResStream       <$ AP.string (T.pack $ show ResStream))
       <|> (ResSubscription <$ AP.string (T.pack $ show ResSubscription))
       <|> (ResShard        <$ AP.string (T.pack $ show ResShard))
       <|> (ResShardReader  <$ AP.string (T.pack $ show ResShardReader))
       <|> (ResConnector    <$ AP.string (T.pack $ show ResConnector))
       <|> (ResQuery        <$ AP.string (T.pack $ show ResQuery))
       <|> (ResView         <$ AP.string (T.pack $ show ResView))
  AP.char '_'
  rid <- AP.takeWhile (const True)
  return (rtype, rid)

lookupResource' :: ServerContext -> ResourceType -> Text -> IO ServerNode
lookupResource' sc@ServerContext{..} rtype rid = do
  let metaId = mkAllocationKey rtype rid
  -- FIXME: it will insert the results of lookup no matter the resource exists or not
  M.getMetaWithVer @P.TaskAllocation metaId metaHandle >>= \case
    Nothing -> do
      (epoch, hashRing) <- readTVarIO loadBalanceHashRing
      theNode <- getResNode hashRing rid scAdvertisedListenersKey
      try (M.insertMeta @P.TaskAllocation metaId (P.TaskAllocation epoch (serverNodeId theNode)) metaHandle) >>=
        \case
          Left (_e :: SomeException) -> lookupResource' sc rtype rid
          Right ()                   -> return theNode
    Just (P.TaskAllocation epoch nodeId, version) -> do
      serverList <- getMemberList gossipContext >>= fmap V.concat . mapM (fromInternalServerNodeWithKey scAdvertisedListenersKey)
      case find ((nodeId == ) . serverNodeId) serverList of
        Just theNode -> return theNode
        Nothing -> do
          (epoch', hashRing) <- readTVarIO loadBalanceHashRing
          if epoch' > epoch
            then do
              theNode' <- getResNode hashRing rid scAdvertisedListenersKey
              try (M.updateMeta @P.TaskAllocation metaId (P.TaskAllocation epoch' (serverNodeId theNode')) (Just version) metaHandle) >>=
                \case
                  Left (_e :: SomeException) -> lookupResource' sc rtype rid
                  Right ()                   -> return theNode'
            else do
              Log.warning "LookupResource: the server has not yet synced with the latest member list "
              throwIO $ HE.ResourceAllocationException "the server has not yet synced with the latest member list"

getResNode :: HashRing -> Text -> Maybe Text -> IO ServerNode
getResNode hashRing hashKey listenerKey = do
  let serverNode = getAllocatedNode hashRing hashKey
  theNodes <- fromInternalServerNodeWithKey listenerKey serverNode
  if V.null theNodes then throwIO $ HE.NodesNotFound "Got empty nodes"
                     else pure $ V.head theNodes

--------------------------------------------------------------------------------

listSubscriptions :: ServerContext ->  Maybe T.Text -> IO (V.Vector Subscription)
listSubscriptions ServerContext{..} sName = do
  subs <- M.listMeta metaHandle
  mapM update $ V.fromList [ sub | sub <- originSub <$> subs,
                                   case sName of
                                     Nothing -> True
                                     Just x  -> subscriptionStreamName sub == x]
 where
   update sub@Subscription{..} = do
     archived <- HS.isArchiveStreamName (textToCBytes subscriptionStreamName)
     if archived then return sub {subscriptionStreamName = "__deleted_stream__"}
                 else return sub

modifySelect :: Text -> RSelect -> RSelect
modifySelect namespace (RSelect a (RFrom t) b c d) = RSelect a (RFrom (modifyTableRef t)) b c d
  where
    modifyTableRef :: RTableRef -> RTableRef
    modifyTableRef (RTableRefSimple                   x my) = RTableRefSimple      (namespace <> x) ((namespace <>) <$> my)
    modifyTableRef (RTableRefCrossJoin          t1 t2 i) = RTableRefCrossJoin   (modifyTableRef t1) (modifyTableRef t2) i
    modifyTableRef (RTableRefNaturalJoin      t1 j t2 i) = RTableRefNaturalJoin (modifyTableRef t1) j (modifyTableRef t2) i
    modifyTableRef (RTableRefJoinOn         t1 j t2 v i) = RTableRefJoinOn      (modifyTableRef t1) j (modifyTableRef t2) v i
    modifyTableRef (RTableRefJoinUsing   t1 j t2 cols i) = RTableRefJoinUsing   (modifyTableRef t1) j (modifyTableRef t2) cols i
