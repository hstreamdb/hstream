{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Validation
  ( validateStream
  , validateAppendRequest
  , validateSubscription
  , validateCreateShardReader
  , validateCreateConnector
  , validateCreateQuery
  , validateCreateQueryWithNamespace
  , validateResLookup
  ) where

import           Control.Exception          (Exception, throwIO)
import           Control.Monad              (unless, when)
import qualified Data.ByteString            as BS
import           Data.Maybe                 (isJust)
import qualified Data.Text                  as T
import           Data.Word                  (Word32)
import           GHC.Stack                  (HasCallStack)

import qualified HStream.Exception          as HE
import           HStream.Server.Core.Common (lookupResource')
import qualified HStream.Server.HStreamApi  as API
import           HStream.Server.Types
import           HStream.Utils              (ResourceType (..),
                                             validateNameAndThrow)

validateStream :: API.Stream -> IO ()
validateStream API.Stream{..} = do
  validateNameAndThrow ResStream streamStreamName >> validateReplica streamReplicationFactor
    >> validateShardCnt streamShardCount

validateAppendRequest :: API.AppendRequest -> IO ()
validateAppendRequest API.AppendRequest{..} = do
  validateAppendRequestPayload appendRequestRecords

validateCreateShardReader :: API.CreateShardReaderRequest -> IO ()
validateCreateShardReader API.CreateShardReaderRequest{..} = do
  validateNameAndThrow ResShardReader createShardReaderRequestReaderId
  >> validateNameAndThrow ResStream createShardReaderRequestStreamName
  >> validateShardOffset createShardReaderRequestShardOffset

validateSubscription :: API.Subscription -> IO ()
validateSubscription API.Subscription{..} = do
  validateNameAndThrow ResSubscription subscriptionSubscriptionId
  >> validateNameAndThrow ResStream subscriptionStreamName

validateCreateConnector :: API.CreateConnectorRequest -> IO ()
validateCreateConnector API.CreateConnectorRequest{..} = do
  validateNameAndThrow ResConnector createConnectorRequestName

validateCreateQuery :: API.CreateQueryRequest -> IO ()
validateCreateQuery API.CreateQueryRequest{..} = do
  validateNameAndThrow ResQuery createQueryRequestQueryName
  >> validateSql HE.InvalidQuerySql createQueryRequestSql

validateCreateQueryWithNamespace :: API.CreateQueryWithNamespaceRequest -> IO ()
validateCreateQueryWithNamespace API.CreateQueryWithNamespaceRequest{..} = do
  validateNameAndThrow ResQuery createQueryWithNamespaceRequestQueryName
  >> validateSql HE.InvalidQuerySql createQueryWithNamespaceRequestSql

--------------------------------------------------------------------------------------------------------------------------------

validateReplica :: Word32 -> IO ()
validateReplica rep = if rep > 0 then pure () else throwIO $ HE.InvalidReplicaFactor "Stream replication factor should greater than zero."

validateShardCnt :: Word32 -> IO ()
validateShardCnt cnt = if cnt > 0 then pure () else throwIO $ HE.InvalidShardCount "Stream replication factor should greater than zero."

validateShardOffset :: Maybe API.ShardOffset -> IO ()
validateShardOffset offset = do
  if offsetShouldNotBeNone offset
    then pure ()
    else throwIO $ HE.InvalidShardOffset "Invalid shard offset"
 where
   offsetShouldNotBeNone :: Maybe API.ShardOffset -> Bool
   offsetShouldNotBeNone (Just offset') = isJust . API.shardOffsetOffset $ offset'
   offsetShouldNotBeNone Nothing = False

validateSql :: Exception e => (String -> e) -> T.Text -> IO ()
validateSql err x = if T.length x > 0 then pure () else throwIO $ err "Empty Sql statement."

validateAppendRequestPayload :: Maybe API.BatchedRecord -> IO ()
validateAppendRequestPayload Nothing = throwIO HE.EmptyBatchedRecord
validateAppendRequestPayload (Just API.BatchedRecord{..}) = when (batchedRecordBatchSize == 0 || BS.null batchedRecordPayload) $ throwIO HE.EmptyBatchedRecord

-------------------------------------------------------------------------------

validateResLookup
  :: HasCallStack
  => ServerContext -> ResourceType -> T.Text -> String -> IO ()
validateResLookup ctx resType name errmsg = do
  API.ServerNode{..} <- lookupResource' ctx resType name
  unless (serverNodeId == serverID ctx) $ throwIO $ HE.WrongServer errmsg
