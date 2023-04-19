{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Validation
  ( ValidationError (..)
  , validateStream
  , validateSubscription
  , validateCreateShardReader
  , validateCreateConnector
  , validateCreateQuery
  , validateCreateQueryWithNamespace
  )
where

import qualified Data.Text                 as T

import           Data.Maybe                (isJust)
import           Data.Word                 (Word32)
import qualified HStream.Server.HStreamApi as API
import           HStream.Utils             (validateNameText)

data ValidationError = StreamNameValidateErr String
                     | ReplicationFactorValidateErr String
                     | ShardCountValidateErr String
                     | ShardOffsetValidateErr String
                     | ShardReaderIdValidateErr String
                     | SubscriptionIdValidateErr String
                     | ConnectorNameValidateErr String
                     | QueryNameValidateErr String
                     | ViewNameValidateErr String
                     | SQLStatementValidateErr String

validateStream :: API.Stream -> Either ValidationError API.Stream
validateStream API.Stream{..} = do
  API.Stream <$> validateStreamName streamStreamName
             <*> validateReplica streamReplicationFactor
             <*> Right streamBacklogDuration
             <*> validateShardCnt streamShardCount
             <*> Right streamCreationTime

validateCreateShardReader :: API.CreateShardReaderRequest -> Either ValidationError API.CreateShardReaderRequest
validateCreateShardReader API.CreateShardReaderRequest{..} = do
  API.CreateShardReaderRequest <$> validateStreamName createShardReaderRequestStreamName
                               <*> Right createShardReaderRequestShardId
                               <*> validateShardOffset createShardReaderRequestShardOffset
                               <*> validateShardReaderId createShardReaderRequestReaderId
                               <*> Right createShardReaderRequestTimeout

validateSubscription :: API.Subscription -> Either ValidationError API.Subscription
validateSubscription API.Subscription{..} = do
  API.Subscription <$> validateSubscriptionId subscriptionSubscriptionId
                   <*> validateStreamName subscriptionStreamName
                   <*> Right subscriptionAckTimeoutSeconds
                   <*> Right subscriptionMaxUnackedRecords
                   <*> Right subscriptionOffset
                   <*> Right subscriptionCreationTime

validateCreateConnector :: API.CreateConnectorRequest -> Either ValidationError API.CreateConnectorRequest
validateCreateConnector API.CreateConnectorRequest{..} = do
  API.CreateConnectorRequest <$> validateConnectorName createConnectorRequestName
                             <*> Right createConnectorRequestType
                             <*> Right createConnectorRequestTarget
                             <*> Right createConnectorRequestConfig

validateCreateQuery :: API.CreateQueryRequest -> Either ValidationError API.CreateQueryRequest
validateCreateQuery API.CreateQueryRequest{..} = do
  API.CreateQueryRequest <$> validateSql createQueryRequestSql
                         <*> validateQueryName createQueryRequestQueryName

validateCreateQueryWithNamespace :: API.CreateQueryWithNamespaceRequest-> Either ValidationError API.CreateQueryWithNamespaceRequest
validateCreateQueryWithNamespace API.CreateQueryWithNamespaceRequest{..} = do
  API.CreateQueryWithNamespaceRequest <$> validateSql createQueryWithNamespaceRequestSql
                                      <*> validateQueryName createQueryWithNamespaceRequestQueryName
                                      <*> Right createQueryWithNamespaceRequestNamespace

--------------------------------------------------------------------------------------------------------------------------------

validateIdentifier :: (String -> ValidationError) -> T.Text -> Either ValidationError T.Text
validateIdentifier err x = case validateNameText x of
  Right res -> Right res
  Left s    -> Left (err s)

validateStreamName :: T.Text -> Either ValidationError T.Text
validateStreamName = validateIdentifier StreamNameValidateErr

validateSubscriptionId :: T.Text -> Either ValidationError T.Text
validateSubscriptionId = validateIdentifier SubscriptionIdValidateErr

validateShardReaderId :: T.Text -> Either ValidationError T.Text
validateShardReaderId = validateIdentifier ShardReaderIdValidateErr

validateConnectorName :: T.Text -> Either ValidationError T.Text
validateConnectorName = validateIdentifier ConnectorNameValidateErr

validateQueryName :: T.Text -> Either ValidationError T.Text
validateQueryName = validateIdentifier QueryNameValidateErr

validateReplica :: Word32 -> Either ValidationError Word32
validateReplica rep = if rep > 0 then Right rep else Left . ReplicationFactorValidateErr $ "Stream replication factor should greater than zero."

validateShardCnt :: Word32 -> Either ValidationError Word32
validateShardCnt cnt = if cnt > 0 then Right cnt else Left . ShardCountValidateErr $ "Stream replication factor should greater than zero."

validateShardOffset :: Maybe API.ShardOffset -> Either ValidationError (Maybe API.ShardOffset)
validateShardOffset offset =
  if offsetShouldNotBeNone offset
    then Right offset
    else Left . ShardOffsetValidateErr $ "Invalid shard offset"
 where
   offsetShouldNotBeNone :: Maybe API.ShardOffset -> Bool
   offsetShouldNotBeNone (Just offset') = isJust . API.shardOffsetOffset $ offset'
   offsetShouldNotBeNone Nothing = False

validateSql :: T.Text -> Either ValidationError T.Text
validateSql x = if T.length x > 0 then Right x else Left . SQLStatementValidateErr $ "Empty Sql statement."
