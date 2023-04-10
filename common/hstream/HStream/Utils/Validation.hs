{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils.Validation where

import           Control.Exception         (throwIO)
import           Data.Char                 (isAlphaNum, isLetter)
import qualified Data.Text                 as T

import           HStream.Exception         (invalidIdentifier)
import           Data.Maybe                (isJust)
import           Data.Word                 (Word32)
import           HStream.Exception         (InvalidObjectIdentifier (..))
import           HStream.Logger            as Log
import qualified HStream.Server.HStreamApi as API

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

-- FIXME: Currently CLI does not support
-- parsing "." or "-" in the stream name
validMarks :: String
validMarks = "-_"

validateNameAndThrow :: API.ResourceType -> T.Text -> IO ()
validateNameAndThrow rType n =
  case validateNameText n of
    Left s   -> do
      Log.warning $ "Invalid Object Identifier:" <> Log.build s
      throwIO (invalidIdentifier rType s)
    Right _ -> return ()

validateStream :: API.Stream -> Either ValidationError API.Stream
validateStream API.Stream{..} = do
  API.Stream <$> validateStreamName streamStreamName
             <*> validateReplica streamReplicationFactor
             <*> Right streamBacklogDuration
             <*> validateShardCnt streamShardCount
             <*> Right streamCreationTime

validateCreateShardReader :: API.CreateShardReaderRequest -> Either ValidationError API.CreateShardReaderRequest
validateCreateShardReader API.CreateShardReaderRequest{..} = do
  API.CreateShardReaderRequest <$> Right createShardReaderRequestStreamName
                               <*> Right createShardReaderRequestShardId
                               <*> validateShardOffset createShardReaderRequestShardOffset
                               <*> validateShardReaderId createShardReaderRequestReaderId
                               <*> Right createShardReaderRequestTimeout

validateSubscription :: API.Subscription -> Either ValidationError API.Subscription
validateSubscription API.Subscription{..} = do
  API.Subscription <$> validateSubscriptionId subscriptionSubscriptionId
                   <*> Right subscriptionStreamName
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

validateSql :: T.Text -> Either ValidationError T.Text
validateSql x = if T.length x > 0 then Right x else Left . SQLStatementValidateErr $ "Empty Sql statement."

validateChar :: Char -> Either String ()
validateChar c
  | isAlphaNum c || elem c validMarks = Right ()
  | otherwise = Left $ "Illegal character \'" ++ (c : "\' found")

validateLength :: T.Text -> Either String ()
validateLength x = if T.length x <= 255 && T.length x > 0 then Right () else Left "The length must be between 1 and 255 characters."

validateHead :: T.Text -> Either String ()
validateHead x = if isLetter (T.head x) then Right () else Left "The first character of a name must be a letter"

validateReserved :: T.Text -> Either String ()
validateReserved x
  | x `elem` reservedName = Left $ "Name \"" ++ T.unpack x ++ "\" is reserved."
  | otherwise = Right ()

reservedName :: [T.Text]
reservedName = ["zookeeper"]

validateNameText :: T.Text -> Either String T.Text
validateNameText x = validateLength x >> validateHead x
  >> T.foldr ((>>) . validateChar) (Right x) x

validateShardReaderId :: T.Text -> Either ValidationError T.Text
validateShardReaderId x = case validateNameText x of
  Right res -> Right res
  Left s    -> Left (ShardReaderIdValidateErr s)

validateStreamName :: T.Text -> Either ValidationError T.Text
validateStreamName x = case validateNameText x of
  Right res -> Right res
  Left s    -> Left (StreamNameValidateErr s)

validateReplica :: Word32 -> Either ValidationError Word32
validateReplica rep = if rep > 0 then Right rep else Left . ReplicationFactorValidateErr $ "Stream replication factor should greater than zero."

validateShardCnt :: Word32 -> Either ValidationError Word32
validateShardCnt cnt = if cnt > 0 then Right cnt else Left . ShardCountValidateErr $ "Stream replication factor should greater than zero."

validateSubscriptionId :: T.Text -> Either ValidationError T.Text
validateSubscriptionId x = case validateNameText x of
  Right res -> Right res
  Left s    -> Left (SubscriptionIdValidateErr s)

validateConnectorName :: T.Text -> Either ValidationError T.Text
validateConnectorName x = case validateNameText x of
  Right res -> Right res
  Left s    -> Left (ConnectorNameValidateErr s)

validateQueryName :: T.Text -> Either ValidationError T.Text
validateQueryName x = case validateNameText x of
  Right res -> Right res
  Left s    -> Left (QueryNameValidateErr s)

validateShardOffset :: Maybe API.ShardOffset -> Either ValidationError (Maybe API.ShardOffset)
validateShardOffset offset =
  if offsetShouldNotBeNone offset
    then Right offset
    else Left . ShardOffsetValidateErr $ "Invalid shard offset"
 where
   offsetShouldNotBeNone :: Maybe API.ShardOffset -> Bool
   offsetShouldNotBeNone (Just offset') = isJust . API.shardOffsetOffset $ offset'
   offsetShouldNotBeNone Nothing = False

