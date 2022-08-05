module HStream.Utils.Validation where

import           Control.Monad
import           Data.Char                 as Char
import qualified Data.Map                  as Map
import qualified Data.Text                 as T

import qualified Google.Protobuf.Struct    as PB
import qualified HStream.Server.HStreamApi as API

class CheckPB a where
  checkPB :: a -> Either String ()

-- TODO: streamBacklogDuration, streamShardCount
instance CheckPB API.Stream where
  checkPB API.Stream{..} = do
    when (streamReplicationFactor == 0) $ Left "Stream replicationFactor cannot be zero"
    checkResourceName streamStreamName

-- TODO: subscriptionAckTimeoutSeconds, subscriptionMaxUnackedRecords
instance CheckPB API.Subscription where
  checkPB API.Subscription{..} = do
    checkResourceName subscriptionSubscriptionId
    checkResourceName subscriptionStreamName

instance CheckPB API.Connector where
  checkPB API.Connector{..} = do
    let xs = do info <- connectorInfo
                name <- join $ Map.lookup "name" $ PB.structFields info
                case name of
                  PB.Value(Just (PB.ValueKindStringValue name)) -> Just $ checkResourceName name
                  _ -> Nothing
    case xs of
      Just xs -> xs
      Nothing -> Left "Invalid connector: missing field \"name\""

-- TODO: createShardReaderRequestTimeout
instance CheckPB API.CreateShardReaderRequest where
  checkPB API.CreateShardReaderRequest{..} = do
    checkResourceName createShardReaderRequestReaderId
    checkResourceName createShardReaderRequestStreamName

instance CheckPB API.StreamingFetchRequest where
  checkPB API.StreamingFetchRequest{..} = do
    checkResourceName streamingFetchRequestConsumerName
    checkResourceName streamingFetchRequestSubscriptionId

checkResourceNameWith :: (T.Text -> Either String ()) -> T.Text -> Either String ()
checkResourceNameWith pred x = do
  when (T.length x > 255) $ Left $
    "Length of resource name must be less than 256"
  when (not $ isValidateResourceName x) $ Left $
    "Resource name "
      <> show x
      <> " is invalid. A valid resource name consists of a letter (range from 'a' to 'z' or 'A' to 'Z') followed by zero or more letters, digits (range from 0 to 9), underscores ('_'), and dashes('-')"
  pred x
  where
    isValidateResourceName :: T.Text -> Bool
    isValidateResourceName name = flip (maybe False) (T.uncons name) $ \(x, xs)
      -> Char.isAlpha x
      && T.all (\c -> Char.isAlpha c || Char.isDigit c || c == '_' || c == '-' || c == '.') xs


validateReserved :: T.Text -> Either String ()
validateReserved x = do
  when (x == "zookeeper") $ Left $
    "Resource name " <> show x <> " is reserved"

checkResourceName :: T.Text -> Either String ()
checkResourceName = checkResourceNameWith validateReserved
