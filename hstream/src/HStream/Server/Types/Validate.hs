module HStream.Server.Types.Validate where

import           Control.Exception
import           Control.Monad
import           Data.Char                 as Char
import qualified Data.Text                 as T


import           HStream.Server.Exception
import qualified HStream.Server.HStreamApi as API


class CheckPB a where
  checkPB :: a -> IO ()

-- TODO: streamBacklogDuration, streamShardCount
instance CheckPB API.Stream where
  checkPB API.Stream{..} = do
    when (streamReplicationFactor == 0) $ throwIO (InvalidArgument "Stream replicationFactor cannot be zero")
    checkResourceName streamStreamName

-- TODO: subscriptionAckTimeoutSeconds, subscriptionMaxUnackedRecords
instance CheckPB API.Subscription where
  checkPB API.Subscription{..} = do
    checkResourceName subscriptionSubscriptionId
    checkResourceName subscriptionStreamName

-- TODO: get from struct
instance CheckPB API.Connector where
  checkPB API.Connector{..} = do
    pure ()

-- TODO: createShardReaderRequestTimeout
instance CheckPB API.CreateShardReaderRequest where
  checkPB API.CreateShardReaderRequest{..} = do
    checkResourceName createShardReaderRequestReaderId
    checkResourceName createShardReaderRequestStreamName

instance CheckPB API.StreamingFetchRequest where
  checkPB API.StreamingFetchRequest{..} = do
    checkResourceName streamingFetchRequestConsumerName
    checkResourceName streamingFetchRequestSubscriptionId

checkResourceName :: T.Text -> IO ()
checkResourceName x = when (not $ isValidateResourceName x) $ throwIO . InvalidArgument . T.unpack $
  "Resource name "
    <> T.pack (show x)
    <> " is invalid. A valid resource name consists of a letter (range from 'a' to 'z' or 'A' to 'Z') followed by zero or more letters, digits (range from 0 to 9), underscores ('_'), and dashes('-')"

isValidateResourceName :: T.Text -> Bool
isValidateResourceName name = flip (maybe False) (T.uncons name) $ \(x, xs)
  -> Char.isAlpha x
  && T.all (\c -> Char.isAlpha c || Char.isDigit c || c == '_' || c == '-' || c == '.') xs
