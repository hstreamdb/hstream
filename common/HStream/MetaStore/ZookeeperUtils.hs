{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.MetaStore.ZookeeperUtils
   where

--------------------------------------------------------------------------------
-- Path

import           Control.Exception    (try)
import           Control.Monad        (void)
import           Data.Aeson           (FromJSON, ToJSON)
import qualified Data.Aeson           as Aeson
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text            as T
import           Z.Data.Vector        (Bytes)
import qualified Z.Foreign            as ZF
import           ZooKeeper            (zooCreate, zooDelete, zooGet, zooSet)
import           ZooKeeper.Exception
import           ZooKeeper.Types      (DataCompletion (DataCompletion), ZHandle,
                                       pattern ZooPersistent, zooOpenAclUnsafe)

import qualified HStream.Logger       as Log
import           HStream.Utils        (textToCBytes)

createInsertZK :: (Show a,ToJSON a) => ZHandle -> T.Text -> a -> IO ()
createInsertZK zk path contents = do
  Log.debug . Log.buildString $ "create path " <> show path <> " with value " <> show contents
  void $ zooCreate zk (textToCBytes path) (Just $ encodeValueToBytes contents) zooOpenAclUnsafe ZooPersistent

encodeValueToBytes :: ToJSON a => a -> Bytes
encodeValueToBytes = ZF.fromByteString . BL.toStrict . Aeson.encode

setZkData :: (ToJSON a) => ZHandle -> T.Text -> a -> Maybe Int -> IO ()
setZkData zk path contents mv = void $ zooSet zk (textToCBytes path) (Just $ encodeValueToBytes contents) (fromIntegral <$> mv)

deleteZKPath :: ZHandle -> T.Text -> Maybe Int -> IO ()
deleteZKPath zk path mv = do
  Log.debug . Log.buildString $ "delete path " <> show path
  void $ zooDelete zk (textToCBytes path) (fromIntegral <$> mv)

decodeZNodeValue :: FromJSON a => ZHandle -> T.Text -> IO (Maybe a)
decodeZNodeValue zk path = do
  e_a <- try $ zooGet zk (textToCBytes path)
  case e_a of
    Left (_ :: ZooException) -> return Nothing
    Right a                  -> return $ decodeDataCompletion a

decodeDataCompletion :: FromJSON a => DataCompletion -> Maybe a
decodeDataCompletion (DataCompletion (Just x) _) =
  case Aeson.eitherDecode' . BL.fromStrict . ZF.toByteString $ x of
    Right a -> Just a
    Left _  -> Nothing
decodeDataCompletion (DataCompletion Nothing _) = Nothing
