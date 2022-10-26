{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.MetaStore.ZookeeperUtils
   where
import           Control.Exception    (catch, try)
import           Control.Monad        (void)
import           Data.Aeson           (FromJSON, ToJSON)
import qualified Data.Aeson           as Aeson
import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text            as T
import           GHC.Stack            (HasCallStack)
import           Z.Data.CBytes        (CBytes)
import           Z.Data.Vector        (Bytes)
import qualified Z.Foreign            as ZF
import           ZooKeeper            (zooCreate, zooDelete, zooGet,
                                       zooGetChildren, zooSet)
import           ZooKeeper.Exception
import           ZooKeeper.Types      (DataCompletion (..), StringVector (..),
                                       StringsCompletion (..), ZHandle,
                                       pattern ZooPersistent, zooOpenAclUnsafe)

import qualified HStream.Logger       as Log
import           HStream.Utils        (textToCBytes)

createInsertZK :: (Show a,ToJSON a) => ZHandle -> T.Text -> a -> IO ()
createInsertZK zk path contents = do
  Log.debug . Log.buildString $ "create path " <> show path <> " with value " <> show contents
  void $ zooCreate zk (textToCBytes path) (Just $ encodeValueToBytes contents) zooOpenAclUnsafe ZooPersistent

encodeValueToBytes :: ToJSON a => a -> Bytes
encodeValueToBytes = ZF.fromByteString . BL.toStrict . Aeson.encode

encodeValueToBS :: ToJSON a => a -> BS.ByteString
encodeValueToBS = BL.toStrict . Aeson.encode

setZkData :: (ToJSON a) => ZHandle -> T.Text -> a -> Maybe Int -> IO ()
setZkData zk path contents mv = void $ zooSet zk (textToCBytes path) (Just $ encodeValueToBytes contents) (fromIntegral <$> mv)

upsertZkData :: (Show a, ToJSON a) => ZHandle -> T.Text -> a -> IO ()
upsertZkData zk path contents = do
  Log.debug . Log.buildString $ "upsert path " <> show path <> " with value " <> show contents
  catch (createInsertZK zk path contents) $
    \(_ :: ZNODEEXISTS) -> do
      Log.debug . Log.buildString $ "path exists, set the value instead"
      setZkData zk path contents Nothing

deleteZkPath :: ZHandle -> T.Text -> Maybe Int -> IO ()
deleteZkPath zk path mv = do
  Log.debug . Log.buildString $ "delete path " <> show path
  void $ zooDelete zk (textToCBytes path) (fromIntegral <$> mv)

deleteZkChildren :: ZHandle -> T.Text -> IO ()
deleteZkChildren zk path = do
  let path' = textToCBytes path
  Log.debug . Log.buildString $ "delete children under path: " <> show path
  StringsCompletion (StringVector children) <- zooGetChildren zk path'
  mapM_ (\p -> zooDelete zk (path' <> "/" <> p) Nothing) children

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

tryCreate :: HasCallStack => ZHandle -> CBytes -> IO ()
tryCreate zk path = catch (createPath zk path) $
  \(_ :: ZNODEEXISTS) -> pure ()

createPath :: HasCallStack => ZHandle -> CBytes -> IO ()
createPath zk path = do
  Log.debug . Log.buildString $ "create path " <> show path
  void $ zooCreate zk path Nothing zooOpenAclUnsafe ZooPersistent
