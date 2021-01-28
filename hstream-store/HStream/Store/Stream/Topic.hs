{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.Stream.Topic
  ( -- * Topic
    Topic
  , getTopicIDByName
  , doesTopicExists
  , createTopicSync
  , createTopicsSync
    -- ** TopicID
  , TopicID
  , topicIDInvalid
  , topicIDInvalid'
  , mkTopicID
    -- ** TopicAttributes
  , TopicAttrs (..)
  , newLogAttrs

    -- * Topic Config
  , TopicRange
  , syncTopicConfigVersion
    -- ** Topic Group
  , StreamTopicGroup
  , makeTopicGroupSync
  , getTopicGroupSync
  , removeTopicGroupSync
  , removeTopicGroupSync'
  , topicGroupGetRange
  , topicGroupGetName
  , topicGroupGetVersion
    -- ** Topic Directory
  , StreamTopicDirectory
  , makeTopicDirectorySync
  , getTopicDirectorySync
  , removeTopicDirectorySync
  , removeTopicDirectorySync'
  , topicDirectoryGetName
  , topicDirectoryGetVersion
  ) where

import           Control.Exception       (try)
import           Control.Monad           (void, when)
import           Data.Bits               (shiftL, xor)
import qualified Data.Cache              as Cache
import           Data.Map.Strict         (Map)
import qualified Data.Map.Strict         as Map
import           Data.Time.Clock.System  (SystemTime (..), getSystemTime)
import           Data.Word               (Word16, Word32, Word64)
import           Foreign.ForeignPtr      (ForeignPtr, newForeignPtr,
                                          withForeignPtr)
import           Foreign.Ptr             (nullPtr)
import           GHC.Stack               (HasCallStack, callStack)
import           System.IO.Unsafe        (unsafePerformIO)
import           System.Random           (randomRIO)
import           Z.Data.CBytes           (CBytes)
import qualified Z.Data.CBytes           as ZC
import qualified Z.Foreign               as Z

import           HStream.Internal.FFI    (StreamClient (..), TopicID (..))
import qualified HStream.Internal.FFI    as FFI
import qualified HStream.Store.Exception as E

-------------------------------------------------------------------------------

topicCache :: Cache.Cache Topic TopicID
topicCache = unsafePerformIO $ Cache.newCache Nothing
{-# NOINLINE topicCache #-}

-- TODO: assert all functions that recv TopicID as a param is a valid TopicID

-- TODO: validation
-- 1. invalid_min < topicID < invalid_max
mkTopicID :: Word64 -> TopicID
mkTopicID = TopicID

topicIDInvalid :: TopicID
topicIDInvalid = TopicID FFI.c_logid_invalid

topicIDInvalid' :: TopicID
topicIDInvalid' = TopicID FFI.c_logid_invalid2

type Topic = CBytes

-- TODO: Default instance
data TopicAttrs = TopicAttrs
  { replicationFactor :: Int
  } deriving (Show)

type LogAttrs = ForeignPtr FFI.LogDeviceLogAttributes

newLogAttrs :: TopicAttrs -> IO LogAttrs
newLogAttrs TopicAttrs{..} = do
  i <- FFI.c_new_log_attributes
  when (replicationFactor > 0) $
    FFI.c_log_attrs_set_replication_factor i (fromIntegral replicationFactor)
  newForeignPtr FFI.c_free_log_attributes_fun i

type TopicRange = (TopicID, TopicID)

createTopicsSync :: StreamClient -> Map Topic TopicAttrs -> IO ()
createTopicsSync client ts =
  mapM_ (\(k, v) -> createTopicSync client k v) (Map.toList ts)

createTopicSync :: HasCallStack
                => StreamClient
                -> Topic
                -> TopicAttrs
                -> IO ()
createTopicSync client topic attrs = go (10 :: Int)
  where
    go maxTries =
      if maxTries <= 0
         then E.throwUserStreamError "Ran out all retries, but still failed :(" callStack
         else do
           topicID <- genRandomTopicID
           result <- try $ makeTopicGroupSync client topic topicID topicID attrs True
           case result of
             Right group            -> do
               syncTopicConfigVersion client =<< (topicGroupGetVersion group)
               Cache.insert topicCache topic topicID
             Left (_ :: E.ID_CLASH) -> go (maxTries - 1)

doesTopicExists :: StreamClient -> Topic -> IO Bool
doesTopicExists client topic = do
  m_v <- Cache.lookup topicCache topic
  case m_v of
    Just _  -> return True
    Nothing -> do r <- try $ getTopicGroupSync client topic
                  case r of
                    Left (_ :: E.NOTFOUND) -> return False
                    Right _                -> return True

getTopicIDByName :: StreamClient -> Topic -> IO TopicID
getTopicIDByName client topic = do
  m_v <- Cache.lookup topicCache topic
  maybe (fmap fst $ topicGroupGetRange =<< getTopicGroupSync client topic) return m_v

-- XXX
genRandomTopicID :: IO TopicID
genRandomTopicID = do
  ts <- systemSeconds <$> getSystemTime
  r <- randomRIO (0, maxBound :: Word16)
  let tsBit = shiftL (fromIntegral ts :: Word32) 16
  return $ TopicID $ fromIntegral tsBit `xor` (fromIntegral r :: Word64)

syncTopicConfigVersion :: StreamClient -> Word64 -> IO ()
syncTopicConfigVersion (StreamClient client) version =
  withForeignPtr client $ \client' -> void $ E.throwStreamErrorIfNotOK $
    FFI.c_ld_client_sync_logsconfig_version_safe client' version

-------------------------------------------------------------------------------
-- TopicGroup

newtype StreamTopicGroup = StreamTopicGroup
  { unStreamTopicGroup :: ForeignPtr FFI.LogDeviceLogGroup }

-- | Creates a log group under a specific directory path.
--
-- Note that, even after this method returns success, it may take some time
-- for the update to propagate to all servers, so the new log group may not
-- be usable for a few seconds (appends may fail with NOTFOUND or
-- NOTINSERVERCONFIG). Same applies to all other logs config update methods,
-- e.g. setAttributes().
makeTopicGroupSync :: StreamClient
                   -> CBytes
                   -> TopicID
                   -> TopicID
                   -> TopicAttrs
                   -> Bool
                   -> IO StreamTopicGroup
makeTopicGroupSync client path (TopicID start) (TopicID end) attrs mkParent = do
  logAttrs <- newLogAttrs attrs
  withForeignPtr (unStreamClient client) $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      ZC.withCBytesUnsafe path $ \path' -> do
        (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' -> do
          void $ E.throwStreamErrorIfNotOK $
            FFI.c_ld_client_make_loggroup_sync client' path' start end attrs' mkParent group''
        StreamTopicGroup <$> newForeignPtr FFI.c_free_lodevice_loggroup_fun group'

getTopicGroupSync :: StreamClient -> CBytes -> IO StreamTopicGroup
getTopicGroupSync client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
      void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_get_loggroup_sync client' path' group''
    StreamTopicGroup <$> newForeignPtr FFI.c_free_lodevice_loggroup_fun group'

removeTopicGroupSync :: StreamClient -> CBytes -> IO ()
removeTopicGroupSync client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_remove_loggroup_sync client' path' nullPtr

-- | The same as 'removeTopicGroupSync', but return the version of the
-- logsconfig at which the topic group got removed.
removeTopicGroupSync' :: StreamClient -> CBytes -> IO Word64
removeTopicGroupSync' client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (version, _) <- Z.withPrimUnsafe 0 $ \version' ->
      E.throwStreamErrorIfNotOK $ FFI.c_ld_client_remove_loggroup_sync' client' path' version'
    return version

topicGroupGetRange :: StreamTopicGroup -> IO TopicRange
topicGroupGetRange group =
  withForeignPtr (unStreamTopicGroup group) $ \group' -> do
    (start_ret, (end_ret, _)) <- Z.withPrimUnsafe (FFI.c_logid_invalid) $ \start' -> do
      Z.withPrimUnsafe FFI.c_logid_invalid $ \end' ->
        FFI.c_ld_loggroup_get_range group' start' end'
    return (mkTopicID start_ret, mkTopicID end_ret)

topicGroupGetName :: StreamTopicGroup -> IO CBytes
topicGroupGetName group =
  withForeignPtr (unStreamTopicGroup group) $ \group' ->
    ZC.fromCString =<< FFI.c_ld_loggroup_get_name group'

topicGroupGetVersion :: StreamTopicGroup -> IO Word64
topicGroupGetVersion (StreamTopicGroup group) =
  withForeignPtr group $ FFI.c_ld_loggroup_get_version

-------------------------------------------------------------------------------
-- TopicDirectory

newtype StreamTopicDirectory = StreamTopicDirectory
  { unStreamTopicDirectory :: ForeignPtr FFI.LogDeviceLogDirectory }

makeTopicDirectorySync :: StreamClient
                       -> CBytes
                       -> TopicAttrs
                       -> Bool
                       -> IO StreamTopicDirectory
makeTopicDirectorySync client path attrs mkParent = do
  logAttrs <- newLogAttrs attrs
  withForeignPtr (unStreamClient client) $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
    ZC.withCBytesUnsafe path $ \path' -> do
      (dir', _) <- Z.withPrimUnsafe nullPtr $ \dir'' -> do
        void $ E.throwStreamErrorIfNotOK $
          FFI.c_ld_client_make_directory_sync client' path' mkParent attrs' dir''
      StreamTopicDirectory <$> newForeignPtr FFI.c_free_lodevice_logdirectory_fun dir'

getTopicDirectorySync :: StreamClient -> CBytes -> IO StreamTopicDirectory
getTopicDirectorySync (StreamClient client) path =
  withForeignPtr client $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (dir', _) <- Z.withPrimUnsafe nullPtr $ \dir'' ->
      void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_get_directory_sync client' path' dir''
    StreamTopicDirectory <$> newForeignPtr FFI.c_free_lodevice_logdirectory_fun dir'

removeTopicDirectorySync :: StreamClient -> CBytes -> Bool -> IO ()
removeTopicDirectorySync (StreamClient client) path recursive =
  withForeignPtr client $ \client' ->
  ZC.withCBytes path $ \path' -> void $ E.throwStreamErrorIfNotOK $
    FFI.c_ld_client_remove_directory_sync_safe client' path' recursive nullPtr

removeTopicDirectorySync' :: StreamClient -> CBytes -> Bool -> IO Word64
removeTopicDirectorySync' (StreamClient client) path recursive =
  withForeignPtr client $ \client' ->
  ZC.withCBytes path $ \path' -> do
    (version, _)<- Z.withPrimSafe 0 $ \version' -> void $ E.throwStreamErrorIfNotOK $
      FFI.c_ld_client_remove_directory_sync_safe client' path' recursive version'
    return version

topicDirectoryGetName :: StreamTopicDirectory -> IO CBytes
topicDirectoryGetName dir = withForeignPtr (unStreamTopicDirectory dir) $
  \dir' -> ZC.fromCString =<< FFI.c_ld_logdirectory_get_name dir'

topicDirectoryGetVersion :: StreamTopicDirectory -> IO Word64
topicDirectoryGetVersion (StreamTopicDirectory dir) =
  withForeignPtr dir $ FFI.c_ld_logdirectory_get_version
