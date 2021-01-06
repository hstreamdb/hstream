module HStream.Store.Topic
  ( TopicID
  , topicIDInvalid
  , topicIDInvalid'
  , mkTopicID

    -- * Topic Config
    -- ** Topic attributes
  , TopicAttributes
  , newTopicAttributes
  , setTopicReplicationFactor
  , setTopicReplicationFactor'
    -- ** Topic Group
  , StreamTopicGroup
  , makeTopicGroupSync
  , getTopicGroupSync
  , removeTopicGroupSync
  , removeTopicGroupSync'
  , topicGroupGetRange
  , topicGroupGetName
    -- ** Topic Directory
  , StreamTopicDirectory
  , makeTopicDirectory
  , topicDirectoryGetName
  ) where

import           Control.Monad           (void)
import           Data.Word               (Word64)
import           Foreign.ForeignPtr      (ForeignPtr, newForeignPtr,
                                          withForeignPtr)
import           Foreign.Ptr             (nullPtr)
import           Z.Data.CBytes           (CBytes)
import qualified Z.Data.CBytes           as ZC
import qualified Z.Foreign               as Z

import           HStream.Internal.FFI    (StreamClient (..), TopicID (..))
import qualified HStream.Internal.FFI    as FFI
import qualified HStream.Store.Exception as E

-- TODO: assert all functions that recv TopicID as a param is a valid TopicID

-- TODO: validation
-- 1. invalid_min < topicID < invalid_max
mkTopicID :: Word64 -> TopicID
mkTopicID = TopicID

topicIDInvalid :: TopicID
topicIDInvalid = TopicID FFI.c_logid_invalid

topicIDInvalid' :: TopicID
topicIDInvalid' = TopicID FFI.c_logid_invalid2

newtype TopicAttributes = TopicAttributes
  { unTopicAttributes :: ForeignPtr FFI.LogDeviceLogAttributes }

newTopicAttributes :: IO TopicAttributes
newTopicAttributes = do
  i <- FFI.c_new_log_attributes
  TopicAttributes <$> newForeignPtr FFI.c_free_log_attributes_fun i

setTopicReplicationFactor :: TopicAttributes -> Int -> IO ()
setTopicReplicationFactor attrs val =
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
    FFI.c_log_attrs_set_replication_factor attrs' (fromIntegral val)

setTopicReplicationFactor' :: TopicAttributes -> Int -> IO TopicAttributes
setTopicReplicationFactor' attrs val =
  setTopicReplicationFactor attrs val >> return attrs

-------------------------------------------------------------------------------

newtype StreamTopicGroup = StreamTopicGroup
  { unStreamTopicGroup :: ForeignPtr FFI.LogDeviceLogGroup }

newtype StreamTopicDirectory = StreamTopicDirectory
  { unStreamTopicDirectory :: ForeignPtr FFI.LogDeviceLogDirectory }

makeTopicDirectory :: StreamClient
                   -> CBytes
                   -> TopicAttributes
                   -> Bool
                   -> IO StreamTopicDirectory
makeTopicDirectory client path attrs mkParent =
  withForeignPtr (unStreamClient client) $ \client' ->
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (dir', _) <- Z.withPrimUnsafe nullPtr $ \dir'' -> do
      void $ E.throwStreamErrorIfNotOK $
        FFI.c_ld_client_make_directory_sync client' path' mkParent attrs' dir''
    StreamTopicDirectory <$> newForeignPtr FFI.c_free_lodevice_logdirectory_fun dir'

topicDirectoryGetName :: StreamTopicDirectory -> IO CBytes
topicDirectoryGetName dir = withForeignPtr (unStreamTopicDirectory dir) $
  ZC.fromCString . FFI.c_ld_logdirectory_get_name

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
                   -> TopicAttributes
                   -> Bool
                   -> IO StreamTopicGroup
makeTopicGroupSync client path (TopicID start) (TopicID end) attrs mkParent =
  withForeignPtr (unStreamClient client) $ \client' ->
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
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

topicGroupGetRange :: StreamTopicGroup -> IO (TopicID, TopicID)
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
