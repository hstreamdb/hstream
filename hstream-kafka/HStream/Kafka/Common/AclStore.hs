module HStream.Kafka.Common.AclStore where

import           Control.Monad
import qualified Data.Aeson                    as Aeson
import           Data.Functor                  ((<&>))
import           Data.IORef
import qualified Data.Map.Strict               as Map
import qualified Data.Set                      as Set
import           Data.Text                     (Text)
import qualified Data.Text                     as T

import           HStream.Kafka.Common.AclEntry
import           HStream.Kafka.Common.Resource
import qualified HStream.Utils                 as Utils

import qualified ZooKeeper                     as ZK
import qualified ZooKeeper.Types               as ZK
import           ZooKeeper.Types               (ZHandle)

-- | Storage interface for ACL data, such as zookeeper.
class AclStore a where
  -- | Path of a pattern type in the store.
  aclStorePath :: a -> PatternType -> Text

  -- | Path of a resource type in the store.
  aclStorePath' :: a -> PatternType -> ResourceType -> Text

  -- | Path of a resource pattern in the store.
  aclStorePath''  :: a -> ResourcePattern -> Text

  -- | List name of ACL stores of the given path.
  listAclStoreNames :: a -> Text -> IO [Text]

  -- | Read an ACL node from the store by the given resource pattern.
  getAclNode    :: a -> ResourcePattern -> IO AclResourceNode

  -- | Write an ACL node to the store by the given resource pattern.
  setAclNode    :: a -> ResourcePattern -> AclResourceNode -> IO ()

  -- | Delete an ACL node from the store by the given resource pattern.
  deleteAclNode :: a -> ResourcePattern -> IO ()

loadAllAcls :: AclStore a => a -> (ResourcePattern -> Acls -> IO ()) -> IO ()
loadAllAcls a aclsConsumer = do
  forM_ [Pat_LITERAL, Pat_PREFIXED] $ \pat -> do
    -- FIXME: 'read'
    resTypes <- listAclStoreNames a (aclStorePath a pat) <&> (map (read . T.unpack))
    forM_ resTypes (go pat)
  where
    go pat resType = do
      let resTypePath = aclStorePath' a pat resType
      children <- listAclStoreNames a resTypePath
      forM_ children $ \resName -> do
        let resource = ResourcePattern resType resName pat
        aclNode <- getAclNode a resource
        aclsConsumer resource (aclResNodeAcls aclNode)

------------------------------------------------------------
-- ZooKeeper specific implementation
------------------------------------------------------------
zkAclStorePath :: PatternType -> Text
zkAclStorePath pat =
  case pat of
    Pat_LITERAL  -> "/kafka-acl"
    Pat_PREFIXED -> "kafka-acl-extended"
    pat_         -> error $ "Invalid pattern type: " <> show pat_ -- FIXME: error

zkAclStorePath' :: PatternType -> ResourceType -> Text
zkAclStorePath' pat resType =
  zkAclStorePath pat <> "/" <> T.pack (show resType)

zkAclStorePath'' :: ResourcePattern -> Text
zkAclStorePath'' ResourcePattern{..} =
  zkAclStorePath' resPatPatternType resPatResourceType <> "/" <> resPatResourceName

instance AclStore ZHandle where
  aclStorePath   _ = zkAclStorePath
  aclStorePath'  _ = zkAclStorePath'
  aclStorePath'' _ = zkAclStorePath''
  listAclStoreNames zkHandle path = do
    ZK.zooGetChildren zkHandle (Utils.textToCBytes path)
      <&> ((map Utils.cBytesToText) . ZK.unStrVec . ZK.strsCompletionValues)
  getAclNode zkHandle resPat = do
    let path = zkAclStorePath'' resPat
    ZK.zooExists zkHandle (Utils.textToCBytes path) >>= \case
      Nothing -> return $ AclResourceNode defaultVersion Set.empty
      Just _  -> do
        ZK.DataCompletion{..} <- ZK.zooGet zkHandle (Utils.textToCBytes path)
        case dataCompletionValue of
          Nothing    -> return $ AclResourceNode defaultVersion Set.empty -- FIXME: or throw exception?
          Just bytes -> do
            let bl = Utils.bytesToLazyByteString bytes
            case Aeson.eitherDecode bl of
              Left err   -> error $ "Invalid ACL node format: " <> err -- FIXME: error
              Right node -> return node
  setAclNode zkHandle resPat node = do
    let path = zkAclStorePath'' resPat
    ZK.zooExists zkHandle (Utils.textToCBytes path) >>= \case
      -- FIXME: zookeeper acl
      Nothing -> void $
        ZK.zooCreate zkHandle (Utils.textToCBytes path) (Just (Utils.lazyByteStringToBytes (Aeson.encode node))) ZK.zooOpenAclUnsafe ZK.ZooPersistent
      -- FIXME: check version
      Just _  -> void $
        ZK.zooSet zkHandle (Utils.textToCBytes path) (Just (Utils.lazyByteStringToBytes (Aeson.encode node))) Nothing
  deleteAclNode zkHandle resPat = do
    let path = zkAclStorePath'' resPat
    -- FIXME: check version
    ZK.zooDelete zkHandle (Utils.textToCBytes path) Nothing

------------------------------------------------------------
-- Mock store implementation (for test only)
------------------------------------------------------------
type MockAclStore = IORef (Map.Map Text -- PatternType
                                   (Map.Map Text -- ResourceType
                                            (Map.Map Text AclResourceNode)
                                   )
                          )

newMockAclStore :: IO MockAclStore
newMockAclStore = newIORef $ Map.fromList [ ("LITERAL" , Map.empty)
                                          , ("PREFIXED", Map.empty)
                                          ]

instance AclStore MockAclStore where
  aclStorePath _ pat = T.pack (show pat)
  aclStorePath' ref pat resType = aclStorePath ref pat <> "/" <>
                                  T.pack (show resType)
  aclStorePath'' ref ResourcePattern{..} =
    aclStorePath' ref resPatPatternType resPatResourceType <> "/" <>
    resPatResourceName
  listAclStoreNames ref path = do
    store <- readIORef ref
    case T.splitOn "/" path of
      [pat] -> case Map.lookup pat store of
        Nothing -> return []
        Just x  -> return $ Map.keys x
      [pat,resType] -> case Map.lookup pat store of
        Nothing -> return []
        Just x  -> case Map.lookup resType x of
          Nothing -> return []
          Just y  -> return $ Map.keys y
      _ -> error "MockAclStore: Invalid path"
  getAclNode ref ResourcePattern{..} = do
    store <- readIORef ref
    case Map.lookup (T.pack (show resPatPatternType)) store of
      Nothing -> return $ AclResourceNode defaultVersion Set.empty
      Just x  -> case Map.lookup (T.pack (show resPatResourceType)) x of
        Nothing -> return $ AclResourceNode defaultVersion Set.empty
        Just y  -> case Map.lookup resPatResourceName y of
          Nothing -> return $ AclResourceNode defaultVersion Set.empty
          Just z  -> return z
  setAclNode ref ResourcePattern{..} node = do
    atomicModifyIORef' ref
      (\x -> let x' = case Map.lookup (T.pack (show resPatPatternType)) x of
                        Nothing -> Map.singleton (T.pack (show resPatResourceType)) (Map.singleton resPatResourceName node)
                        Just y  -> case Map.lookup (T.pack (show resPatResourceType)) y of
                          Nothing -> Map.insert (T.pack (show resPatResourceType)) (Map.singleton resPatResourceName node) y
                          Just z  -> Map.insert (T.pack (show resPatResourceType)) (Map.insert resPatResourceName node z) y
             in (Map.insert (T.pack (show resPatPatternType)) x' x, ()))
  deleteAclNode ref ResourcePattern{..} = do
    atomicModifyIORef' ref
      (\x -> let x' = case Map.lookup (T.pack (show resPatPatternType)) x of
                        Nothing -> x
                        Just y  -> case Map.lookup (T.pack (show resPatResourceType)) y of
                          Nothing -> x
                          Just z  -> let z' = Map.delete resPatResourceName z
                                         y' = Map.insert (T.pack (show resPatResourceType)) z' y
                                      in Map.insert (T.pack (show resPatPatternType)) y' x
             in (x', ()))
