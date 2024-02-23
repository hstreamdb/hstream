{-# LANGUAGE UndecidableInstances #-}

module HStream.Kafka.Common.Authorizer where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.Either
import qualified Data.Foldable                         as Foldable
import           Data.Functor                          ((<&>))
import           Data.IORef
import qualified Data.List                             as L
import qualified Data.Map.Strict                       as Map
import           Data.Maybe
import qualified Data.Set                              as Set
import           Data.Text                             (Text)
import qualified Data.Text                             as T
import qualified Data.Vector                           as V
import           GHC.Utils.Misc                        ((<&&>), (<||>))

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.AclEntry
import           HStream.Kafka.Common.AclStore
import           HStream.Kafka.Common.Authorizer.Class
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Security
import qualified HStream.Logger                        as Log
import qualified HStream.MetaStore.Types               as Meta
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Error                  as K
import qualified Kafka.Protocol.Message                as K

------------------------------------------------------------
-- ACL authorizer with an abstract metadata store
------------------------------------------------------------
-- | An authorizer, with a in-memory cache, a presistent store and a lock.
--   The lock ensures that only one thread can update the cache and the store
--   at the same time.
data AclAuthorizer a = AclAuthorizer
  { authorizerCache     :: IORef AclCache
  , authorizerMetaStore :: a
  , authorizerLock      :: MVar ()
  }

newAclAuthorizer :: IO a -> IO (AclAuthorizer a)
newAclAuthorizer newStore = do
  let cache = AclCache Map.empty Map.empty
  AclAuthorizer <$> newIORef cache <*> newStore <*> newMVar ()

initAclAuthorizer :: ( Meta.MetaStore AclResourceNode a
                     , Meta.HasPath AclResourceNode a
                     )
                  => AclAuthorizer a
                  -> IO ()
initAclAuthorizer authorizer =
  loadAllAcls (authorizerMetaStore authorizer) $ \res acls ->
    atomicModifyIORef' (authorizerCache authorizer)
                       (\x -> (updateCache x res acls, ()))

-- | Synchronize the cache with the (central) metastore periodically.
--   This function runs forever. So it should be run in a separate thread.
--   This is an alternative method to metastore change notification.
syncAclAuthorizer :: ( Meta.MetaStore AclResourceNode a
                     , Meta.HasPath AclResourceNode a
                     )
                  => AclAuthorizer a
                  -> IO ()
syncAclAuthorizer authorizer = forever $ do
  loadAllAcls (authorizerMetaStore authorizer) $ \res acls ->
    atomicModifyIORef' (authorizerCache authorizer)
                       (\x -> (updateCache x res acls, ()))
  threadDelay (5 * 1000 * 1000) -- FIXME: configuable sync interval

------------------------------------------------------------
-- Class instance
------------------------------------------------------------
instance (Meta.MetaType AclResourceNode a) => Authorizer (AclAuthorizer a) where
  createAcls = aclCreateAcls
  deleteAcls = aclDeleteAcls
  getAcls    = aclGetAcls
  aclCount   = aclAclCount
  authorize  = aclAuthorize

------------------------------------------------------------
-- Authorizer implementation
------------------------------------------------------------

-- FIXME: Does this function behave the same as Kafka?
--        e.g. List or Set?
-- | Get matching ACLs in cache for the given resource.
--   This is a pure function.
matchingAcls :: AclCache -> ResourceType -> Text -> Acls
matchingAcls AclCache{..} resType resName =
  Set.unions [prefixed, wildcard, literal]
  where
    wildcard :: Acls
    wildcard = let resPat = ResourcePattern resType wildcardResourceName Pat_LITERAL
                in fromMaybe Set.empty (Map.lookup resPat aclCacheAcls)
    literal :: Acls
    literal = let resPat = ResourcePattern resType resName Pat_LITERAL
               in fromMaybe Set.empty (Map.lookup resPat aclCacheAcls)
    prefixed :: Acls
    prefixed = Set.unions . Map.elems $
               Map.filterWithKey (\ResourcePattern{..} _ ->
                                    resPatResourceType == resType &&
                                    resName `T.isPrefixOf` resPatResourceName
                                 ) aclCacheAcls

-- | Check if a set of ACLs contains a matching ACL for the given filter.
--   Log it if there is a matching ACL.
--   See kafka.security.authorizer.AclAuthorizer#matchingAclExists.
doesMatchingAclExist :: AclOperation
                     -> ResourcePattern
                     -> Principal
                     -> Text
                     -> AclPermissionType
                     -> Acls
                     -> IO Bool
doesMatchingAclExist op resPat principal host permType acls = do
  case Foldable.find (\AclEntry{..} ->
               aclEntryPermissionType == permType &&
               (aclEntryPrincipal == principal || aclEntryPrincipal == wildcardPrincipal) &&
               (op == aclEntryOperation || aclEntryOperation == AclOp_ALL) &&
               (aclEntryHost == host || aclEntryHost == wildcardHost)
            ) acls of
    Nothing  -> return False
    Just acl -> do
      Log.debug $ "operation = "     <> Log.buildString' op       <>
                  " on resource = "  <> Log.buildString' resPat   <>
                  " from host = "    <> Log.buildString' host     <>
                  " is "             <> Log.buildString' permType <>
                  " based on acl = " <> Log.buildString' acl
      return True

-- | Authorize an ACL action based on the request context and the given ACL cache.
authorizeAction :: AuthorizableRequestContext
                -> AclAuthorizer a
                -> AclAction
                -> IO AuthorizationResult
authorizeAction reqCtx authorizer action@AclAction{..} = do
  cache <- readIORef (authorizerCache authorizer)
  case resPatPatternType aclActionResPat of
    Pat_LITERAL -> do
      let matchedAcls = matchingAcls cache (resPatResourceType aclActionResPat)
                                           (resPatResourceName aclActionResPat)
      -- NOTE: we check:
      --       1. if the session principal is a super user
      --       2. if there is no matching ACL but the system allow empty ACL
      --       3. if there is no DENY ACL and there is at least one ALLOW ACLs
      isAuthorized <- (isSuperUser (authReqCtxPrincipal reqCtx)) <||>
        (isEmptyAclAndAuthorized matchedAcls <||>
                        ((not <$> doesDenyAclExist matchedAcls) <&&>
                          (doesAllowAclExist matchedAcls)
                        )
                      )
      logAuditMessage reqCtx action isAuthorized
      if isAuthorized
        then return Authz_ALLOWED
        else return Authz_DENIED
    _ -> error $ "Only literal resource patterns are supported for authorization. Got: " <> show (resPatPatternType aclActionResPat)
  where
    -- | Check if the given principal is a super user.
    isSuperUser :: Principal -> IO Bool
    isSuperUser principal =
      case Set.member principal superUsers of
        True  -> do
          Log.debug $ "principal = " <> Log.buildString' principal <>
                      " is a super user, allowing operation without checking acls."
          return True
        False -> return False
    -- | Check if there is no matching ACL but the system allow empty ACL.
    isEmptyAclAndAuthorized :: Acls -> IO Bool
    isEmptyAclAndAuthorized acls =
      if Set.null acls then do
        Log.debug $ "No acl found for resource " <> Log.buildString' aclActionResPat <>
                    ", authorized = "            <> Log.buildString' allowIfNoAclFound
        return allowIfNoAclFound
      else return False

    -- | Check if there is a matching DENY ACL for the given filter.
    doesDenyAclExist :: Acls -> IO Bool
    doesDenyAclExist acls =
      doesMatchingAclExist aclActionOp aclActionResPat (authReqCtxPrincipal reqCtx) (authReqCtxHost reqCtx) AclPerm_DENY acls

    -- | Check if there is a matching ALLOW ACL for the given filter.
    --   NOTE: `DESCRIBE` is implied by `READ`, `WRITE`, `DELETE` and `ALTER`.
    --         See kafka.security.authorizer.AclAuthorizer#allowAclExists and
    --         org.apache.kafka.common.acl.AclOperation.
    doesAllowAclExist :: Acls -> IO Bool
    doesAllowAclExist acls = do
      let canAllowOps =
            case aclActionOp of
              AclOp_DESCRIBE         -> [AclOp_DESCRIBE, AclOp_READ, AclOp_WRITE, AclOp_DELETE, AclOp_ALTER]
              AclOp_DESCRIBE_CONFIGS -> [AclOp_DESCRIBE_CONFIGS, AclOp_ALTER_CONFIGS]
              _                      -> [aclActionOp]
      -- short circuit
      foldM (\acc op ->
               if acc
               then return True
               else doesMatchingAclExist op aclActionResPat (authReqCtxPrincipal reqCtx) (authReqCtxHost reqCtx) AclPerm_ALLOW acls
            ) False canAllowOps

-- | Authorize a list of ACL actions based on the request context and the given ACL cache.
aclAuthorize :: AuthorizableRequestContext
             -> AclAuthorizer a
             -> [AclAction]
             -> IO [AuthorizationResult]
aclAuthorize reqCtx authorizer actions =
  forM actions (authorizeAction reqCtx authorizer)

-- | Get ACL bindings (ACL entry with resource) in cache matching the given filter.
aclGetAcls :: AuthorizableRequestContext
           -> AclAuthorizer a
           -> AclBindingFilter
           -> IO [AclBinding]
aclGetAcls _ AclAuthorizer{..} aclFilter = do
  cache <- readIORef authorizerCache
  return $ Map.foldrWithKey' f [] (aclCacheAcls cache)
  where
    f resPat acls acc =
      let g aclEntry acc' =
            let thisAclBinding = AclBinding resPat (aclEntryToAce aclEntry)
             in if match thisAclBinding aclFilter
                then acc' ++ [thisAclBinding]
                else acc'
       in Set.foldr' g acc acls

-- | Create ACLs for the given bindings.
--   It updates both the cache and the store.
aclCreateAcls :: (Meta.MetaType AclResourceNode a)
              => AuthorizableRequestContext
              -> AclAuthorizer a
              -> [AclBinding]
              -> IO K.CreateAclsResponse
aclCreateAcls _ authorizer bindings = withMVar (authorizerLock authorizer) $ \_ -> do
  let bindingsWithIdx = L.zip [0..] bindings
  (lefts_, rights_) <- partitionEithers <$> mapM validateEachBinding bindingsWithIdx
  let errorResults = Map.fromList lefts_
      resourcesToUpdate = L.foldl' (\acc (res,x) -> Map.insertWith (++) res [x] acc) Map.empty rights_
  addResults <-
    -- FIXME: recheck kafka's behavior, always take the latest result for each binding?
    (forM (Map.toList resourcesToUpdate) addAclsForEachRes) <&> (Map.unions . (L.map Map.fromList))
  let results = Map.union errorResults addResults
  -- FIXME: throttle time
  return $ K.CreateAclsResponse 0 (K.KaArray . Just . V.fromList . Map.elems $ results)
  where
    validateEachBinding :: (Int, AclBinding)
                        -> IO (Either (Int, K.AclCreationResult)
                                      (ResourcePattern, (Int, AclBinding))
                              )
    validateEachBinding (i, b@AclBinding{..}) = do
      case supportExtenedAcl of
        False -> do
          -- FIXME: ERROR CODE
          let result = K.AclCreationResult K.NONE (Just "Adding ACLs on prefixed resource patterns requires version xxx or higher")
          return $ Left (i, result)
        True  -> case validateAclBinding b of
          -- FIXME: ERROR CODE
          Left s  -> return $ Left  (i, K.AclCreationResult K.NONE (Just . T.pack $ s))
          Right _ -> return $ Right (aclBindingResourcePattern, (i, b))

    addAclsForEachRes :: (ResourcePattern, [(Int, AclBinding)]) -> IO [(Int, K.AclCreationResult)]
    addAclsForEachRes (res, bs) = do
      results_e <- try $ updateResourceAcls authorizer res $ \curAcls ->
        let aclsToAdd = Set.fromList ((aceToAclEntry . aclBindingACE . snd) <$> bs)
            newAcls = Set.union curAcls aclsToAdd
            results = L.map (\(i,_) -> (i, K.AclCreationResult K.NONE mempty)) bs
         in (newAcls, results)
      case results_e of
        -- FIXME: ERROR CODE
        Left (e :: SomeException) -> return $ L.map (\(i,_) -> (i, K.AclCreationResult K.NONE (Just $ "Failed to update ACLs" <> (T.pack (show e))))) bs
        Right x                   -> return x

-- | Delete ACls for the given filters.
--   It updates both the cache and the store.
aclDeleteAcls :: (Meta.MetaType AclResourceNode a)
              => AuthorizableRequestContext
              -> AclAuthorizer a
              -> [AclBindingFilter]
              -> IO K.DeleteAclsResponse
aclDeleteAcls _ authorizer filters = withMVar (authorizerLock authorizer) $ \_ -> do
  AclCache{..} <- readIORef (authorizerCache authorizer)
  let filtersWithIdx = L.zip [0..] filters
  let possibleResources = Map.keys aclCacheAcls <>
                          (concatMap ( Set.toList
                                     . resourcePatternFromFilter
                                     . aclBindingFilterResourcePatternFilter
                                     ) (filter matchAtMostOne filters)
                          )
      resourcesToUpdate =
        Map.fromList $
        L.filter (\(_,fs) -> not (L.null fs)) $
        L.map (\res ->
                  let matchedFilters = L.filter (\(_,x) -> match res (aclBindingFilterResourcePatternFilter x)) filtersWithIdx
                   in (res, matchedFilters)
              ) possibleResources
  -- Note: 'const' for always taking the first match (of each ACL)
  removedBindingsToFilter <-
    (mapM deleteAclsForRes (Map.toList resourcesToUpdate)) <&> (Map.unionsWith const)
  let removeResult = groupByValue removedBindingsToFilter
  let filterResults = L.map newFilterResult (Map.toList removeResult)
  -- FIXME: throttle time
  return $ K.DeleteAclsResponse 0 (K.KaArray . Just . V.fromList $ filterResults)
  where
    -- For each resource, we update its ACLs and return the removed bindings.
    -- WARNING & FIXME: Difference from Kafka:
    --          Kafka returns exception of each matched ACL (equal to the exception
    --          of the whole update operation). However, we just return empty list
    --          which ignores the exceptions of each matched ACL.
    deleteAclsForRes :: (ResourcePattern, [(Int, AclBindingFilter)])
                     -> IO (Map.Map AclBinding Int)
    deleteAclsForRes (res,matchedFilters) = do
      removedBindings_e <-
        try $ updateResourceAcls authorizer res $ \curAcls ->
          let bindingsToRemove =
                let maps = catMaybes (L.map matchOneAcl (Set.toList curAcls))
                    -- Note: 'const' for always taking the first match
                 in Map.unionsWith const maps
              aclsToRemove = Set.filter (isJust . matchOneAcl) curAcls
              newAcls = Set.difference curAcls aclsToRemove
           in (newAcls, bindingsToRemove)
      case removedBindings_e of
        Left (_ :: SomeException) -> return Map.empty
        Right x                   -> return x
      where
        -- For each ACE of certain resource, there can be zero or many filters
        -- that match it. This function find the FIRST filter which can match it
        -- or return Nothing if no matching.
        matchOneAcl :: AclEntry -> Maybe (Map.Map AclBinding Int)
        matchOneAcl entry = let ace = aclEntryToAce entry in
          case L.find (\(_,f) -> match ace (aclBindingFilterACEFilter f)) matchedFilters of
            Nothing      -> Nothing
            Just (idx,_) ->
              let aclBinding = AclBinding res ace
               in Just (Map.singleton aclBinding idx)

    newFilterResult :: (Int, [AclBinding]) -> K.DeleteAclsFilterResult
    -- FIXME: L.(!!)
    -- Note: 'filters' here is the argument of 'deleteAcls' function
    newFilterResult (_, bs) =
      let matchingAcls_ =
            L.map (\AclBinding{..} ->
                      K.DeleteAclsMatchingAcl K.NONE
                                              mempty
                                              (fromIntegral (fromEnum (resPatResourceType aclBindingResourcePattern)))
                                              (resPatResourceName aclBindingResourcePattern)
                                              (aceDataPrincipal (aceData aclBindingACE))
                                              (aceDataHost (aceData aclBindingACE))
                                              (fromIntegral (fromEnum (aceDataOperation (aceData aclBindingACE))))
                                              (fromIntegral (fromEnum (aceDataPermissionType (aceData aclBindingACE))))
                  ) bs
       in K.DeleteAclsFilterResult K.NONE mempty (K.KaArray . Just . V.fromList $ matchingAcls_)

-- FIXME: Make sure only one thread can run this function at the same time.
--        Currently, we make it by ensuring only one thread can invoke
--        this function.
-- | Update the ACLs of certain resource pattern using the given transform
--   function, and return the extra value of the function.
--   It updates both the cache and the store.
--   It will retry for some times.
--   This function may throw exceptions.
updateResourceAcls :: (Meta.MetaType AclResourceNode s)
                   => AclAuthorizer s
                   -> ResourcePattern
                   -> (Acls -> (Acls, a))
                   -> IO a
updateResourceAcls authorizer resPat f = do
  cache <- readIORef (authorizerCache authorizer)
  curAcls <- case Map.lookup resPat (aclCacheAcls cache) of
    -- Note: Load from store if not found in cache because other thread may
    --       have updated the cache.
    --       (Won't happen now because of one global cache and lock)
    Nothing   ->
      Meta.getMeta (resourcePatternToMetadataKey resPat)
                   (authorizerMetaStore authorizer) <&>
        (maybe Set.empty aclResNodeAcls)
    Just acls -> return acls
  (newAcls, a) <- go curAcls (0 :: Int)
  when (curAcls /= newAcls) $ do
    -- update cache
    atomicModifyIORef' (authorizerCache authorizer)
                       (\x -> (updateCache x resPat newAcls ,()))
  return a
  where
    go oldAcls retries
      | retries <= 5 = do -- FIXME: max retries
          let (newAcls, a) = f oldAcls
          try $ if Set.null newAcls then do
            Log.debug $ "Deleting path for " <> Log.buildString' resPat <> " because it had no ACLs remaining"
            -- delete from store
            Meta.deleteMeta @AclResourceNode
                            (resourcePatternToMetadataKey resPat)
                            Nothing
                            (authorizerMetaStore authorizer)
            return (newAcls, a)
            else do
            -- update store
            let newNode = AclResourceNode defaultVersion newAcls -- FIXME: version
            Meta.upsertMeta (resourcePatternToMetadataKey resPat)
                            newNode
                            (authorizerMetaStore authorizer)
            return (newAcls, a)
          >>= \case
            -- FIXME: catch all exceptions?
            Left (e :: SomeException) -> do
              Log.warning $ "Failed to update ACLs for " <> Log.buildString' resPat <>
                            ". Reading data and retrying update." <>
                            " error: " <> Log.buildString' e
              threadDelay (50 * 1000) -- FIXME: retry interval
              go oldAcls (retries + 1)
            Right acls_ -> return acls_
      | otherwise = do
          Log.fatal $ "Failed to update ACLs for "    <> Log.buildString' resPat  <>
                      " after retrying a maximum of " <> Log.buildString' retries <>
                      " times"
          error "update ACLs failed" -- FIXME: throw proper exception

-- | Set ACLs for certain resource pattern to the given ones in cache.
--   This is a pure function.
updateCache :: AclCache -> ResourcePattern -> Acls -> AclCache
updateCache AclCache{..} resPat@ResourcePattern{..} acls =
  let curAces = maybe Set.empty (Set.map aclEntryToAce) (Map.lookup resPat aclCacheAcls)
      newAces = Set.map aclEntryToAce acls
      acesToAdd    = Set.difference newAces curAces
      acesToRemove = Set.difference curAces newAces
   in let cacheResAfterAdd =
            Set.foldr' (\ace acc ->
              let key = (ace,resPatResourceType,resPatPatternType)
               in Map.insertWith Set.union key (Set.singleton resPatResourceName) acc
                       ) aclCacheResources acesToAdd
          cacheResAfterRemove =
            Set.foldr' (\ace acc ->
              let key = (ace,resPatResourceType,resPatPatternType)
               in Map.update (\x -> if x == Set.singleton resPatResourceName then Nothing else Just (Set.delete resPatResourceName x)) key acc
                       ) cacheResAfterAdd acesToRemove
       in let newAcls = if Set.null acls
                        then Map.delete resPat aclCacheAcls
                        else Map.insert resPat acls aclCacheAcls
           in AclCache newAcls cacheResAfterRemove

-- | Get the current number of ACLs. Return -1 if not implemented.
-- TODO: implement this
aclAclCount :: AuthorizableRequestContext
            -> AclAuthorizer a
            -> IO Int
aclAclCount _ _ = pure (-1)

------------------------------------------------------------
-- Helper functions
------------------------------------------------------------
groupByValue :: Ord v => Map.Map k v -> Map.Map v [k]
groupByValue m = Map.foldrWithKey' f Map.empty m
  where
    f k v acc = Map.insertWith (++) v [k] acc

supportExtenedAcl :: Bool
supportExtenedAcl = True

superUsers :: Set.Set Principal
superUsers = Set.empty

allowIfNoAclFound :: Bool
allowIfNoAclFound = False

-- | Log an authorization action result.
--   Note: `allowed` actions use level `debug` if configured while
--         `denied` use `info`.
--   See kafka.security.Authorizer.AclAuthorizer$logAuditMessage.
logAuditMessage :: AuthorizableRequestContext -> AclAction -> Bool -> IO ()
logAuditMessage AuthorizableRequestContext{..} AclAction{..} isAuthorized = do
  let msg = "Principal = "    <> show authReqCtxPrincipal                       <>
            " is "            <> (if isAuthorized then "Allowed" else "Denied") <>
            " Operation = "   <> show aclActionOp                               <>
            " from host = "   <> show authReqCtxHost                            <>
            " on resource = " <> show aclActionResPat
{-
-- TODO: apiKey and resource ref count
            " for request = " <> apiKey <>
            " with resouceRefCount = " <> show refCount
-}
  case isAuthorized of
    True  -> case aclActionLogIfAllowed of
               True  -> Log.debug . Log.buildString $ msg
               False -> Log.trace . Log.buildString $ msg
    False -> case aclActionLogIfDenied of
               True  -> Log.info  . Log.buildString $ msg
               False -> Log.trace . Log.buildString $ msg

----
aceToAclDescription :: AccessControlEntry -> K.AclDescription
aceToAclDescription (AccessControlEntry AccessControlEntryData{..}) =
  K.AclDescription
  { principal = aceDataPrincipal
  , host = aceDataHost
  , operation = fromIntegral (fromEnum aceDataOperation)
  , permissionType = fromIntegral (fromEnum aceDataPermissionType)
  }

aclBindingsToDescribeAclsResource :: [AclBinding] -> K.DescribeAclsResource
aclBindingsToDescribeAclsResource xs =
  K.DescribeAclsResource
  { resourceType = fromIntegral . fromEnum . resPatResourceType . aclBindingResourcePattern $ head xs -- FIXME: L.head
  , resourceName = resPatResourceName . aclBindingResourcePattern $ head xs -- FIXME: L.head
  , acls = K.KaArray (Just (V.fromList (aceToAclDescription . aclBindingACE <$> xs)))
  }
