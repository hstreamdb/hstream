{-# LANGUAGE FunctionalDependencies #-}

module HStream.Kafka.Common.TestUtils
  ( typed
  , match
  , from
  , does
  , is
  , on

  , genResource
  , withZkBasedAclAuthorizer
  , withRqliteBasedAclAuthorizer
  , withFileBasedAclAuthorizer

  , ldclient
  ) where

import           Data.Maybe
import           Data.Text                             (Text)
import qualified Data.Text                             as T
import qualified Data.UUID                             as UUID
import qualified Data.UUID.V4                          as UUID
import qualified HStream.Store.Logger                  as S
import qualified Network.HTTP.Client                   as HTTP
import           System.Environment                    (lookupEnv)
import           System.IO.Unsafe                      (unsafePerformIO)
import           Test.Hspec
import qualified Z.Data.CBytes                         as CB
import qualified Z.Data.CBytes                         as CBytes
import qualified ZooKeeper                             as ZK

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Authorizer
import           HStream.Kafka.Common.Authorizer.Class
import           HStream.Kafka.Common.Resource         hiding (match)
import           HStream.Kafka.Common.Security
import qualified HStream.Kafka.Server.MetaData         as Meta
import qualified HStream.MetaStore.Types               as Meta
import qualified HStream.Store                         as S

------------------------------------------------------------
--   Construct 'ResourcePattern':
--   > resourceName `typed` Res_TOPIC `match` Pat_LITERAL
------------------------------------------------------------
-- | For constructing a 'ResourcePattern' with 'match'.
infixl 6 `typed`
typed :: Text -> ResourceType -> PatternType -> ResourcePattern
typed = flip ResourcePattern

-- | For constructing a 'ResourcePattern' with 'typed'.
infixl 6 `match`
match :: (PatternType -> ResourcePattern)
      -> PatternType
      -> ResourcePattern
match cont pt = cont pt

------------------------------------------------------------
--   Construct 'AccessControlEntry':
--   > principal `from` host `does` AclOp_READ `is` AclPerm_ALLOW

--   Construct 'AuthorizableRequestContext':
--   > principal `from` host
------------------------------------------------------------
-- | For constructing an ACE or 'AuthorizableRequestContext'.
class From r where
  infixl 6 `from`
  from :: Principal -> Text -> r
instance From (AclOperation -> AclPermissionType -> AccessControlEntry) where
  from p n o t = AccessControlEntry (AccessControlEntryData (T.pack (show p)) n o t)
instance From AuthorizableRequestContext where
  from = flip AuthorizableRequestContext

-- | For constructing an ACE with 'from' and 'is'.
infixl 6 `does`
does :: (AclOperation -> AclPermissionType -> AccessControlEntry)
     -> AclOperation
     -> AclPermissionType -> AccessControlEntry
does cont op = cont op

-- | For constructing an ACE with 'from' and 'does'.
infixl 6 `is`
is :: (AclPermissionType -> AccessControlEntry)
   -> AclPermissionType
   -> AccessControlEntry
is cont pt = cont pt

------------------------------------------------------------
--   Construct 'AclAction':
--   > AclOp_READ `on` resource

--   Construct 'AclBinding':
--   > ace `on` resource
------------------------------------------------------------
-- | For constructing an 'AclAction' or 'AclBinding'.
class On a b r | a -> b, a -> r where
  infixl 6 `on`
  on :: a -> b -> r
instance On AclOperation ResourcePattern AclAction where
  on op res = AclAction res op True True
instance On AccessControlEntry ResourcePattern AclBinding where
  on ace res = AclBinding res ace
instance On AccessControlEntryFilter ResourcePatternFilter AclBindingFilter where
  on aceFilter resFilter = AclBindingFilter resFilter aceFilter

--------------------------------------------------------------------------------
genResource :: ResourceType -> IO ResourcePattern
genResource resType = do
  uuid <- UUID.toText <$> UUID.nextRandom
  return $ ("foo-" <> uuid) `typed` resType `match` Pat_LITERAL

withZkBasedAclAuthorizer :: ActionWith AuthorizerObject -> IO ()
withZkBasedAclAuthorizer action = do
  zkPortStr <- fromMaybe "2181" <$> lookupEnv "ZOOKEEPER_LOCAL_PORT"
  let zkAddr = "127.0.0.1" <> ":" <> CB.pack zkPortStr
  let res = ZK.zookeeperResInit zkAddr Nothing 5000 Nothing 0
  ZK.withResource res $ \zkHandle -> do
    Meta.initKafkaZkPaths zkHandle
    authorizer <- newAclAuthorizer (pure zkHandle)
    initAclAuthorizer authorizer
    action (AuthorizerObject $ Just authorizer)

withRqliteBasedAclAuthorizer :: ActionWith AuthorizerObject -> IO ()
withRqliteBasedAclAuthorizer action = do
  rqPortStr <- fromMaybe "4001" <$> lookupEnv "RQLITE_LOCAL_PORT"
  let rqAddr = "127.0.0.1" <> ":" <> T.pack rqPortStr
  m <- HTTP.newManager HTTP.defaultManagerSettings
  let rq = Meta.RHandle m rqAddr
  Meta.initKafkaRqTables rq
  authorizer <- newAclAuthorizer (pure rq)
  initAclAuthorizer authorizer
  action (AuthorizerObject $ Just authorizer)

withFileBasedAclAuthorizer :: ActionWith AuthorizerObject -> IO ()
withFileBasedAclAuthorizer action = do
  let filePath = "/tmp/hstream_metadata"
  Meta.initKafkaFileTables filePath
  authorizer <- newAclAuthorizer (pure filePath)
  initAclAuthorizer authorizer
  action (AuthorizerObject $ Just authorizer)

--------------------------------------------------------------------------------

-- Copy from: hstream-store/test/HStream/Store/SpecUtils.hs
ldclient :: S.LDClient
ldclient = unsafePerformIO $ do
  config <- fromMaybe "/data/store/logdevice.conf" <$> lookupEnv "TEST_LD_CONFIG"
  _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
  S.newLDClient $ CBytes.pack config
{-# NOINLINE ldclient #-}
