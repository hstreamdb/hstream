module HStream.Kafka.Common.AclEntry where

import           Data.Aeson                    ((.:), (.=))
import qualified Data.Aeson                    as Aeson
import qualified Data.Map.Strict               as Map
import qualified Data.Set                      as Set
import           Data.Text                     (Text)
import qualified Data.Text                     as T

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Security

data AclEntry = AclEntry
  { aclEntryPrincipal      :: Principal
  , aclEntryHost           :: Text
  , aclEntryOperation      :: AclOperation
  , aclEntryPermissionType :: AclPermissionType
  } deriving (Eq, Ord)
instance Show AclEntry where
  show AclEntry{..} =
    s_principal                                                   <>
    " has "                        <> show aclEntryPermissionType <>
    " permission for operations: " <> show aclEntryOperation      <>
    " from hosts: "                <> s_host
    where s_principal = show aclEntryPrincipal
          s_host      = T.unpack aclEntryHost
instance Aeson.ToJSON AclEntry where
  toJSON AclEntry{..} =
    Aeson.object [ "host"           .= aclEntryHost
                 , "permissionType" .= show aclEntryPermissionType
                 , "operation"      .= show aclEntryOperation
                 , "principal"      .= show aclEntryPrincipal
                 ]
instance Aeson.FromJSON AclEntry where
  parseJSON (Aeson.Object v) = AclEntry
    <$> (principalFromText <$> v .: "principal")
    <*> v .: "host"
    <*> (read <$> v .: "operation")
    <*> (read <$> v .: "permissionType")
  parseJSON o = fail $ "Invalid AclEntry: " <> show o

aceToAclEntry :: AccessControlEntry -> AclEntry
aceToAclEntry AccessControlEntry{ aceData = AccessControlEntryData{..} } =
  AclEntry{..}
  where aclEntryPrincipal      = principalFromText aceDataPrincipal
        aclEntryHost           = aceDataHost
        aclEntryOperation      = aceDataOperation
        aclEntryPermissionType = aceDataPermissionType

aclEntryToAce :: AclEntry -> AccessControlEntry
aclEntryToAce AclEntry{..} =
  AccessControlEntry{ aceData = AccessControlEntryData{..} }
  where aceDataPrincipal      = T.pack (show aclEntryPrincipal)
        aceDataHost           = aclEntryHost
        aceDataOperation      = aclEntryOperation
        aceDataPermissionType = aclEntryPermissionType

type Acls = Set.Set AclEntry
type Version = Int

defaultVersion :: Version
defaultVersion = 1

data AclResourceNode = AclResourceNode
  { aclResNodeVersion :: Version
  , aclResNodeAcls    :: Acls
  } deriving (Eq, Show)
instance Aeson.ToJSON AclResourceNode where
  toJSON AclResourceNode{..} =
    Aeson.object [ "version" .= defaultVersion -- FIXME: version
                 , "acls"    .= aclResNodeAcls
                 ]
instance Aeson.FromJSON AclResourceNode where
  parseJSON (Aeson.Object v) = AclResourceNode
    <$> v .: "version"
    <*> v .: "acls"
  parseJSON o = fail $ "Invalid AclResourceNode: " <> show o

data AclCache = AclCache
  { aclCacheAcls      :: Map.Map ResourcePattern Acls
  , aclCacheResources :: Map.Map (AccessControlEntry,ResourceType,PatternType)
                                 (Set.Set Text)
  }
