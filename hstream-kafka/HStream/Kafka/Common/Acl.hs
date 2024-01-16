{-# LANGUAGE RecordWildCards #-}

module HStream.Kafka.Common.Acl where

import           Data.Char
import           Data.Maybe
import           Data.Text                     (Text)
import qualified Data.Text                     as T

import           HStream.Kafka.Common.Resource

data AclOperation
  = AclOp_UNKNOWN
  | AclOp_ANY
  | AclOp_ALL
  | AclOp_READ
  | AclOp_WRITE
  | AclOp_CREATE
  | AclOp_DELETE
  | AclOp_ALTER
  | AclOp_DESCRIBE
  | AclOp_CLUSTER_ACTION
  | AclOp_DESCRIBE_CONFIGS
  | AclOp_ALTER_CONFIGS
  | AclOp_IDEMPOTENT_WRITE
  | AclOp_CREATE_TOKENS
  | AclOp_DESCRIBE_TOKENS
  deriving (Eq, Ord)
instance Show AclOperation where
  show AclOp_UNKNOWN          = "Unknown"
  show AclOp_ANY              = "Any"
  show AclOp_ALL              = "All"
  show AclOp_READ             = "Read"
  show AclOp_WRITE            = "Write"
  show AclOp_CREATE           = "Create"
  show AclOp_DELETE           = "Delete"
  show AclOp_ALTER            = "Alter"
  show AclOp_DESCRIBE         = "Describe"
  show AclOp_CLUSTER_ACTION   = "ClusterAction"
  show AclOp_DESCRIBE_CONFIGS = "DescribeConfigs"
  show AclOp_ALTER_CONFIGS    = "AlterConfigs"
  show AclOp_IDEMPOTENT_WRITE = "IdempotentWrite"
  show AclOp_CREATE_TOKENS    = "CreateTokens"
  show AclOp_DESCRIBE_TOKENS  = "DescribeTokens"
instance Read AclOperation where
  readsPrec _ s = case toUpper <$> s of
    "UNKNOWN"         -> [(AclOp_UNKNOWN, "")]
    "ANY"             -> [(AclOp_ANY, "")]
    "ALL"             -> [(AclOp_ALL, "")]
    "READ"            -> [(AclOp_READ, "")]
    "WRITE"           -> [(AclOp_WRITE, "")]
    "CREATE"          -> [(AclOp_CREATE, "")]
    "DELETE"          -> [(AclOp_DELETE, "")]
    "ALTER"           -> [(AclOp_ALTER, "")]
    "DESCRIBE"        -> [(AclOp_DESCRIBE, "")]
    "CLUSTERACTION"   -> [(AclOp_CLUSTER_ACTION, "")]
    "DESCRIBECONFIGS" -> [(AclOp_DESCRIBE_CONFIGS, "")]
    "ALTERCONFIGS"    -> [(AclOp_ALTER_CONFIGS, "")]
    "IDEMPOTENTWRITE" -> [(AclOp_IDEMPOTENT_WRITE, "")]
    "CREATETOKENS"    -> [(AclOp_CREATE_TOKENS, "")]
    "DESCRIBETOKENS"  -> [(AclOp_DESCRIBE_TOKENS, "")]
    _                 -> []
-- Note: a little different from derived 'Enum' instance.
--       indexes out of range is mapped to 'AclOp_UNKNOWN'.
instance Enum AclOperation where
  toEnum n | n == 0  = AclOp_UNKNOWN
           | n == 1  = AclOp_ANY
           | n == 2  = AclOp_ALL
           | n == 3  = AclOp_READ
           | n == 4  = AclOp_WRITE
           | n == 5  = AclOp_CREATE
           | n == 6  = AclOp_DELETE
           | n == 7  = AclOp_ALTER
           | n == 8  = AclOp_DESCRIBE
           | n == 9  = AclOp_CLUSTER_ACTION
           | n == 10 = AclOp_DESCRIBE_CONFIGS
           | n == 11 = AclOp_ALTER_CONFIGS
           | n == 12 = AclOp_IDEMPOTENT_WRITE
           | n == 13 = AclOp_CREATE_TOKENS
           | n == 14 = AclOp_DESCRIBE_TOKENS
           | otherwise = AclOp_UNKNOWN
  fromEnum AclOp_UNKNOWN          = 0
  fromEnum AclOp_ANY              = 1
  fromEnum AclOp_ALL              = 2
  fromEnum AclOp_READ             = 3
  fromEnum AclOp_WRITE            = 4
  fromEnum AclOp_CREATE           = 5
  fromEnum AclOp_DELETE           = 6
  fromEnum AclOp_ALTER            = 7
  fromEnum AclOp_DESCRIBE         = 8
  fromEnum AclOp_CLUSTER_ACTION   = 9
  fromEnum AclOp_DESCRIBE_CONFIGS = 10
  fromEnum AclOp_ALTER_CONFIGS    = 11
  fromEnum AclOp_IDEMPOTENT_WRITE = 12
  fromEnum AclOp_CREATE_TOKENS    = 13
  fromEnum AclOp_DESCRIBE_TOKENS  = 14


-- [0..3]
data AclPermissionType
  = AclPerm_UNKNOWN
  | AclPerm_ANY -- used in filter
  | AclPerm_DENY
  | AclPerm_ALLOW
  deriving (Eq, Ord)
instance Show AclPermissionType where
  show AclPerm_UNKNOWN = "Unknown"
  show AclPerm_ANY     = "Any"
  show AclPerm_DENY    = "Deny"
  show AclPerm_ALLOW   = "Allow"
instance Read AclPermissionType where
  readsPrec _ s = case toUpper <$> s of
    "UNKNOWN" -> [(AclPerm_UNKNOWN, "")]
    "ANY"     -> [(AclPerm_ANY, "")]
    "DENY"    -> [(AclPerm_DENY, "")]
    "ALLOW"   -> [(AclPerm_ALLOW, "")]
    _         -> []
-- Note: a little different from derived 'Enum' instance.
--       indexes out of range is mapped to 'AclPerm_UNKNOWN'.
instance Enum AclPermissionType where
  toEnum n | n == 0 = AclPerm_UNKNOWN
           | n == 1 = AclPerm_ANY
           | n == 2 = AclPerm_DENY
           | n == 3 = AclPerm_ALLOW
           | otherwise = AclPerm_UNKNOWN
  fromEnum AclPerm_UNKNOWN = 0
  fromEnum AclPerm_ANY     = 1
  fromEnum AclPerm_DENY    = 2
  fromEnum AclPerm_ALLOW   = 3

-- | Data of an access control entry (ACE), which is a 4-tuple of principal,
--   host, operation and permission type.
--   Used in both 'AccessControlEntry' and 'AccessControlEntryFilter',
--   with slightly different field requirements.
data AccessControlEntryData = AccessControlEntryData
  { aceDataPrincipal      :: Text
  , aceDataHost           :: Text
  , aceDataOperation      :: AclOperation
  , aceDataPermissionType :: AclPermissionType
  } deriving (Eq, Ord)
instance Show AccessControlEntryData where
  show AccessControlEntryData{..} =
    "(principal="       <> s_principal                <>
    ", host="           <> s_host                     <>
    ", operation="      <> show aceDataOperation      <>
    ", permissionType=" <> show aceDataPermissionType <> ")"
    where s_principal = if T.null aceDataPrincipal then "<any>" else T.unpack aceDataPrincipal
          s_host      = if T.null aceDataHost      then "<any>" else T.unpack aceDataHost

-- | An access control entry (ACE).
--   Requirements: principal and host can not be null.
--                 operation can not be 'AclOp_ANY'.
--                 permission type can not be 'AclPerm_ANY'.
newtype AccessControlEntry = AccessControlEntry
  { aceData :: AccessControlEntryData
  } deriving (Eq, Ord)
instance Show AccessControlEntry where
  show AccessControlEntry{..} = show aceData

-- | A filter which matches access control entry(ACE)s.
--   Requirements: principal and host can both be null.
newtype AccessControlEntryFilter = AccessControlEntryFilter
  { aceFilterData :: AccessControlEntryData
  }
instance Show AccessControlEntryFilter where
  show AccessControlEntryFilter{..} = show aceFilterData

instance Matchable AccessControlEntry AccessControlEntryFilter where
  -- See org.apache.kafka.common.acl.AccessControlEntryFilter#matches
  match AccessControlEntry{..} AccessControlEntryFilter{..}
    | not (T.null (aceDataPrincipal aceFilterData)) &&
      aceDataPrincipal aceFilterData /= aceDataPrincipal aceData = False
    | not (T.null (aceDataHost aceFilterData)) &&
      aceDataHost aceFilterData /= aceDataHost aceData = False
    | aceDataOperation aceFilterData /= AclOp_ANY &&
      aceDataOperation aceFilterData /= aceDataOperation aceData = False
    | otherwise = aceDataPermissionType aceFilterData == AclPerm_ANY ||
                  aceDataPermissionType aceFilterData == aceDataPermissionType aceData
  matchAtMostOne = isNothing . indefiniteFieldInFilter
  indefiniteFieldInFilter AccessControlEntryFilter{ aceFilterData = AccessControlEntryData{..} }
    | T.null aceDataPrincipal                  = Just "Principal is NULL"
    | T.null aceDataHost                       = Just "Host is NULL"
    | aceDataOperation == AclOp_ANY            = Just "Operation is ANY"
    | aceDataOperation == AclOp_UNKNOWN        = Just "Operation is UNKNOWN"
    | aceDataPermissionType == AclPerm_ANY     = Just "Permission type is ANY"
    | aceDataPermissionType == AclPerm_UNKNOWN = Just "Permission type is UNKNOWN"
    | otherwise                                = Nothing
  toFilter = AccessControlEntryFilter . aceData
  anyFilter = AccessControlEntryFilter $ AccessControlEntryData "" "" AclOp_ANY AclPerm_ANY

-- | A binding between a resource pattern and an access control entry (ACE).
data AclBinding = AclBinding
  { aclBindingResourcePattern :: ResourcePattern
  , aclBindingACE             :: AccessControlEntry
  } deriving (Eq, Ord)
instance Show AclBinding where
  show AclBinding{..} =
    "(pattern=" <> show aclBindingResourcePattern <>
    ", entry="  <> show aclBindingACE             <> ")"

-- | A filter which can match 'AclBinding's.
data AclBindingFilter = AclBindingFilter
  { aclBindingFilterResourcePatternFilter :: ResourcePatternFilter
  , aclBindingFilterACEFilter             :: AccessControlEntryFilter
  }
instance Show AclBindingFilter where
  show AclBindingFilter{..} =
    "(patternFilter=" <> show aclBindingFilterResourcePatternFilter <>
    ", entryFilter="  <> show aclBindingFilterACEFilter             <> ")"

instance Matchable AclBinding AclBindingFilter where
  -- See org.apache.kafka.common.acl.AclBindingFilter#matches
  match AclBinding{..} AclBindingFilter{..} =
    match aclBindingResourcePattern aclBindingFilterResourcePatternFilter &&
    match aclBindingACE             aclBindingFilterACEFilter
  matchAtMostOne AclBindingFilter{..} =
    matchAtMostOne aclBindingFilterResourcePatternFilter &&
    matchAtMostOne aclBindingFilterACEFilter
  indefiniteFieldInFilter AclBindingFilter{..} =
    indefiniteFieldInFilter aclBindingFilterResourcePatternFilter <>
    indefiniteFieldInFilter aclBindingFilterACEFilter
  toFilter AclBinding{..} = AclBindingFilter
    { aclBindingFilterResourcePatternFilter = toFilter aclBindingResourcePattern
    , aclBindingFilterACEFilter             = toFilter aclBindingACE
    }
  anyFilter = AclBindingFilter anyFilter anyFilter

-- TODO: validate
-- 1. No UNKNOWN contained
-- 2. resource pattern name is not empty and does not contain '/'
validateAclBinding :: AclBinding -> Either String ()
validateAclBinding AclBinding{..}
  | T.null (resPatResourceName aclBindingResourcePattern) = Left "Resource name is empty"
  | T.any (== '/') (resPatResourceName aclBindingResourcePattern) = Left "Resource name contains '/'"
  | otherwise = Right () -- FIXME
