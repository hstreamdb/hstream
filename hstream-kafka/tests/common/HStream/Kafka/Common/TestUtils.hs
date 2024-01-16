{-# LANGUAGE FunctionalDependencies #-}

module HStream.Kafka.Common.TestUtils
  ( typed
  , match
  , from
  , does
  , is
  , on

  , genResource
  ) where

import           Data.Text                             (Text)
import qualified Data.Text                             as T
import qualified Data.UUID                             as UUID
import qualified Data.UUID.V4                          as UUID

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.AclEntry
import           HStream.Kafka.Common.Authorizer.Class
import           HStream.Kafka.Common.Resource         hiding (match)
import           HStream.Kafka.Common.Security

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
