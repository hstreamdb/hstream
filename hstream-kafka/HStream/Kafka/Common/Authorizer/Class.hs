{-# LANGUAGE FunctionalDependencies #-}

module HStream.Kafka.Common.Authorizer.Class where

import           Data.Text                     (Text)

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Security
import qualified Kafka.Protocol.Message        as K

data AclAction = AclAction
  { aclActionResPat       :: ResourcePattern
  , aclActionOp           :: AclOperation
  , aclActionLogIfAllowed :: Bool
  , aclActionLogIfDenied  :: Bool
  -- , more...
  }
instance Show AclAction where
  show AclAction{..} =
    "Action(resourcePattern='" <> show aclActionResPat       <>
    "', operation='"           <> show aclActionOp           <>
    "', logIfAllowed='"        <> show aclActionLogIfAllowed <>
    "', logIfDenied='"         <> show aclActionLogIfDenied  <>
    "')"

data AuthorizationResult
  = Authz_ALLOWED
  | Authz_DENIED
  deriving (Eq, Enum, Show)

-- TODO
data AuthorizableRequestContext = AuthorizableRequestContext
  { authReqCtxHost      :: Text
  , authReqCtxPrincipal :: Principal
  -- , ...
  }

class Authorizer s where
  -- | Create new ACL bindings.
  createAcls :: AuthorizableRequestContext
             -> s
             -> [AclBinding]
             -> IO K.CreateAclsResponse

  -- | Remove matched ACL bindings.
  deleteAcls :: AuthorizableRequestContext
             -> s
             -> [AclBindingFilter]
             -> IO K.DeleteAclsResponse

  -- | Get matched ACL bindings
  getAcls :: AuthorizableRequestContext
          -> s
          -> AclBindingFilter
          -> IO [AclBinding]

  -- | Get the current number of ACLs. Return -1 if not implemented.
  aclCount :: AuthorizableRequestContext
           -> s
           -> Int

  -- | Authorize the specified actions.
  authorize :: AuthorizableRequestContext
            -> s
            -> [AclAction]
            -> IO [AuthorizationResult]
