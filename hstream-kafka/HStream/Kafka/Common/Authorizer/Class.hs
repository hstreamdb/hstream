module HStream.Kafka.Common.Authorizer.Class where

import           Control.Exception
import           Data.Text                           (Text)

import           HStream.Kafka.Common.Acl
import qualified HStream.Kafka.Common.KafkaException as K
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Security
import qualified Kafka.Protocol.Error                as K
import qualified Kafka.Protocol.Message              as K

------------------------------------------------------------
-- Helper types
------------------------------------------------------------
data AclAction = AclAction
  { aclActionResPat       :: !ResourcePattern
  , aclActionOp           :: !AclOperation
  , aclActionLogIfAllowed :: !Bool
  , aclActionLogIfDenied  :: !Bool
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
  { authReqCtxHost      :: !Text
  , authReqCtxPrincipal :: !Principal
  -- , ...
  }

------------------------------------------------------------
-- Abstract authorizer interface
------------------------------------------------------------
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
           -> IO Int

  -- | Authorize the specified actions.
  authorize :: AuthorizableRequestContext
            -> s
            -> [AclAction]
            -> IO [AuthorizationResult]

------------------------------------------------------------
-- Existential wrapper for Authorizer
------------------------------------------------------------
data AuthorizerObject where
  AuthorizerObject :: Authorizer s => Maybe s -> AuthorizerObject

withAuthorizerObject :: AuthorizerObject
                     -> (forall s. Authorizer s => Maybe s -> a)
                     -> a
withAuthorizerObject (AuthorizerObject x) f = f x

-- NOTE: 'AuthorizerObject' can contain 'Nothing'.
--       Methods behave differently in two types on 'Nothing':
--       1. management methods ('createAcls', 'deleteAcls' and 'getAcls'): throw an exception
--       2. 'authorize' and 'aclCount': always return "true" values as if ACL not implemented
instance Authorizer AuthorizerObject where
  createAcls ctx (AuthorizerObject x) =
    maybe (\_ -> throwIO $ K.ErrorCodeException K.SECURITY_DISABLED) (createAcls ctx) x
  deleteAcls ctx (AuthorizerObject x) =
    maybe (\_ -> throwIO $ K.ErrorCodeException K.SECURITY_DISABLED) (deleteAcls ctx) x
  getAcls    ctx (AuthorizerObject x) =
    maybe (\_ -> throwIO $ K.ErrorCodeException K.SECURITY_DISABLED) (getAcls    ctx) x
  aclCount   ctx (AuthorizerObject x) =
    maybe (pure (-1)) (aclCount ctx) x
  authorize  ctx (AuthorizerObject x) =
    case x of
      Nothing -> mapM (const $ pure Authz_ALLOWED)
      Just s  -> authorize ctx s
