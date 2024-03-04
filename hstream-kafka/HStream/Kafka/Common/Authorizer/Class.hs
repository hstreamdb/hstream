module HStream.Kafka.Common.Authorizer.Class
  ( AclAction(..)
  , AuthorizationResult(..)
  , AuthorizableRequestContext(..)
  , toAuthorizableReqCtx

  , Authorizer(..)
  , AuthorizerObject(..)

  , simpleAuthorize
  ) where

import           Control.Exception
import           Control.Monad
import           Data.Maybe
import           Data.Text                           (Text)
import qualified Data.Text                           as T

import           HStream.Kafka.Common.Acl
import qualified HStream.Kafka.Common.KafkaException as K
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Security
import qualified Kafka.Protocol.Error                as K
import qualified Kafka.Protocol.Message              as K
import qualified Kafka.Protocol.Service              as K

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

-- FIXME: is it suitable to place this function here?
toAuthorizableReqCtx :: K.RequestContext -> AuthorizableRequestContext
toAuthorizableReqCtx reqCtx =
  AuthorizableRequestContext (T.pack reqCtx.clientHost)
                             (Principal "User" (fromMaybe "" (join reqCtx.clientId)))

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

------------------------------------------------------------
-- Helper functions for using authorizers
------------------------------------------------------------
-- | The simplest way to authorize a single action.
simpleAuthorize :: Authorizer s
                => AuthorizableRequestContext
                -> s
                -> ResourceType
                -> Text
                -> AclOperation
                -> IO Bool
simpleAuthorize ctx authorizer resType resName op = do
  let resPat = ResourcePattern
             { resPatResourceType = resType
             , resPatResourceName = resName
             , resPatPatternType  = Pat_LITERAL -- FIXME: support extended?
             }
      action = AclAction
             { aclActionResPat = resPat
             , aclActionOp     = op
             , aclActionLogIfAllowed = defaultLogIfAllowed
             , aclActionLogIfDenied  = defaultLogIfDenied
             }
  authorize ctx authorizer [action] >>= \case
    [Authz_ALLOWED] -> return True
    [Authz_DENIED]  -> return False
    _               -> error "what happened?" -- FIXME: error
  where
    -- FIXME: configuable
    defaultLogIfAllowed :: Bool
    defaultLogIfAllowed = False

    -- FIXME: configuable
    defaultLogIfDenied :: Bool
    defaultLogIfDenied = True
