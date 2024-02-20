module HStream.Kafka.Server.Handler.Security
  ( handleSaslHandshake
  , handleSaslHandshakeAfterAuth
  , handleSaslAuthenticate

  , handleDescribeAcls
  , handleCreateAcls
  , handleDeleteAcls
  ) where

import qualified Data.Vector                           as V

import           HStream.Kafka.Server.Security.SASL    (serverSupportedMechanismNames)
import           HStream.Kafka.Server.Types            (ServerContext (..))
import qualified HStream.Logger                        as Log
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Error                  as K
import qualified Kafka.Protocol.Message                as K
import qualified Kafka.Protocol.Service                as K

import           Control.Monad
import           Data.Function                         (on)
import qualified Data.List                             as L
import           Data.Maybe
import qualified Data.Text                             as T
import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Authorizer
import           HStream.Kafka.Common.Authorizer.Class
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Security
-------------------------------------------------------------------------------

handleSaslHandshake :: ServerContext -> K.RequestContext -> K.SaslHandshakeRequest -> IO K.SaslHandshakeResponse
handleSaslHandshake _ _ K.SaslHandshakeRequest{..} = do
  -- isLibSupported <- runSASL (serverSupports reqMechanism)
  -- FIXME: check if lib supports the mechanism
  if mechanism `elem` serverSupportedMechanismNames then do -- FIXME: case insensitive?
    Log.debug $ "SASL: client requests " <> Log.buildString' mechanism
    return $ K.SaslHandshakeResponse K.NONE (K.KaArray $ Just (V.singleton mechanism))
    else do
    Log.warning $ "SASL: client requests " <> Log.buildString' mechanism <> ", but I do not support it..."
    return $ K.SaslHandshakeResponse K.UNSUPPORTED_SASL_MECHANISM (K.KaArray $ Just (V.fromList serverSupportedMechanismNames))

handleSaslHandshakeAfterAuth
  :: ServerContext
  -> K.RequestContext
  -> K.SaslHandshakeRequest
  -> IO K.SaslHandshakeResponse
handleSaslHandshakeAfterAuth _ _ _ = do
  Log.warning $ "SASL: client requests handshake after successful authentication."
  return $ K.SaslHandshakeResponse K.ILLEGAL_SASL_STATE (K.KaArray $ Just (V.fromList serverSupportedMechanismNames))

handleSaslAuthenticate :: ServerContext -> K.RequestContext -> K.SaslAuthenticateRequest -> IO K.SaslAuthenticateResponse
handleSaslAuthenticate _ _ _ = do
  Log.warning $ "SASL: client requests authenticate after successful authentication."
  return $ K.SaslAuthenticateResponse K.ILLEGAL_SASL_STATE
                                      (Just "SaslAuthenticate request received after successful authentication")
                                      mempty

-------------------------------------------------------------------------------

toAuthorizableReqCtx :: K.RequestContext -> AuthorizableRequestContext
toAuthorizableReqCtx reqCtx =
  AuthorizableRequestContext (T.pack reqCtx.clientHost)
                             (Principal "User" (fromMaybe "" (join reqCtx.clientId)))

handleDescribeAcls :: ServerContext
                   -> K.RequestContext
                   -> K.DescribeAclsRequest
                   -> IO K.DescribeAclsResponse
handleDescribeAcls ctx reqCtx req = do
  let aclBindingFilter =
        AclBindingFilter
          (ResourcePatternFilter (toEnum . fromIntegral $ req.resourceTypeFilter)
                                 (fromMaybe "" req.resourceNameFilter)
                                 Pat_LITERAL)
          (AccessControlEntryFilter
            (AccessControlEntryData (fromMaybe "" req.principalFilter)
                                    (fromMaybe "" req.hostFilter)
                                    (toEnum . fromIntegral $ req.operation)
                                    (toEnum . fromIntegral $ req.permissionType)))
  let authCtx = toAuthorizableReqCtx reqCtx
  aclBindings <- (withAuthorizerObject ctx.authorizer (getAcls authCtx)) aclBindingFilter
  let xss = L.groupBy ((==) `on` aclBindingResourcePattern) aclBindings
  let ress = K.KaArray (Just (V.fromList (aclBindingsToDescribeAclsResource <$> xss)))
  return K.DescribeAclsResponse
           { throttleTimeMs = 0
           , errorCode = K.NONE
           , errorMessage = Just ""
           , resources = ress
           }


----
aclCreationToAclBinding :: K.AclCreation -> AclBinding
aclCreationToAclBinding x =
  AclBinding (ResourcePattern (toEnum . fromIntegral $ x.resourceType)
                              x.resourceName
                              Pat_LITERAL)
             (AccessControlEntry
               (AccessControlEntryData x.principal
                                       x.host
                                       (toEnum . fromIntegral $ x.operation)
                                       (toEnum . fromIntegral $ x.permissionType)))

handleCreateAcls :: ServerContext
                 -> K.RequestContext
                 -> K.CreateAclsRequest
                 -> IO K.CreateAclsResponse
handleCreateAcls ctx reqCtx req = do
  let authCtx = toAuthorizableReqCtx reqCtx
  let aclBindings = aclCreationToAclBinding <$> maybe [] V.toList (K.unKaArray req.creations)
  (withAuthorizerObject ctx.authorizer (createAcls authCtx)) aclBindings

--
deleteAclsFilterToAclBindingFilter :: K.DeleteAclsFilter -> AclBindingFilter
deleteAclsFilterToAclBindingFilter x =
  AclBindingFilter (ResourcePatternFilter (toEnum (fromIntegral x.resourceTypeFilter))
                                          (fromMaybe "" x.resourceNameFilter)
                                          Pat_LITERAL)
                   (AccessControlEntryFilter
                     (AccessControlEntryData (fromMaybe "" x.principalFilter)
                                             (fromMaybe "" x.hostFilter)
                                             (toEnum (fromIntegral x.operation))
                                             (toEnum (fromIntegral x.permissionType))))

handleDeleteAcls :: ServerContext
                 -> K.RequestContext
                 -> K.DeleteAclsRequest
                 -> IO K.DeleteAclsResponse
handleDeleteAcls ctx reqCtx req = do
  let authCtx = toAuthorizableReqCtx reqCtx
  let filters = maybe [] (fmap deleteAclsFilterToAclBindingFilter . V.toList)
                         (K.unKaArray req.filters)
  (withAuthorizerObject ctx.authorizer (deleteAcls authCtx)) filters
