module HStream.Kafka.Common.AuthorizerSpec where

import           Control.Monad
import qualified Data.Vector                           as V
import           HStream.Kafka.Common.TestUtils
import           Test.Hspec

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Authorizer.Class
import           HStream.Kafka.Common.Resource         hiding (match)
import           HStream.Kafka.Common.Security
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Message                as K

spec :: Spec
spec = do
  withZkBasedAclAuthorizer     `aroundAll` (specWithAuthorizer "ZooKeeper")
  withRqliteBasedAclAuthorizer `aroundAll` (specWithAuthorizer "Rqlite")
  withFileBasedAclAuthorizer   `aroundAll` (specWithAuthorizer "File")

specWithAuthorizer :: Authorizer a => String -> SpecWith a
specWithAuthorizer storeType =
  describe ("Authorizer based on " <> storeType) $ do
    let baseUsername = "alice"
        basePrincipal = Principal "User" baseUsername
        baseReqCtx = basePrincipal `from` "192.168.0.1"
    let allowReadAcl  = wildcardPrincipal `from` wildcardHost `does` AclOp_READ  `is` AclPerm_ALLOW
        allowWriteAcl = wildcardPrincipal `from` wildcardHost `does` AclOp_WRITE `is` AclPerm_ALLOW
        denyReadAcl   = wildcardPrincipal `from` wildcardHost `does` AclOp_READ  `is` AclPerm_DENY
    it "basic tasks (add and delete ACLs) should work" $ \a -> do
      resource <- genResource Res_GROUP
      let action = AclOp_READ `on` resource
      authorize baseReqCtx a [action] `shouldReturn` [Authz_DENIED]
      let binding = basePrincipal `from` "*" `does` AclOp_READ `is` AclPerm_ALLOW `on` resource
      void $ createAcls baseReqCtx a [binding]
      authorize  baseReqCtx a [action] `shouldReturn` [Authz_ALLOWED]
      void $ deleteAcls baseReqCtx a [toFilter binding]
      authorize  baseReqCtx a [action] `shouldReturn` [Authz_DENIED]

    it "ACLs on topic should work" $ \a -> do
      resource <- genResource Res_TOPIC
      let user1 = Principal "User" "alice"
          user2 = Principal "User" "bob"
          user3 = Principal "User" "cathy"
      let host1 = "192.168.1.1"
          host2 = "192.168.1.2"
      let acl1 = user1 `from` host1        `does` AclOp_READ     `is` AclPerm_ALLOW
          acl2 = user1 `from` host2        `does` AclOp_READ     `is` AclPerm_ALLOW
          acl3 = user1 `from` host1        `does` AclOp_READ     `is` AclPerm_DENY
          acl4 = user1 `from` host1        `does` AclOp_WRITE    `is` AclPerm_ALLOW
          acl5 = user1 `from` wildcardHost `does` AclOp_DESCRIBE `is` AclPerm_ALLOW
          acl6 = user2 `from` wildcardHost `does` AclOp_READ     `is` AclPerm_ALLOW
          acl7 = user3 `from` wildcardHost `does` AclOp_WRITE    `is` AclPerm_ALLOW
      let bindings = map (`on` resource) [acl1, acl2, acl3, acl4, acl5, acl6, acl7]
      void $ createAcls (user1 `from` host1) a bindings
      authorize (user1 `from` host2) a [AclOp_READ     `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user1 `from` host1) a [AclOp_READ     `on` resource] `shouldReturn` [Authz_DENIED]
      authorize (user1 `from` host1) a [AclOp_WRITE    `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user1 `from` host2) a [AclOp_WRITE    `on` resource] `shouldReturn` [Authz_DENIED]
      authorize (user1 `from` host1) a [AclOp_DESCRIBE `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user1 `from` host2) a [AclOp_DESCRIBE `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user1 `from` host1) a [AclOp_ALTER    `on` resource] `shouldReturn` [Authz_DENIED]
      authorize (user1 `from` host2) a [AclOp_ALTER    `on` resource] `shouldReturn` [Authz_DENIED]
      authorize (user2 `from` host1) a [AclOp_DESCRIBE `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user3 `from` host1) a [AclOp_DESCRIBE `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user2 `from` host1) a [AclOp_READ     `on` resource] `shouldReturn` [Authz_ALLOWED]
      authorize (user3 `from` host1) a [AclOp_WRITE    `on` resource] `shouldReturn` [Authz_ALLOWED]
    it "authorize with empty resource name should succeed" $ \a -> do
      authorize baseReqCtx a [AclOp_READ `on` ("" `typed` Res_GROUP `match` Pat_LITERAL)] `shouldReturn` [Authz_DENIED]
      void $ createAcls baseReqCtx a [allowReadAcl `on` (wildcardResourceName `typed` Res_GROUP `match` Pat_LITERAL)]
      authorize baseReqCtx a [AclOp_READ `on` ("" `typed` Res_GROUP `match` Pat_LITERAL)] `shouldReturn` [Authz_ALLOWED]
    it "create acl with empty resource name should fail" $ \a -> do
      K.CreateAclsResponse{..} <-
        createAcls baseReqCtx a [allowReadAcl `on` ("" `typed` Res_GROUP `match` Pat_LITERAL)]
      -- FIXME: error code
      results `shouldBe` K.KaArray (Just . V.singleton $ K.AclCreationResult 0 (Just "Resource name is empty"))
    it "deny rule should override allow rule" $ \a -> do
      let host = "192.168.2.1"
          principal = Principal "User" baseUsername
          reqCtx = principal `from` host
      resource <- genResource Res_TOPIC
      let allowAll = wildcardPrincipal `from` wildcardHost `does` AclOp_ALL `is` AclPerm_ALLOW
          denyAll  = principal         `from` host         `does` AclOp_ALL `is` AclPerm_DENY
      void $ createAcls reqCtx a ((`on` resource) <$> [allowAll, denyAll])
      authorize reqCtx a [AclOp_READ `on` resource] `shouldReturn` [Authz_DENIED]
    it "allow all really works" $ \a -> do
      let host = "192.0.4.4"
          principal = Principal "User" "random"
          reqCtx = principal `from` host
      resource <- genResource Res_TOPIC
      let allowAll = wildcardPrincipal `from` wildcardHost `does` AclOp_ALL `is` AclPerm_ALLOW
      void $ createAcls reqCtx a [allowAll `on` resource]
      authorize reqCtx a [AclOp_READ `on` resource] `shouldReturn` [Authz_ALLOWED]
    it "delete all acls" $ \a -> do
      let anyResPatFilter = ResourcePatternFilter Res_ANY wildcardResourceName Pat_LITERAL
          anyAceFilter = AccessControlEntryFilter $
                           AccessControlEntryData "" ""AclOp_ANY AclPerm_ANY
      let anyAclBindingFilter = AclBindingFilter anyResPatFilter anyAceFilter
      void $ deleteAcls baseReqCtx a [anyAclBindingFilter]
      getAcls baseReqCtx a anyAclBindingFilter `shouldReturn` []
