module HStream.Kafka.Common.AuthorizerSpec where

import           Control.Monad
import           Data.Text                             (Text)
import qualified Data.Vector                           as V
import           HStream.Kafka.Common.TestUtils
import           Test.Hspec
import           Test.Hspec.Expectations

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.AclEntry
import           HStream.Kafka.Common.AclStore
import           HStream.Kafka.Common.Authorizer
import           HStream.Kafka.Common.Authorizer.Class hiding (Authorizer (..))
import           HStream.Kafka.Common.Resource         hiding (match)
import           HStream.Kafka.Common.Security
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Message                as K

spec :: Spec
spec =
  describe "authorizer" $ do
    let baseUsername = "alice"
        basePrincipal = Principal "User" baseUsername
        baseReqCtx = basePrincipal `from` "192.168.0.1"
    let allowReadAcl  = wildcardPrincipal `from` wildcardHost `does` AclOp_READ  `is` AclPerm_ALLOW
        allowWriteAcl = wildcardPrincipal `from` wildcardHost `does` AclOp_WRITE `is` AclPerm_ALLOW
        denyReadAcl   = wildcardPrincipal `from` wildcardHost `does` AclOp_READ  `is` AclPerm_DENY
    it "basic tasks (add and delete ACLs) should work" $ do
      a <- newAclAuthorizer newMockAclStore
      initAclAuthorizer a
      resource <- genResource Res_GROUP
      let action = AclOp_READ `on` resource
      authorize baseReqCtx a [action] `shouldReturn` [Authz_DENIED]
      let binding = basePrincipal `from` "*" `does` AclOp_READ `is` AclPerm_ALLOW `on` resource
      createAcls baseReqCtx a [binding]
      authorize  baseReqCtx a [action] `shouldReturn` [Authz_ALLOWED]
      deleteAcls baseReqCtx a [toFilter binding]
      authorize  baseReqCtx a [action] `shouldReturn` [Authz_DENIED]

    it "ACLs on topic should work" $ do
      a <- newAclAuthorizer newMockAclStore
      initAclAuthorizer a
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
      createAcls (user1 `from` host1) a bindings
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
    it "authorize with empty resource name should succeed" $ do
      a <- newAclAuthorizer newMockAclStore
      initAclAuthorizer a
      authorize baseReqCtx a [AclOp_READ `on` ("" `typed` Res_GROUP `match` Pat_LITERAL)] `shouldReturn` [Authz_DENIED]
      createAcls baseReqCtx a [allowReadAcl `on` (wildcardResourceName `typed` Res_GROUP `match` Pat_LITERAL)]
      authorize baseReqCtx a [AclOp_READ `on` ("" `typed` Res_GROUP `match` Pat_LITERAL)] `shouldReturn` [Authz_ALLOWED]
    it "create acl with empty resource name should fail" $ do
      a <- newAclAuthorizer newMockAclStore
      initAclAuthorizer a
      K.CreateAclsResponse{..} <-
        createAcls baseReqCtx a [allowReadAcl `on` ("" `typed` Res_GROUP `match` Pat_LITERAL)]
      -- FIXME: error code
      results `shouldBe` K.KaArray (Just . V.singleton $ K.AclCreationResult 0 (Just "Resource name is empty"))
    it "deny rule should override allow rule" $ do
      a <- newAclAuthorizer newMockAclStore
      initAclAuthorizer a
      let host = "192.168.2.1"
          principal = Principal "User" baseUsername
          reqCtx = principal `from` host
      resource <- genResource Res_TOPIC
      let allowAll = wildcardPrincipal `from` wildcardHost `does` AclOp_ALL `is` AclPerm_ALLOW
          denyAll  = principal         `from` host         `does` AclOp_ALL `is` AclPerm_DENY
      createAcls reqCtx a ((`on` resource) <$> [allowAll, denyAll])
      authorize reqCtx a [AclOp_READ `on` resource] `shouldReturn` [Authz_DENIED]
    it "allow all really works" $ do
      a <- newAclAuthorizer newMockAclStore
      initAclAuthorizer a
      let host = "192.0.4.4"
          principal = Principal "User" "random"
          reqCtx = principal `from` host
      resource <- genResource Res_TOPIC
      let allowAll = wildcardPrincipal `from` wildcardHost `does` AclOp_ALL `is` AclPerm_ALLOW
      createAcls reqCtx a [allowAll `on` resource]
      authorize reqCtx a [AclOp_READ `on` resource] `shouldReturn` [Authz_ALLOWED]
