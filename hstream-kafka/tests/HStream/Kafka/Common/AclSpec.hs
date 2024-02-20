module HStream.Kafka.Common.AclSpec where

import           Control.Monad
import qualified Data.Aeson                            as Aeson
import qualified Data.Set                              as Set
import           Data.Text                             (Text)
import           HStream.Kafka.Common.TestUtils
import           Test.Hspec
import           Test.Hspec.Expectations

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.AclEntry
import           HStream.Kafka.Common.AclStore
import           HStream.Kafka.Common.Authorizer
import           HStream.Kafka.Common.Authorizer.Class hiding (Authorizer (..))
import           HStream.Kafka.Common.Resource         hiding (match)
import qualified HStream.Kafka.Common.Resource         as Resource
import           HStream.Kafka.Common.Security

spec :: Spec
spec =
  describe "AclBinding" $ do
    let acl1 :: AclBinding = Principal "User" "ANONYMOUS" `from` "host" `does` AclOp_ALL `is` AclPerm_ALLOW `on` ("mytopic" `typed` Res_TOPIC `match` Pat_LITERAL)
        acl2 :: AclBinding = Principal "User" "*" `from` "host" `does` AclOp_READ `is` AclPerm_ALLOW `on` ("mytopic" `typed` Res_TOPIC `match` Pat_LITERAL)
        acl3 :: AclBinding = Principal "User" "ANONYMOUS" `from` "127.0.0.1" `does` AclOp_READ `is` AclPerm_DENY `on` ("mytopic2" `typed` Res_TOPIC `match` Pat_LITERAL)
        aclUnknown :: AclBinding = Principal "User" "ANONYMOUS" `from` "127.0.0.1" `does` AclOp_UNKNOWN `is` AclPerm_DENY `on` ("mytopic2" `typed` Res_TOPIC `match` Pat_LITERAL)

        aclFilterAnyAnonymous :: AclBindingFilter = (toFilter $ Principal "User" "ANONYMOUS" `from` "" `does` AclOp_ANY `is` AclPerm_ANY) `on` anyFilter
        aclFilterAnyDeny :: AclBindingFilter = (toFilter $ Principal "" "" `from` "" `does` AclOp_ANY `is` AclPerm_DENY) `on` anyFilter
        aclFilterAnyMyTopic :: AclBindingFilter = (toFilter $ Principal "" "" `from` "" `does` AclOp_ANY `is` AclPerm_ANY) `on` (toFilter $ "mytopic" `typed` Res_TOPIC `match` Pat_LITERAL)
    it "eq" $ do
      acl1 `shouldNotBe` acl2
      acl2 `shouldNotBe` acl1
    it "match" $ do
      acl1 `Resource.match` anyFilter `shouldBe` True
      acl2 `Resource.match` anyFilter `shouldBe` True
      acl3 `Resource.match` anyFilter `shouldBe` True

      acl1 `Resource.match` aclFilterAnyAnonymous `shouldBe` True
      acl2 `Resource.match` aclFilterAnyAnonymous `shouldBe` False
      acl3 `Resource.match` aclFilterAnyAnonymous `shouldBe` True

      acl1 `Resource.match` aclFilterAnyDeny `shouldBe` False
      acl2 `Resource.match` aclFilterAnyDeny `shouldBe` False
      acl3 `Resource.match` aclFilterAnyDeny `shouldBe` True

      acl1 `Resource.match` aclFilterAnyMyTopic `shouldBe` True
      acl2 `Resource.match` aclFilterAnyMyTopic `shouldBe` True
      acl3 `Resource.match` aclFilterAnyMyTopic `shouldBe` False

      aclUnknown `Resource.match` aclFilterAnyAnonymous `shouldBe` True
      aclUnknown `Resource.match` aclFilterAnyDeny      `shouldBe` True
      aclUnknown `Resource.match` aclFilterAnyMyTopic   `shouldBe` False
    it "matchAtMostOne" $ do
      indefiniteFieldInFilter (toFilter acl1) `shouldBe` Nothing
      indefiniteFieldInFilter (toFilter acl2) `shouldBe` Nothing
      indefiniteFieldInFilter (toFilter acl3) `shouldBe` Nothing
      matchAtMostOne aclFilterAnyAnonymous `shouldBe` False
      matchAtMostOne aclFilterAnyDeny      `shouldBe` False
      matchAtMostOne aclFilterAnyMyTopic   `shouldBe` False
