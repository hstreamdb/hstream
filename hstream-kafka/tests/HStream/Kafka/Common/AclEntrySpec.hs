module HStream.Kafka.Common.AclEntrySpec where

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
import           HStream.Kafka.Common.Security

spec :: Spec
spec =
  describe "AclResourceNode" $ do
    let ace1 = Principal "User" "alice" `from` "host1" `does` AclOp_READ `is` AclPerm_DENY
        ace2 = Principal "User" "bob"   `from` "*"     `does` AclOp_READ `is` AclPerm_ALLOW
        ace3 = Principal "User" "bob"   `from` "host1" `does` AclOp_READ `is` AclPerm_DENY
    let entries = aceToAclEntry <$> [ace1, ace2, ace3]
        resNode = AclResourceNode 1 (Set.fromList entries)
    let aclJson = "{\"version\": 1,  \"acls\": [{\"host\": \"host1\",\"permissionType\": \"Deny\",\"operation\": \"READ\", \"principal\": \"User:alice\"  }, {  \"host\":  \"*\" ,  \"permissionType\": \"Allow\",  \"operation\":  \"Read\", \"principal\": \"User:bob\"  }, {  \"host\": \"host1\",  \"permissionType\": \"Deny\",  \"operation\":   \"Read\" ,  \"principal\": \"User:bob\"}]}"
    it "fromJSON . toJSON == id" $ do
      Aeson.decode (Aeson.encode resNode) `shouldBe` Just resNode
    it "from JSON should work on extra spaces and random ordered lists" $ do
      Aeson.decode aclJson `shouldBe` Just resNode
