module HStream.Store.SettingsSpec where

import           Test.Hspec
import qualified Z.Data.Builder          as B
import qualified Z.Data.CBytes           as CBytes
import           Z.Data.Vector           (packASCII)

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "StoreSpec" $ do
  base

base :: Spec
base = describe "Base" $ do
  let logid = 1

  it "default payload size of local-dev-cluster is 1MB" $ do
    S.getMaxPayloadSize client `shouldReturn` (1024 * 1024)

  it "modify max-payload-size for this client" $ do
    S.setClientSetting client "max-payload-size" "1024" -- minimum value: 16
    S.getMaxPayloadSize client `shouldReturn` 1024

    let payload n = packASCII $ replicate n 'a'
    _ <- S.append client logid (payload 1024) Nothing
    _ <- S.appendBatch client logid [payload 1024] S.CompressionNone Nothing
    S.append client logid (payload 1025) Nothing `shouldThrow` S.isTOOBIG
    S.appendBatch client logid [payload 1024, payload 20, payload 20, payload 20] S.CompressionNone Nothing `shouldThrow` S.isTOOBIG

    S.setClientSetting client "max-payload-size" $ CBytes.buildCBytes $ B.int @Int (1024 * 1024)
