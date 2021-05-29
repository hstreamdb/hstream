module HStream.Store.LogDeviceSpec where

import           Data.List                        (sort)
import qualified Data.Map.Strict                  as Map
import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as I
import           HStream.Store.SpecUtils
import           Test.Hspec
import qualified Z.IO.FileSystem                  as FS

spec :: Spec
spec = do
  configType

configType :: Spec
configType = describe "LogConfigType" $ do
  it "get log directory children name" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [("A", "B")]
                                        }
    dirname <- ("/" `FS.join`) =<< newRandomName 10
    _ <- I.makeLogDirectory client dirname attrs False
    _ <- I.makeLogDirectory client (dirname <> "/A") attrs False
    _ <- I.makeLogDirectory client (dirname <> "/B") attrs False
    dir <- I.getLogDirectory client dirname
    names <- I.logDirChildrenNames dir
    sort names `shouldBe` ["A", "B"]
    I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True
    I.getLogDirectory client dirname `shouldThrow` anyException
