module HStream.Store.LogDeviceSpec where

import           Data.List                        (sort)
import qualified Data.Map.Strict                  as Map
import           Foreign                          (newForeignPtr_)
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
    version <- I.logDirectoryGetVersion =<< I.makeLogDirectory client (dirname <> "/B") attrs False
    I.syncLogsConfigVersion client version
    dir <- I.getLogDirectory client dirname
    names <- I.logDirChildrenNames dir
    sort names `shouldBe` ["A", "B"]
    I.logDirLogsNames dir `shouldReturn` []
    I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True
    I.getLogDirectory client dirname `shouldThrow` anyException

  it "get log directory logs name" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [("A", "B")]
                                        }
        logid1 = 101
        logid2 = 102
    dirname <- ("/" `FS.join`) =<< newRandomName 10
    _ <- I.makeLogDirectory client dirname attrs False
    _ <- I.makeLogGroup client (dirname <> "/A") logid1 logid1 attrs False
    version <- I.logGroupGetVersion =<<
      I.makeLogGroup client (dirname <> "/B") logid2 logid2 attrs False
    I.syncLogsConfigVersion client version
    dir <- I.getLogDirectory client dirname
    names <- I.logDirLogsNames dir
    sort names `shouldBe` ["A", "B"]
    I.logDirChildrenNames dir `shouldReturn` []
    I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True
    I.getLogDirectory client dirname `shouldThrow` anyException

  it "get log group and child directory" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [("A", "B")]
                                        }
        logid = 103
    dirname <- ("/" `FS.join`) =<< newRandomName 10
    _ <- I.makeLogDirectory client dirname attrs False
    _ <- I.makeLogDirectory client (dirname <> "/A") attrs False
    version <- I.logGroupGetVersion =<<
      I.makeLogGroup client (dirname <> "/B") logid logid attrs False
    I.syncLogsConfigVersion client version
    dir <- I.getLogDirectory client dirname
    nameA <- I.logDirectoryGetFullName =<< I.getLogDirectory client =<< I.logDirChildFullName dir "A"
    nameA `shouldBe` dirname <> "/A/"
    nameB <- I.logGroupGetFullName =<< I.getLogGroup client =<< I.logDirLogFullName dir "B"
    nameB `shouldBe` dirname <> "/B"
    I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True
    I.getLogDirectory client dirname `shouldThrow` anyException

  it "log group get attrs" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.fromList [("A", "B")]
                                        }
        logid = 101
    lg <- I.makeLogGroup client "lg" logid logid attrs False
    _ <- I.syncLogsConfigVersion client =<< I.logGroupGetVersion lg
    attrs' <- I.ldLogAttrsToHsLogAttrs =<< newForeignPtr_ =<< I.logGroupGetAttrs lg
    _ <- I.removeLogGroup client "lg"
    S.logReplicationFactor attrs' `shouldBe` 1
    Map.lookup "A" (S.logExtraAttrs attrs') `shouldBe` Just "B"
