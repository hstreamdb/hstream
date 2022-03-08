module HStream.Store.LogDeviceSpec where

import           Control.Exception                (bracket)
import           Control.Monad                    (void)
import           Data.Default                     (def)
import           Data.List                        (sort)
import qualified Data.Map.Strict                  as Map
import           Test.Hspec
import           Z.Data.CBytes                    (CBytes)
import qualified Z.IO.FileSystem                  as FS

import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as I
import           HStream.Store.SpecUtils

spec :: Spec
spec = do
  loggroupSpec
  logdirSpec

logdirSpec :: Spec
logdirSpec = describe "LogDirectory" $ do
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

loggroupAround :: SpecWith (CBytes, S.C_LogID) -> Spec
loggroupAround = aroundAll $ \runTest -> bracket setup clean runTest
  where
    setup = do
      let attrs = def { I.logReplicationFactor = I.def1 1
                      , I.logBacklogDuration = I.def1 (Just 60)
                      , I.logAttrsExtras = Map.fromList [("A", "B")]
                      }
          logid = 104
          logname = "LogDeviceSpec_LogGroupSpec"
      lg <- I.makeLogGroup_ client logname logid logid (Just attrs) False
      void $ I.syncLogsConfigVersion client =<< I.logGroupGetVersion lg
      return (logname, logid)
    clean (logname, _logid) = do
      I.syncLogsConfigVersion client =<< I.removeLogGroup client logname

loggroupSpec :: Spec
loggroupSpec = describe "LogGroup" $ loggroupAround $ parallel $ do
  it "log group get attrs" $ \(lgname, _logid) -> do
    lg <- I.getLogGroup client lgname
    attrs' <- I.logGroupGetAttrs lg
    I.logReplicationFactor attrs' `shouldBe` I.def1 1
    I.logBacklogDuration attrs' `shouldBe` I.def1 (Just 60)
    Map.lookup "A" (I.logAttrsExtras attrs') `shouldBe` Just "B"

  it "log group get and set range" $ \(lgname, logid) -> do
    let logid' = logid + 1
    lg <- I.getLogGroup client lgname
    I.logGroupGetRange lg `shouldReturn`(logid, logid)
    I.syncLogsConfigVersion client =<< I.logGroupSetRange client lgname (logid',logid')
    range' <- I.logGroupGetRange =<< I.getLogGroup client lgname
    range' `shouldBe` (logid', logid')

  it "get a nonexist loggroup should throw NOTFOUND" $ \(_, _) -> do
    I.getLogGroup client "this_is_a_non_exist_logroup" `shouldThrow` S.isNOTFOUND
