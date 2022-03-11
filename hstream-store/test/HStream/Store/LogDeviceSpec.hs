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

logdirAround :: I.LogAttributes -> SpecWith CBytes -> Spec
logdirAround attrs = aroundAll $ \runTest -> bracket setup clean runTest
  where
    setup = do
      dirname <- ("/" `FS.join`) =<< newRandomName 10
      lddir <- I.makeLogDirectory client dirname attrs False
      void $ I.syncLogsConfigVersion client =<< I.logDirectoryGetVersion lddir
      return dirname
    clean dirname =
      I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True

logdirSpec :: Spec
logdirSpec = describe "LogDirectory" $ do
  let attrs = def { I.logReplicationFactor = I.defAttr1 1
                  , I.logBacklogDuration = I.defAttr1 (Just 60)
                  , I.logAttrsExtras = Map.fromList [("A", "B")]
                  }

  it "get log directory children name" $ do
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
    let logid1 = 101
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
    let logid = 103
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

  let attrs_ra = def { I.logReplicateAcross = I.defAttr1 [(S.NodeLocationScope_DATA_CENTER, 3)] }
  logdirAround attrs_ra $ it "attributes: logReplicateAcross" $ \dirname -> do
    dir <- I.getLogDirectory client dirname
    attrs_got <- I.logDirectoryGetAttrs dir
    S.logReplicateAcross attrs_got `shouldBe` I.defAttr1 [(S.NodeLocationScope_DATA_CENTER, 3)]

  it "Loggroup's attributes should be inherited by the parent directory" $ do
    dirname <- ("/" `FS.join`) =<< newRandomName 10
    let logid = 104
        lgname = dirname <> "/A"
    _ <- I.makeLogDirectory client dirname attrs False
    I.syncLogsConfigVersion client =<< I.logGroupGetVersion
                                   =<< I.makeLogGroup client lgname logid logid def False
    lg <- I.getLogGroup client lgname
    attrs' <- I.logGroupGetAttrs lg
    I.logReplicationFactor attrs' `shouldBe` I.Attribute (Just 1) True
    I.logBacklogDuration attrs' `shouldBe` I.Attribute (Just (Just 60)) True
    Map.lookup "A" (I.logAttrsExtras attrs') `shouldBe` Just "B"
    I.syncLogsConfigVersion client =<< I.removeLogDirectory client dirname True

loggroupAround :: SpecWith (CBytes, S.C_LogID) -> Spec
loggroupAround = aroundAll $ \runTest -> bracket setup clean runTest
  where
    setup = do
      let attrs = def { I.logReplicationFactor = I.defAttr1 1
                      , I.logBacklogDuration = I.defAttr1 (Just 60)
                      , I.logSingleWriter = I.defAttr1 True
                      , I.logSyncReplicationScope = I.defAttr1 S.NodeLocationScope_DATA_CENTER
                      , I.logAttrsExtras = Map.fromList [("A", "B")]
                      }
          logid = 104
          logname = "LogDeviceSpec_LogGroupSpec"
      lg <- I.makeLogGroup client logname logid logid attrs False
      void $ I.syncLogsConfigVersion client =<< I.logGroupGetVersion lg
      return (logname, logid)
    clean (logname, _logid) =
      I.syncLogsConfigVersion client =<< I.removeLogGroup client logname

loggroupSpec :: Spec
loggroupSpec = describe "LogGroup" $ loggroupAround $ parallel $ do
  it "log group get attrs" $ \(lgname, _logid) -> do
    lg <- I.getLogGroup client lgname
    attrs' <- I.logGroupGetAttrs lg
    I.logReplicationFactor attrs' `shouldBe` I.defAttr1 1
    I.logBacklogDuration attrs' `shouldBe` I.defAttr1 (Just 60)
    I.logSingleWriter attrs' `shouldBe` I.defAttr1 True
    I.logSyncReplicationScope attrs' `shouldBe` I.defAttr1 S.NodeLocationScope_DATA_CENTER
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
