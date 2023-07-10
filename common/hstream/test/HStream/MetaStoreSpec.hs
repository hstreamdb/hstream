module HStream.MetaStoreSpec where

import           Control.Monad                    (void)
import qualified Data.Aeson                       as A
import qualified Data.ByteString.Lazy             as BSL
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromMaybe)
import qualified Data.Text                        as T
import           Network.HTTP.Client              (defaultManagerSettings,
                                                   newManager)
import           System.Directory                 (getTemporaryDirectory,
                                                   removeFile)
import           System.Environment               (lookupEnv)
import           System.IO                        (hClose, openTempFile)
import           Test.Hspec
import           Test.QuickCheck                  (generate)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)
import           ZooKeeper.Types                  (ZHandle)

import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.FileUtils      as File
import           HStream.MetaStore.RqliteUtils    (createTable, deleteTable)
import           HStream.MetaStore.Types          (FHandle, HasPath (..),
                                                   MetaHandle (..),
                                                   MetaMulti (..),
                                                   MetaStore (..), RHandle (..))
import           HStream.MetaStore.ZookeeperUtils (tryCreate)
import           HStream.TestUtils                (MetaExample (..), metaGen)
import           HStream.Utils                    (textToCBytes)

spec :: Spec
spec = do
  runIO $ Log.setDefaultLoggerLevel Log.DEBUG
  m <- runIO $ newManager defaultManagerSettings
  let host = "127.0.0.1"

  -- rqlite
  portRq <- runIO $ fromMaybe "4001" <$> lookupEnv "RQLITE_LOCAL_PORT"
  let urlRq = T.pack $ host <> ":" <> portRq
  let mHandle1 = RLHandle $ RHandle m urlRq
  afterAll_ (void $ deleteTable m urlRq (myRootPath @MetaExample @RHandle)) (smokeTest mHandle1)

  -- zookeeper
  portZk <- runIO $ fromMaybe "2181" <$> lookupEnv "ZOOKEEPER_LOCAL_PORT"
  let urlZk = textToCBytes $ T.pack $ host <> ":" <> portZk
  let res = zookeeperResInit urlZk Nothing 5000 Nothing 0
  runIO $ withResource res $ \zk -> do
    let mHandle2 = ZkHandle zk
    hspec $ smokeTest mHandle2

  -- local file
  tmpfile <- runIO $ do
    tmpdir <- getTemporaryDirectory
    (file, tmphandle) <- openTempFile tmpdir "metastore"
    BSL.hPut tmphandle $ A.encode (mempty :: File.Contents)
    hClose tmphandle
    pure file
  afterAll_ (removeFile tmpfile) (smokeTest $ FileHandle tmpfile)

initMeta :: MetaHandle -> IO ()
initMeta h =
  case h of
    ZkHandle zk -> do
      tryCreate zk (textToCBytes $ myRootPath @MetaExample @ZHandle)
    RLHandle (RHandle m url) -> do
      createTable m url (myRootPath @MetaExample @RHandle)
    FileHandle fh -> do
      File.createTable (myRootPath @MetaExample @FHandle) fh

smokeTest :: HasCallStack => MetaHandle -> Spec
smokeTest h = beforeAll_ (initMeta h) $ do
  describe ("Run with " <> show h) $ do
    it "Basic Meta Test " $ do
      meta@Meta{metaId = id_1} <- generate metaGen
      meta2@Meta{metaId = id_2} <- generate metaGen
      newMeta <- generate metaGen
      insertMeta id_1 meta h
      upsertMeta id_2 meta2 h
      checkMetaExists @MetaExample id_1 h `shouldReturn` True
      checkMetaExists @MetaExample id_2 h `shouldReturn` True
      getMeta @MetaExample id_1 h `shouldReturn` Just meta
      getMeta @MetaExample id_2 h `shouldReturn` Just meta2
      updateMeta id_1 newMeta (Just 0) h
      upsertMeta id_2 newMeta h
      getMeta @MetaExample id_1 h `shouldReturn` Just newMeta
      getMeta @MetaExample id_2 h `shouldReturn` Just newMeta
      listMeta @MetaExample h `shouldReturn` [newMeta, newMeta]
      getAllMeta @MetaExample h `shouldReturn` Map.fromList [(id_1, newMeta), (id_2, newMeta)]
      deleteMeta @MetaExample id_1 Nothing h
      deleteMeta @MetaExample id_2 Nothing h
      getMeta @MetaExample id_1 h `shouldReturn` Nothing
      checkMetaExists @MetaExample id_1 h `shouldReturn` False
      listMeta @MetaExample h `shouldReturn` []

    it "Meta MultiOps Test" $ do
      meta1@Meta{metaId = metaId1} <- generate metaGen
      meta2@Meta{metaId = metaId2} <- generate metaGen
      newMeta1@Meta{metaId = _newMetaId1} <- generate metaGen
      newMeta2@Meta{metaId = _newMetaId2} <- generate metaGen
      let opInsert =
           [ insertMetaOp metaId1 meta1 h
           , insertMetaOp metaId2 meta2 h]
      let opUpdateFail =
           [ checkOp @MetaExample metaId1 1 h
           , updateMetaOp metaId1 newMeta1 Nothing h
           , updateMetaOp metaId2 newMeta2 Nothing h]
      let opUpdate =
           [ checkOp @MetaExample metaId1 0 h
           , updateMetaOp metaId1 newMeta1 Nothing h
           , updateMetaOp metaId2 newMeta2 Nothing h]
      let opDelete =
           [ deleteMetaOp @MetaExample metaId1 Nothing h
           , deleteMetaOp @MetaExample metaId2 Nothing h ]
      metaMulti opInsert h
      checkMetaExists @MetaExample metaId1 h `shouldReturn` True
      checkMetaExists @MetaExample metaId2 h `shouldReturn` True
      L.sort <$> listMeta @MetaExample h `shouldReturn` L.sort [meta1, meta2]
      getAllMeta @MetaExample h `shouldReturn` Map.fromList [(metaId1, meta1), (metaId2, meta2)]
      metaMulti opUpdateFail h `shouldThrow` anyException
      getMeta @MetaExample metaId1 h `shouldReturn` Just meta1
      getMeta @MetaExample metaId2 h `shouldReturn` Just meta2
      metaMulti opUpdate h
      getMeta @MetaExample metaId1 h `shouldReturn` Just newMeta1
      getMeta @MetaExample metaId2 h `shouldReturn` Just newMeta2
      L.sort <$> listMeta @MetaExample h `shouldReturn` L.sort [newMeta1, newMeta2]
      getAllMeta @MetaExample h `shouldReturn` Map.fromList [(metaId1, newMeta1), (metaId2, newMeta2)]
      metaMulti opDelete h
      checkMetaExists @MetaExample metaId1 h `shouldReturn` False
      checkMetaExists @MetaExample metaId2 h `shouldReturn` False
      getMeta @MetaExample metaId1 h `shouldReturn` Nothing
      getMeta @MetaExample metaId2 h `shouldReturn` Nothing
      listMeta @MetaExample h `shouldReturn` []
      getAllMeta @MetaExample h `shouldReturn` mempty

    -- TODO: add test for Exceptions
