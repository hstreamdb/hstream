module HStream.MetaStoreSpec where
import           Control.Monad                    (void)
import qualified Data.List                        as L
import           Data.Maybe
import qualified Data.Text                        as T
import           Network.HTTP.Client              (defaultManagerSettings,
                                                   newManager)
import           System.Environment               (lookupEnv)
import           Test.Hspec                       (HasCallStack, Spec,
                                                   afterAll_, describe, hspec,
                                                   it, runIO, shouldReturn)
import           Test.QuickCheck                  (generate)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)
import           ZooKeeper.Types                  (ZHandle)


import qualified HStream.Logger                   as Log
import           HStream.MetaStore.RqliteUtils    (createTable, deleteTable)
import           HStream.MetaStore.Types          (HasPath (..),
                                                   MetaHandle (..),
                                                   MetaMulti (..),
                                                   MetaStore (..), RHandle (..))
import           HStream.MetaStore.ZookeeperUtils (tryCreate)
import           HStream.TestUtils                (MetaExample (..), metaGen)
import           HStream.Utils                    (textToCBytes)

spec :: Spec
spec = do
  runIO $ Log.setLogLevel (Log.Level Log.INFO) True
  m <- runIO $ newManager defaultManagerSettings
  portRq <- runIO $ fromMaybe "4001" <$> lookupEnv "RQLITE_LOCAL_PORT"
  portZk <- runIO $ fromMaybe "2181" <$> lookupEnv "ZOOKEEPER_LOCAL_PORT"
  let host = "127.0.0.1"
  let urlRq = T.pack $ host <> ":" <> portRq
  let urlZk = textToCBytes $ T.pack $ host <> ":" <> portZk
  let mHandle1 = RLHandle $ RHandle m urlRq
  let res = zookeeperResInit urlZk Nothing 5000 Nothing 0
  afterAll_ (void $ deleteTable m urlRq (myRootPath @MetaExample @RHandle)) (smokeTest mHandle1)
  runIO $ withResource res $ \zk -> do
    let mHandle2 = ZkHandle zk
    hspec $ smokeTest mHandle2

smokeTest :: HasCallStack => MetaHandle -> Spec
smokeTest h = do
  hName <- case h of
    ZkHandle zk -> do
      runIO $ tryCreate zk (textToCBytes $ myRootPath @MetaExample @ZHandle)
      return "Zookeeper Handle"
    RLHandle (RHandle m url) -> do
      runIO $ createTable m url (myRootPath @MetaExample @RHandle) `shouldReturn` True
      return "RQLite Handle"
  describe ("Run with " <> hName) $ do
    it "Basic Meta Test " $ do
      meta@Meta{..} <- generate metaGen
      newMeta <- generate metaGen
      insertMeta metaId meta h
      checkMetaExists @MetaExample metaId h `shouldReturn` True
      getMeta @MetaExample metaId h `shouldReturn` Just meta
      updateMeta metaId newMeta Nothing h
      getMeta @MetaExample metaId h `shouldReturn` Just newMeta
      listMeta @MetaExample h `shouldReturn` [newMeta]
      deleteMeta @MetaExample metaId Nothing h
      getMeta @MetaExample metaId h `shouldReturn` Nothing
      checkMetaExists @MetaExample metaId h `shouldReturn` False
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
           [ checkOp @MetaExample metaId1 2 h
           , updateMetaOp metaId1 newMeta1 Nothing h
           , updateMetaOp metaId2 newMeta2 Nothing h]
      let opUpdate =
           [ updateMetaOp metaId1 newMeta1 Nothing h
           , updateMetaOp metaId2 newMeta2 Nothing h]
      let opDelete =
           [ deleteMetaOp @MetaExample metaId1 Nothing h
           , deleteMetaOp @MetaExample metaId2 Nothing h ]
      metaMulti opInsert h `shouldReturn` True
      checkMetaExists @MetaExample metaId1 h `shouldReturn` True
      checkMetaExists @MetaExample metaId2 h `shouldReturn` True
      L.sort <$> listMeta @MetaExample h `shouldReturn` L.sort [meta1, meta2]
      metaMulti opUpdateFail h `shouldReturn` False
      getMeta @MetaExample metaId1 h `shouldReturn` Just meta1
      getMeta @MetaExample metaId2 h `shouldReturn` Just meta2
      metaMulti opUpdate h `shouldReturn` True
      getMeta @MetaExample metaId1 h `shouldReturn` Just newMeta1
      getMeta @MetaExample metaId2 h `shouldReturn` Just newMeta2
      L.sort <$> listMeta @MetaExample h `shouldReturn` L.sort [newMeta1, newMeta2]
      metaMulti opDelete h `shouldReturn` True
      checkMetaExists @MetaExample metaId1 h `shouldReturn` False
      checkMetaExists @MetaExample metaId2 h `shouldReturn` False
      getMeta @MetaExample metaId1 h `shouldReturn` Nothing
      getMeta @MetaExample metaId2 h `shouldReturn` Nothing
      listMeta @MetaExample h `shouldReturn` []
