module HStream.RqliteUtilsSpec where

import qualified Data.Aeson                    as A
import qualified Data.ByteString.Lazy          as BL
import           Data.Maybe                    (fromMaybe)
import qualified Data.Text                     as T
import           Network.HTTP.Client           (Manager, defaultManagerSettings,
                                                newManager)
import           System.Environment            (lookupEnv)
import           Test.Hspec
import           Test.QuickCheck

import qualified HStream.Logger                as Log
import           HStream.MetaStore.RqliteUtils (ROp (..), createTable,
                                                deleteFrom, deleteTable,
                                                insertInto, selectFrom,
                                                transaction, updateSet)
import           HStream.TestUtils             (AB (..), MetaExample (..), name)

spec :: Spec
spec = do
  runIO $ Log.setLogLevel (Log.Level Log.INFO) True
  m <- runIO $ newManager defaultManagerSettings
  port <- runIO $ fromMaybe "4001" <$> lookupEnv "RQLITE_LOCAL_PORT"
  let host = "localhost"
  let url = T.pack $ host <> ":" <> port

  it "Smoke Test" $ do
    table <- generate name
    let v  = AB { a = "EXAMPLE-VALUE", b =1}
    let v2 = AB { a = "EXAMPLE-VALUE-2", b =2}
    createTable m url table `shouldReturn` True
    insertInto  m url table "my-id-1" v          `shouldReturn` True
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` Just [v]
    updateSet   m url table "my-id-1" v2 Nothing `shouldReturn` True
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` Just [v2]
    deleteFrom  m url table "my-id-1" Nothing    `shouldReturn` True
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` (Nothing :: Maybe [AB])
    deleteTable m url table `shouldReturn` True

  it "MultiOp Smoke Test" $ do
    table <- generate name
    let v  = AB { a = "EXAMPLE-VALUE", b = 1}
    let v2 = AB { a = "EXAMPLE-VALUE-2", b =2}
    let vBS = BL.toStrict $ A.encode v
    let v2BS = BL.toStrict $ A.encode v2
    let id1 = "my-id-1" :: T.Text
    let id2 = "my-id-2" :: T.Text
    let opInsert =
         [ InsertROp table id1 vBS
         , InsertROp table id2 vBS]
    let opUpdateFail =
         [ CheckROp  table id1 2
         , UpdateROp table id1 v2BS Nothing
         , UpdateROp table id2 v2BS Nothing]
    let opUpdate =
         [ UpdateROp table id1 v2BS Nothing
         , UpdateROp table id2 v2BS Nothing]
    let opDelete =
         [ DeleteROp table id1 Nothing
         , DeleteROp table id2 Nothing ]

    createTable m url table `shouldReturn` True
    -- Insert
    transaction m url opInsert `shouldReturn` True
    selectFrom m url table (Just id1) `shouldReturn` Just [v]
    selectFrom m url table (Just id2) `shouldReturn` Just [v]

    -- Update check fail
    transaction m url opUpdateFail `shouldReturn` False
    selectFrom m url table (Just id1) `shouldReturn` Just [v]
    selectFrom m url table (Just id2) `shouldReturn` Just [v]

    -- Update
    transaction m url opUpdate `shouldReturn` True
    selectFrom m url table (Just id1) `shouldReturn` Just [v2]
    selectFrom m url table (Just id2) `shouldReturn` Just [v2]

    -- Delete
    transaction m url opDelete `shouldReturn` True
    selectFrom m url table (Just id1) `shouldReturn` (Nothing :: Maybe [AB])
    selectFrom m url table (Just id2) `shouldReturn` (Nothing :: Maybe [AB])
    deleteTable m url table `shouldReturn` True

  aroundAll (runWithUrlAndTable m url) $ do
    describe "Detailed test" $ do
      it "Insert into table with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

      it "Update with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        putStrLn "Update Empty"
        updateSet m url table metaId meta Nothing `shouldReturn` False
        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Update with version"
        meta2 <- generate (arbitrary :: Gen MetaExample)
        updateSet m url table metaId meta2 (Just 1) `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta2]

        putStrLn "Update with no version"
        updateSet m url table metaId meta Nothing`shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Update with invalid version"
        updateSet m url table metaId meta2 (Just 1) `shouldReturn` False
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

      it "Delete from with id" $ \table -> do
        meta@Meta{..} <- generate arbitrary

        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Delete with wrong version"
        deleteFrom m url table metaId (Just 10) `shouldReturn` False
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Delete with no version"
        deleteFrom m url table metaId Nothing `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` (Nothing :: Maybe [MetaExample])

        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Delete with version"
        deleteFrom m url table metaId (Just 1) `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` (Nothing :: Maybe [MetaExample])


runWithUrlAndTable :: Manager -> T.Text -> ActionWith T.Text -> IO ()
runWithUrlAndTable manager url action = do
  tableName <- generate name
  createTable manager url tableName `shouldReturn` True
  action tableName
  deleteTable manager url tableName `shouldReturn` True
