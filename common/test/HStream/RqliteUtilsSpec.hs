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
    createTable m url table
    insertInto  m url table "my-id-1" v
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` [v]
    updateSet   m url table "my-id-1" v2 Nothing
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` [v2]
    deleteFrom  m url table "my-id-1" Nothing
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` ([] :: [AB])
    deleteTable m url table

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
         , UpdateROp table id1 v2BS
         , UpdateROp table id2 v2BS]
    let opUpdate =
         [ UpdateROp table id1 v2BS
         , UpdateROp table id2 v2BS]
    let opDelete =
         [ DeleteROp table id1
         , DeleteROp table id2 ]

    createTable m url table
    -- Insert
    transaction m url opInsert
    selectFrom m url table (Just id1) `shouldReturn` [v]
    selectFrom m url table (Just id2) `shouldReturn` [v]

    -- Update check fail
    transaction m url opUpdateFail `shouldThrow` anyException
    selectFrom m url table (Just id1) `shouldReturn` [v]
    selectFrom m url table (Just id2) `shouldReturn` [v]

    -- Update
    transaction m url opUpdate
    selectFrom m url table (Just id1) `shouldReturn` [v2]
    selectFrom m url table (Just id2) `shouldReturn` [v2]

    -- Delete
    transaction m url opDelete
    selectFrom m url table (Just id1) `shouldReturn` ([] :: [AB])
    selectFrom m url table (Just id2) `shouldReturn` ([] :: [AB])
    deleteTable m url table

  aroundAll (runWithUrlAndTable m url) $ do
    describe "Detailed test" $ do
      it "Insert into table with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

      it "Update with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        putStrLn "Update Empty"
        updateSet m url table metaId meta Nothing `shouldThrow` anyException
        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

        putStrLn "Update with version"
        meta2 <- generate (arbitrary :: Gen MetaExample)
        updateSet m url table metaId meta2 (Just 1)
        selectFrom m url table (Just metaId) `shouldReturn` [meta2]

        putStrLn "Update with no version"
        updateSet m url table metaId meta Nothing
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

        putStrLn "Update with invalid version"
        updateSet m url table metaId meta2 (Just 1) `shouldThrow` anyException
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

      it "Delete from with id" $ \table -> do
        meta@Meta{..} <- generate arbitrary

        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

        putStrLn "Delete with wrong version"
        deleteFrom m url table metaId (Just 10) `shouldThrow` anyException
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

        putStrLn "Delete with no version"
        deleteFrom m url table metaId Nothing
        selectFrom m url table (Just metaId) `shouldReturn` ([] :: [MetaExample])

        insertInto m url table metaId meta
        selectFrom m url table (Just metaId) `shouldReturn` [meta]

        putStrLn "Delete with version"
        deleteFrom m url table metaId (Just 1)
        selectFrom m url table (Just metaId) `shouldReturn` ([] :: [MetaExample])


runWithUrlAndTable :: Manager -> T.Text -> ActionWith T.Text -> IO ()
runWithUrlAndTable manager url action = do
  tableName <- generate name
  createTable manager url tableName
  action tableName
  deleteTable manager url tableName
